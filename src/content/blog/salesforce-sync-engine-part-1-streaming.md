---
title: "Building a Salesforce Sync Engine, Part 1: Streaming Bulk API 2.0 into Postgres COPY"
description: "How to stream large Salesforce query results directly into PostgreSQL staging tables with a flat memory profile using HTTP chunk streaming and asyncpg COPY."
pubDate: 2026-10-10
tags: ["Salesforce", "Postgres", "Python", "Data Pipelines", "Architecture"]
---

> **Series: Architecture of a Salesforce-to-Postgres Sync Engine**  
> - **Part 1: Streaming Bulk API 2.0 into Postgres COPY** (This article)  
> - [Part 2: In-Database Transforms and Schema Drift](/blog/salesforce-sync-engine-part-2-schema-transforms)  
> - [Part 3: Foreign Key Resolution and Distributed Locks](/blog/salesforce-sync-engine-part-3-foreign-keys-resilience)

---

Pulling data out of Salesforce with Python usually starts with standard REST API calls or small Bulk API jobs, parsing the CSV or JSON payload into memory, and writing it to PostgreSQL with batch inserts. 

That approach works fine for small tables, but it runs into memory issues once objects contain hundreds of thousands of records. In Python, representing records as lists of dictionaries or DataFrames creates significant per-field allocation overhead. Each string, integer, and dictionary key has its own `PyObject` header and pointer structure. On wide objects like `Account` or `Contact`, loading a multi-gigabyte query result directly into memory can trigger an out-of-memory kill on constrained worker containers.

To avoid keeping rows in Python memory at all, we built an ingestion pipeline that pipes bytes directly from Salesforce Bulk API 2.0 into PostgreSQL using `httpx` and `asyncpg`.

---

## The Streaming Pipeline

Instead of parsing rows in application code, Python acts purely as an asynchronous byte conduit between the Salesforce HTTP connection and PostgreSQL's text `COPY` interface.

```mermaid
flowchart LR
    SF["Salesforce Bulk API 2.0<br/>Query Results Endpoint"] -->|Async HTTP Chunks<br/>response.aiter_bytes()| Peeker["CSVStreamPeeker<br/>(Reads header line only)"]
    Peeker -->|Async Byte Stream<br/>Backtick Delimited| PG["PostgreSQL<br/>conn.copy_to_table()"]
    PG --> Staging["temp_sf_to_odoo.account<br/>(Unlogged Staging Table)"]
```

The process works in five steps:

1. Create a SOQL query job using Salesforce Bulk API 2.0 with a high watermark (`WHERE SystemModstamp > :last_synced_at`).
2. Poll the job status with backoff until it completes.
3. Open an HTTP streaming GET request to the results endpoint.
4. Read just enough bytes to extract the header line and determine column names.
5. Pass the unbuffered byte stream directly to `asyncpg.Connection.copy_to_table()`, writing straight into an unlogged staging table.

---

## Reading the Header Without Buffering the File

PostgreSQL's `COPY` command needs to know the column list ahead of time if you want to map specific columns or handle null fields. However, reading the entire file to find the header defeats the purpose of streaming.

We handle this with a small helper class, `CSVStreamPeeker`. It reads just enough chunks from the async generator to find the first newline, extracts the header, and then yields the remaining buffer followed by the rest of the stream:

```python
import csv
import io
import typing


class CSVStreamPeeker:
    """Reads the CSV header from an async byte stream, then yields the rest."""

    def __init__(self, async_gen: typing.AsyncIterable[bytes]):
        self.async_gen = aiter(async_gen)
        self.buffer = b""

    async def peek_header(self, delimiter: str = "`") -> list[str]:
        async for chunk in self.async_gen:
            self.buffer += chunk
            if b"\n" in self.buffer:
                break

        if b"\n" not in self.buffer:
            line = self.buffer
            self.buffer = b""
        else:
            line, self.buffer = self.buffer.split(b"\n", 1)

        header_str = line.decode("utf-8").strip("\r\n")
        reader = csv.reader(io.StringIO(header_str), delimiter=delimiter)
        return next(reader)

    async def __aiter__(self):
        if self.buffer:
            yield self.buffer
            self.buffer = b""
        async for chunk in self.async_gen:
            yield chunk
```

We configure Bulk API 2.0 to use the backtick (`` ` ``) delimiter (`columnDelimiter: "BACKQUOTE"`). Salesforce freeform text fields often contain commas, newlines, and unescaped quotes. Using backticks prevents delimiter collisions without requiring complex CSV escaping rules.

---

## Piping Bytes to Postgres COPY

Next, we connect `httpx` chunk streaming directly to `asyncpg`:

```python
import typing
from typing import AsyncIterable
import httpx
import asyncpg


class CSVStreamer:
    def __init__(self, http_client: httpx.AsyncClient, auth_headers: dict[str, str]):
        self._client = http_client
        self._headers = auth_headers

    async def stream_results(self, instance_url: str, job_id: str) -> AsyncIterable[bytes]:
        url = f"{instance_url}/services/data/v60.0/jobs/query/{job_id}/results?maxRecords=9000000"
        
        async with self._client.stream("GET", url, headers=self._headers, timeout=600.0) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes(chunk_size=65536):
                yield chunk


class PostgresCopier:
    async def copy_stream(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        schema_name: str,
        byte_stream: AsyncIterable[bytes],
        force_null_columns: list[str] | None = None,
    ) -> None:
        peeker = CSVStreamPeeker(byte_stream)
        csv_columns = await peeker.peek_header(delimiter="`")

        kwargs: dict[str, typing.Any] = {
            "table_name": table_name,
            "schema_name": schema_name,
            "format": "csv",
            "delimiter": "`",
            "header": False,
            "columns": csv_columns,
            "source": peeker,
        }

        if force_null_columns:
            kwargs["null"] = ""
            kwargs["force_null"] = force_null_columns

        # Streams directly from the HTTP socket into Postgres
        await conn.copy_to_table(**kwargs)
```

`asyncpg.Connection.copy_to_table` accepts an `AsyncIterable[bytes]` as its source. As network packets arrive from Salesforce, `asyncpg` forwards them straight to PostgreSQL's `COPY` engine. 

Because chunks are processed as they arrive, the worker process maintains a low, stable memory baseline (typically 30–50MB RSS) regardless of the size of the result set.

---

## Using Unlogged Staging Tables

We write the raw stream into unlogged tables in a temporary staging schema (`temp_sf_to_odoo`):

```sql
CREATE SCHEMA IF NOT EXISTS temp_sf_to_odoo;

DROP TABLE IF EXISTS temp_sf_to_odoo.account;
CREATE UNLOGGED TABLE temp_sf_to_odoo.account (
    "Id" text PRIMARY KEY,
    "Name" text,
    "BillingCity" text,
    "BillingPostalCode" text,
    "AnnualRevenue" numeric(18, 2),
    "LastModifiedDate" timestamp,
    "x_odoo_id" numeric(16, 0)
);
```

Using unlogged tables has two advantages here:
- It skips Write-Ahead Log (WAL) generation during the bulk ingestion phase, which reduces disk writes and speeds up the load.
- It isolates the raw Salesforce data so we can validate types, resolve picklists, and run set-based merges without touching production tables.

---

## Next Step

Once data is in the staging table, the next challenge is reconciling field differences and updating production tables efficiently. 

In **[Part 2: In-Database Transforms and Schema Drift](/blog/salesforce-sync-engine-part-2-schema-transforms)**, we look at how to use `information_schema` to handle custom field changes automatically and map picklist values using Postgres JSONB operators.
