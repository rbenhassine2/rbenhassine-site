---
title: "Building a Salesforce Sync Engine, Part 2: In-Database Transforms and Handling Schema Drift"
description: "How to handle Salesforce custom field drift and execute high-speed picklist mapping using PostgreSQL information_schema intersections and JSONB operators."
pubDate: 2026-10-11
tags: ["Salesforce", "Postgres", "SQL", "Data Pipelines", "Architecture"]
---

> **Series: Architecture of a Salesforce-to-Postgres Sync Engine**  
> - [Part 1: Streaming Bulk API 2.0 into Postgres COPY](/blog/salesforce-sync-engine-part-1-streaming)  
> - **Part 2: In-Database Transforms and Schema Drift** (This article)  
> - [Part 3: Foreign Key Resolution and Distributed Locks](/blog/salesforce-sync-engine-part-3-foreign-keys-resilience)

---

In [Part 1](/blog/salesforce-sync-engine-part-1-streaming), we looked at how to stream raw Salesforce query results straight into Postgres staging tables (`temp_sf_to_odoo`) without buffering them in Python.

Once the data lands in staging, it needs to be transformed and merged into production tables. In typical sync scripts, this step often suffers from two practical issues:

1. **Row-by-row Python processing**: Pulling records back into Python memory, looping through each row to translate picklist values, and executing individual updates. On large tables, this creates substantial network round-trip overhead and slows down the pipeline.
2. **Schema drift**: When a Salesforce admin adds a custom field or updates an object definition, sync scripts that assume a 1:1 matching table schema crash on unexpected columns, stalling the pipeline until someone updates the local database.

Here is how our engine handles schema changes dynamically and runs transformations directly inside PostgreSQL.

---

## Dynamic Column Matching with `information_schema`

In enterprise Salesforce instances, custom fields are added frequently. If an admin creates a new field in Salesforce that has not been defined in PostgreSQL yet, the sync should continue processing the existing fields rather than failing.

Instead of maintaining hardcoded column lists in Python code, our `Upserter` queries `information_schema.columns` at runtime to find the intersection of columns that exist in both the staging table and the destination table:

```python
import asyncpg
from datasync.models import SyncObjectConfig


class Upserter:
    async def _get_merge_columns(
        self,
        conn: asyncpg.Connection,
        object_config: SyncObjectConfig,
        pk_column: str,
    ) -> set[str]:
        columns_list_sql = """
            SELECT c.column_name
            FROM information_schema.columns c
            WHERE c.table_schema = $1 
              AND c.table_name = $2
              AND c.column_name NOT IN ('id', $3);
        """

        # Fetch columns present in the staging table
        temp_rows = await conn.fetch(
            columns_list_sql, "temp_sf_to_odoo", object_config.pg_table, pk_column
        )
        temp_columns: set[str] = {row["column_name"] for row in temp_rows}

        # Fetch columns present in the destination public table
        public_rows = await conn.fetch(
            columns_list_sql, "public", object_config.pg_table, pk_column
        )
        public_columns: set[str] = {row["column_name"] for row in public_rows}

        # Only merge columns that exist in both tables
        return temp_columns & public_columns
```

This simple intersection provides useful resilience:
- If Salesforce returns new fields that aren't yet modeled in Postgres, the staging table holds them, but the merge query simply leaves them out.
- If the Postgres table has local columns (like `create_uid` or internal timestamps), the merge query does not overwrite them with nulls.
- Syncs keep running smoothly even during intermediate schema migrations.

---

## Generating Set-Based Upserts

With the matching columns identified, the engine generates a parameterized `INSERT ... ON CONFLICT DO UPDATE` query:

```python
    def _build_merge_sql(self, table_name: str, pk_column: str, columns: set[str]) -> str:
        quoted_pk = f'"{pk_column}"'
        columns.add("x_salesforce_id")
        
        columns_clause = ", ".join({f'"{c}"' for c in columns})
        update_clause = ", ".join({f'"{c}" = EXCLUDED."{c}"' for c in columns})

        return f"""
            INSERT INTO public."{table_name}" ({columns_clause})
            SELECT {columns_clause} 
            FROM temp_sf_to_odoo."{table_name}"
            ON CONFLICT ({quoted_pk}) DO UPDATE SET 
                {update_clause};
        """
```

The resulting SQL runs entirely within Postgres:

```sql
INSERT INTO public."res_partner" ("x_salesforce_id", "name", "email", "phone", "vat")
SELECT "x_salesforce_id", "name", "email", "phone", "vat"
FROM temp_sf_to_odoo."res_partner"
ON CONFLICT ("x_salesforce_id") DO UPDATE SET
    "name" = EXCLUDED."name",
    "email" = EXCLUDED."email",
    "phone" = EXCLUDED."phone",
    "vat" = EXCLUDED."vat";
```

Executing this as a single set-based query lets PostgreSQL optimize disk writes and index updates internally, avoiding thousands of round-trips back and forth between Python and the database.

---

## Mapping Picklists in SQL Using `JSONB`

Salesforce picklist strings rarely match target database values directly. For example, Salesforce might use `"Qualified Lead"` while the local system uses an internal stage ID like `2`.

Instead of iterating through rows in Python to translate values, our `Transformer` passes the mapping dictionary into Postgres as a JSONB literal and executes the mapping with a single `UPDATE`:

```python
import json
import asyncpg
from datasync.models import SyncObjectConfig


class Transformer:
    async def map_picklist_values(
        self, conn: asyncpg.Connection, object_config: SyncObjectConfig
    ) -> None:
        commands: list[str] = []

        def escape_literal(value: str) -> str:
            return value.replace("'", "''")

        for field in object_config.main_fields:
            if field.picklist_value_mapping:
                mapping_json = escape_literal(json.dumps(field.picklist_value_mapping))
                fallback = (
                    f"'{escape_literal(field.picklist_fallback_value)}'"
                    if field.picklist_fallback_value
                    else f"{field.column_name}::text"
                )

                commands.append(f"""
                    UPDATE temp_sf_to_odoo.{object_config.pg_table}
                    SET {field.column_name} = COALESCE(
                        '{mapping_json}'::jsonb ->> {field.column_name}::text,
                        {fallback}
                    )
                    WHERE {field.column_name} IS NOT NULL;
                """)

        if commands:
            await conn.execute("\n".join(commands))
```

The generated SQL looks like this:

```sql
UPDATE temp_sf_to_odoo.res_partner
SET stage_id = COALESCE(
    '{"Qualified Lead": 2, "Closed Won": 5, "Proposal": 3}'::jsonb ->> stage_id::text,
    stage_id::text
)
WHERE stage_id IS NOT NULL;
```

Postgres evaluates the JSON key lookup (`->>`) natively in C during the table scan, translating thousands of values in place before the data is merged into production.

---

## Normalizing Multi-Select Picklists

Salesforce stores multi-select picklists as semicolon-separated strings (for example, `"Enterprise;Fintech;EMEA"`). In a relational schema, these belong in a many-to-many junction table.

We split and link them directly in Postgres using `string_to_array` and `unnest`:

```sql
-- Add any newly discovered tags to the tag table
INSERT INTO public.partner_tag (name)
SELECT DISTINCT unnested_val
FROM (
    SELECT unnest(string_to_array("Category__c", ';')) AS unnested_val
    FROM temp_sf_to_odoo.res_partner
    WHERE "Category__c" IS NOT NULL
) sub
ON CONFLICT (name) DO NOTHING;

-- Populate the junction table
INSERT INTO public.res_partner_tag_rel (partner_id, tag_id)
SELECT DISTINCT p.id, t.id
FROM (
    SELECT "Id" AS sf_id, unnest(string_to_array("Category__c", ';')) AS tag_name
    FROM temp_sf_to_odoo.res_partner
    WHERE "Category__c" IS NOT NULL
) raw
JOIN public.res_partner p ON p.x_salesforce_id = raw.sf_id
JOIN public.partner_tag t ON t.name = raw.tag_name
ON CONFLICT DO NOTHING;
```

String splitting, deduplication, and ID resolution all happen in a single query plan without moving data back and forth to application memory.

---

## Next Step

At this point, records and picklists are updated cleanly. But relational systems have foreign keys, and syncing objects like Account and Contact often introduces circular references.

In **[Part 3: Foreign Key Resolution and Distributed Locks](/blog/salesforce-sync-engine-part-3-foreign-keys-resilience)**, we cover how to resolve cross-system foreign keys using a two-pass identity index and ensure safe execution with Postgres advisory locks.
