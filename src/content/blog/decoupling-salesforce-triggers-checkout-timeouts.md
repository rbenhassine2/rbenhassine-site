---
title: "Eliminating Checkout Timeouts: Decoupling Frontends from Salesforce Triggers"
description: "How slow Apex triggers cause checkout lockups, and how to use the transactional outbox pattern to keep customer requests fast and reliable."
pubDate: 2026-10-09
tags: ["Salesforce", "Performance", "Apex", "FastAPI", "Architecture"]
---

When an e-commerce or customer portal checkout writes synchronously to Salesforce during payment processing, slow Apex triggers and database lock contention can easily push HTTP response times past typical 30-second gateway timeout limits. If the browser times out and the user clicks submit again, they often end up with duplicate charges.

The root cause in these setups is usually not the web server or the payment gateway, but synchronous coupling to Salesforce record updates.

---

## The Problem with Synchronous CRM Updates

A typical synchronous checkout flow looks like this:

```
Customer Browser
      │ 1. POST /checkout/submit
      ▼
Python Backend (FastAPI / Django)
      │ 2. Charge payment provider (Stripe, etc.)
      ▼
Salesforce REST API Callout
      │ 3. Update Contact & create Opportunity
      ▼
Apex Triggers (Salesforce Execution Context)
      ├─ Trigger 1: recalculate customer status
      ├─ Trigger 2: workflow updates related account records
      ├─ Trigger 3: outbound notification to sales reps
      ├─ Trigger 4: audit logs and external callouts
      └─ [Cascading trigger executions can easily take 15–30+ seconds]
      ▼
Gateway Timeout (504)
```

Because the web backend waits for Salesforce to finish executing all trigger logic before responding to the browser, any lock contention or queue delay in Salesforce stalls the customer's checkout request.

---

## Decoupling the Critical Path with an Outbox Table

The fix is to remove Salesforce callouts from the critical synchronous checkout request. The user only needs to know that their payment succeeded and their order was recorded. Syncing that order to the CRM can happen asynchronously in the background.

```
[Synchronous Request Flow]
Customer Browser
      │ POST /checkout/submit
      ▼
Python Backend
      ├─ 1. Authorize & capture charge with payment gateway
      ├─ 2. Write order record to local Postgres (state: captured)
      ├─ 3. Write sync payload to outbox table (state: pending)
      └─ 4. Return HTTP 200 to customer browser

[Background Worker Flow]
Outbox Worker Task
      │ Read pending rows from outbox table
      ▼
Salesforce API Callout
      ├─ Upsert records using an External ID
      └─ Update outbox row to synced
```

---

## Implementing the Transactional Outbox Pattern

Using PostgreSQL transactions, the order record and the outbox event are saved atomically in the same database transaction:

```python
from fastapi import APIRouter, HTTPException
import asyncpg

router = APIRouter()


@router.post("/checkout/submit")
async def process_checkout(payload: CheckoutRequest, db: asyncpg.Connection):
    # 1. Authorize charge with the payment gateway
    charge = await payment_gateway.charge(amount=payload.amount, token=payload.token)
    if not charge.success:
        raise HTTPException(status_code=400, detail="Payment declined")

    # 2. Atomic write to local database and outbox table
    async with db.transaction():
        order_id = await db.fetchval(
            """
            INSERT INTO orders (customer_id, amount, status, transaction_ref)
            VALUES ($1, $2, 'captured', $3)
            RETURNING id
            """,
            payload.customer_id, payload.amount, charge.transaction_id,
        )

        await db.execute(
            """
            INSERT INTO crm_outbox (aggregate_type, aggregate_id, payload, status)
            VALUES ('order', $1, $2, 'pending')
            """,
            order_id, payload.model_dump_json(),
        )

    # 3. Return immediate success to the client
    return {"status": "success", "order_id": order_id}
```

If the database write fails, the entire transaction rolls back. If it succeeds, the outbox record is persisted and guaranteed to be picked up by the worker, even if Salesforce is temporarily down for maintenance.

---

## Optimizing Salesforce Triggers

On the Salesforce side, heavy automations should never run synchronously on single record updates:

1. **Move non-essential logic to Queueable Apex**:
   Reporting rollups, Slack/email notifications, and secondary updates can be enqueued asynchronously:
   ```apex
   public class OrderTriggerHandler extends TriggerHandler {
       public override void afterInsert() {
           // Queue background processing without blocking the transaction
           System.enqueueJob(new AsyncNotificationQueueable(Trigger.new));
       }
   }
   ```
2. **Prevent recursive trigger execution**:
   Use static execution guards so related record updates do not cause the same trigger handler to re-run in a loop.

---

## Practical Benefits

Decoupling the checkout path this way solves several problems:
- **Faster checkout response times**: The user's request only waits for the payment gateway and a single local database transaction.
- **Resilience against CRM downtime**: If Salesforce is undergoing maintenance or experiencing API rate limits, checkouts continue processing normally. The background outbox worker simply retries and catches up once the CRM is reachable again.
- **Zero duplicate transactions**: Because timeouts are eliminated, users don't repeatedly hit the submit button on pending charges.
