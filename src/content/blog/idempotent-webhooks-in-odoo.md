---
title: "Idempotent Webhook Ingestion in Odoo 17 & 19"
description: "How to handle out-of-order deliveries and network retries gracefully using HMAC-SHA256, database deduplication, and a dead-letter queue."
pubDate: 2026-10-09
tags: ["Odoo", "Python", "Webhooks", "Distributed Systems"]
---

When integrating external platforms (like Stripe, Salesforce, or billing systems) with Odoo, webhook endpoints often start with a simple controller route:

```python
# Vulnerable to duplicate executions during retries
@http.route('/webhook/payment', type='json', auth='none', csrf=False)
def receive_payment(self, **post):
    request.env['sale.order'].sudo().create_invoice(...)
    return {'status': 'ok'}
```

In production, simple endpoints run into several issues:
1. **Network retries**: If a database lock delays the response and the caller times out, the external service will retry the request, potentially creating duplicate invoices or payments.
2. **Out-of-order delivery**: Network delays can cause an `order_updated` event to arrive before the initial `order_created` event.
3. **Invalid or spoofed payloads**: Without signature verification, endpoints accept forged requests.

Making webhook ingestion reliable in Odoo requires three components: constant-time HMAC signature checks, database-level deduplication, and an exponential backoff retry queue.

---

## The Ingestion Pipeline

```
External Service (Stripe / Salesforce / Billing)
         │
         ▼ POST /api/v1/webhook/<endpoint_slug>
┌────────────────────────────────────────────────────────┐
│ Inbound Controller                                     │
│  1. Verify HMAC-SHA256 signature                       │
│  2. Extract Idempotency Key (header or payload)        │
│  3. Check if key already exists in database            │
└───────────────────────┬────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────────────────────────┐
│ webhook.event Queue Model                              │
├────────────────────────────────────────────────────────┤
│ If Duplicate:                                          │
│   Return HTTP 200 OK (ignored)                         │
│ If Synchronous:                                        │
│   Execute handler method; schedule retry on failure    │
│ If Asynchronous:                                       │
│   Return HTTP 202 Accepted; ir.cron worker processes   │
└────────────────────────────────────────────────────────┘
```

---

## Constant-Time HMAC Verification

Comparing HMAC signatures using standard string equality (`==`) can leak timing information, because string equality checks return early on the first mismatched byte.

Python's `hmac.compare_digest` prevents timing differences by comparing bytes in constant time:

```python
import hashlib
import hmac


def verify_hmac(secret: str, raw_body: bytes, header_signature: str) -> bool:
    if not header_signature or not secret:
        return False

    # Strip prefixes like sha256= (used by Stripe and GitHub)
    signature = header_signature.split('=', 1)[1] if '=' in header_signature else header_signature
    
    secret_bytes = secret.encode('utf-8')
    computed = hmac.new(secret_bytes, raw_body, hashlib.sha256).hexdigest()

    return hmac.compare_digest(computed, signature)
```

---

## Database-Level Deduplication

In-memory stores like Redis can drop keys or restart. Enforcing a unique constraint directly in PostgreSQL ensures deduplication remains consistent even across concurrent worker processes.

In Odoo, define a composite unique constraint across the endpoint and the idempotency key:

```python
class WebhookEvent(models.Model):
    _name = 'webhook.event'
    _description = 'Inbound Webhook Audit Record'

    endpoint_id = fields.Many2one('webhook.endpoint', required=True, index=True)
    idempotency_key = fields.Char(string='Idempotency Key', index=True, copy=False)
    state = fields.Selection([
        ('pending', 'Queued'),
        ('done', 'Completed'),
        ('failed', 'Failed (Retryable)'),
        ('dead_letter', 'Dead Letter Queue'),
        ('ignored', 'Ignored Duplicate'),
    ], default='pending', required=True)

    _sql_constraints = [
        (
            'endpoint_idempotency_unique',
            'unique(endpoint_id, idempotency_key)',
            'An event with this idempotency key has already been received.'
        ),
    ]
```

When an event arrives:
- Check if `(endpoint_id, idempotency_key)` exists in the database.
- If it does, return `HTTP 200 OK` with `status: "duplicate_ignored"` immediately. The sender considers the delivery complete, and your business logic runs only once.

---

## Exponential Backoff and Dead Letter Queue

Temporary failures (like database lock contention or transient third-party rate limits) should be retried automatically. Permanent failures (like schema validation errors or missing fields) should be set aside so they don't block the queue.

We use scheduled actions (`ir.cron`) to run retries with exponential backoff:

```python
import datetime
from odoo import fields, models


class WebhookEvent(models.Model):
    _inherit = 'webhook.event'

    retry_count = fields.Integer(default=0)
    next_retry_time = fields.Datetime()
    error_message = fields.Text()

    def schedule_retry_or_dead_letter(self, exception):
        self.ensure_one()
        self.retry_count += 1
        
        if self.retry_count >= self.endpoint_id.max_retries:
            # Move to Dead Letter Queue for inspection
            self.write({
                'state': 'dead_letter',
                'error_message': str(exception),
                'next_retry_time': False,
            })
        else:
            # Exponential backoff: 2m, 4m, 8m, 16m...
            backoff_seconds = (2 ** self.retry_count) * 60
            self.write({
                'state': 'failed',
                'error_message': str(exception),
                'next_retry_time': fields.Datetime.now() + datetime.timedelta(seconds=backoff_seconds),
            })
```

Moving permanently failed payloads into a Dead Letter Queue view in the Odoo backend gives developers a place to inspect stack traces, fix any mapping bugs, and manually re-trigger the webhook handler once resolved.
