# Async Webhook Dispatcher & Dead-Letter Queue (DLQ)

A resilient, high-throughput asynchronous microservice designed for reliable third-party webhook delivery. Built with Python, `asyncio`, `aiohttp`, and SQLite, it features configurable exponential backoff retry mechanisms, cryptographic payload signing (HMAC-SHA256), and a persistent Dead-Letter Queue (DLQ) to ensure zero message loss.

---

## Architecture Overview & Capabilities

```
[ Client / Producer ] 
         │ (HTTP POST)
         ▼
[ Async Dispatcher ] ──(Immediate Attempt)──► [ Target Endpoint ]
         │                                            │
         │ (On Failure / Rate Limit)                  │ (Success)
         ▼                                            ▼
[ Exponential Backoff Scheduler ]             [ Delivery Complete ]
         │
         ├──► [ Max Retries Reached? ] ──Yes──► [ Persistent DLQ (SQLite) ]
         └──► [ Retry Again ] ──────────────────────┘
```

### Core Capabilities
* **Asynchronous Execution:** Built on `asyncio` and `aiohttp` for non-blocking concurrent network operations.
* **Resilient Retries:** Implements customizable exponential backoff with jitter capability (`initial_delay * (backoff_factor ** current_retry)`).
* **Payload Integrity & Security:** Automatically signs outgoing requests using HMAC-SHA256 (`X-Hub-Signature-256`) when a secret is provided.
* **Persistent Dead-Letter Queue (DLQ):** Failed events that exhaust their retry lifecycle are persisted to disk via SQLite, ensuring durability across service restarts.
* **Observability:** Structured logging throughout the dispatch and retry lifecycle for easy integration with monitoring agents (Datadog, Prometheus, etc.).

---

## API Endpoints

### 1. Dispatch Webhook
Queue a webhook event for asynchronous delivery.

* **URL:** `/webhook`
* **Method:** `POST`
* **Content-Type:** `application/json`

**Example Request (`curl`):**
```bash
curl -X POST http://localhost:8000/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://httpbin.org/post",
    "payload": {"event": "user.created", "user_id": 42},
    "secret": "whsec_supersecretkey123",
    "headers": {"X-Custom-Header": "value"},
    "max_retries": 3,
    "backoff_factor": 2.0
  }'
```

**Example Response:**
```json
{
  "status": "queued",
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef0123456789",
  "message": "Webhook accepted for delivery."
}
```

### 2. Inspect Dead-Letter Queue (DLQ)
Retrieve failed webhook events that have exhausted their retry attempts.

* **URL:** `/dlq`
* **Method:** `GET`

**Example Request (`curl`):**
```bash
curl -X GET http://localhost:8000/dlq
```

**Example Response:**
```json
{
  "total_failed": 1,
  "events": [
    {
      "id": "a1b2c3d4-e5f6-7890-abcd-ef0123456789",
      "url": "https://unreliable-endpoint.local/hook",
      "payload": {"event": "payment.failed", "invoice_id": "inv_99"},
      "current_retry": 3,
      "max_retries": 3,
      "last_error": "ClientConnectorError: Cannot connect to host unreliable-endpoint.local:443",
      "created_at": 1698000000.0
    }
  ]
}
```

---

## Native Execution Instructions

### Prerequisites
* Python 3.10+
* `pip`

### Step 1: Clone and Setup Virtual Environment
```bash
git clone https://github.com/your-org/async-webhook-dispatcher.git
cd async-webhook-dispatcher

python3 -m venv venv
source venv/bin/activate
```

### Step 2: Install Dependencies
```bash
pip install aiohttp
```

### Step 3: Run the Service
```bash
python main.py
```
*(Note: Ensure your application entrypoint script binds to your desired host/port if wrapping with an ASGI/WSGI or custom runner).*

---

## Docker Execution Instructions

### Prerequisites
* Docker engine installed on your host machine.

### Step 1: Build the Docker Image
Create a `Dockerfile` in your project root (if not already present) and run:
```bash
docker build -t webhook-dispatcher:latest .
```

### Step 2: Run the Container
Mount a local volume for the SQLite DLQ database to persist failures across container restarts:
```bash
docker run -d \
  --name webhook-dispatcher \
  -p 8000:8000 \
  -v $(pwd)/data:/app/data \
  -e DLQ_DB_PATH="/app/data/dlq.db" \
  webhook-dispatcher:latest
```

---

## Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PORT` | Port for the dispatcher service to listen on | `8000` |
| `DLQ_DB_PATH` | File path for the SQLite Dead-Letter Queue database | `dlq.db` |
| `LOG_LEVEL` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`) | `INFO` |

---

## License
[MIT License](LICENSE)