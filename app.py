import asyncio
import hashlib
import hmac
import json
import logging
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional
import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("WebhookDispatcher")

class WebhookStatus(Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    RETRYING = "RETRYING"
    FAILED = "FAILED"

@dataclass
class WebhookEvent:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    url: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    secret: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    max_retries: int = 5
    current_retry: int = 0
    backoff_factor: float = 2.0
    initial_delay: float = 1.0
    status: WebhookStatus = WebhookStatus.PENDING
    created_at: float = field(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    next_retry_at: float = field(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    last_error: Optional[str] = None

class PersistentDeadLetterQueue:
    def __init__(self, db_path: str = "dlq.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dlq (
                    id TEXT PRIMARY KEY,
                    url TEXT,
                    payload TEXT,
                    secret TEXT,
                    headers TEXT,
                    max_retries INTEGER,
                    current_retry INTEGER,
                    backoff_factor REAL,
                    initial_delay REAL,
                    status TEXT,
                    created_at REAL,
                    next_retry_at REAL,
                    last_error TEXT
                )
            """)
            conn.commit()

    async def add(self, event: WebhookEvent, reason: str) -> None:
        event.status = WebhookStatus.FAILED
        event.last_error = reason
        
        def _insert():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO dlq (
                        id, url, payload, secret, headers, max_retries, 
                        current_retry, backoff_factor, initial_delay, 
                        status, created_at, next_retry_at, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event.id,
                    event.url,
                    json.dumps(event.payload),
                    event.secret,
                    json.dumps(event.headers),
                    event.max_retries,
                    event.current_retry,
                    event.backoff_factor,
                    event.initial_delay,
                    event.status.value,
                    event.created_at,
                    event.next_retry_at,
                    event.last_error
                ))
                conn.commit()

        await asyncio.to_thread(_insert)
        logger.error(f"Event {event.id} moved to persistent DLQ. URL: {event.url}. Reason: {reason}")

    def get_all(self) -> List[WebhookEvent]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM dlq")
            rows = cursor.fetchall()
            
        events = []
        for row in rows:
            events.append(WebhookEvent(
                id=row[0],
                url=row[1],
                payload=json.loads(row[2]),
                secret=row[3],
                headers=json.loads(row[4]),
                max_retries=row[5],
                current_retry=row[6],
                backoff_factor=row[7],
                initial_delay=row[8],
                status=WebhookStatus(row[9]),
                created_at=row[10],
                next_retry_at=row[11],
                last_error=row[12]
            ))
        return events

    async def cleanup_old_events(self, older_than_days: int = 7) -> None:
        cutoff = datetime.now(timezone.utc).timestamp() - (older_than_days * 86400)
        def _delete():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM dlq WHERE created_at < ?", (cutoff,))
                conn.commit()
        await asyncio.to_thread(_delete)

class WebhookDispatcher:
    def __init__(self, concurrency_limit: int = 10, request_timeout: float = 10.0, max_queue_size: int = 1000):
        self.concurrency_limit = concurrency_limit
        self.request_timeout = request_timeout
        self.retry_queue: asyncio.Queue[WebhookEvent] = asyncio.Queue(maxsize=max_queue_size)
        self.dlq = PersistentDeadLetterQueue()
        self._semaphore = asyncio.Semaphore(concurrency_limit)
        self._worker_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._is_running = False

    def _generate_signature(self, secret: str, payload_bytes: bytes) -> str:
        signature = hmac.new(
            secret.encode("utf-8"),
            payload_bytes,
            hashlib.sha256
        ).hexdigest()
        return f"sha256={signature}"

    async def dispatch(self, event: WebhookEvent) -> None:
        event.next_retry_at = datetime.now(timezone.utc).timestamp()
        await self.retry_queue.put(event)
        logger.info(f"Queued webhook event {event.id} for {event.url}")

    async def start(self) -> None:
        if self._is_running:
            return
        self._is_running = True
        self._worker_task = asyncio.create_task(self._process_queue())
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
        logger.info("Webhook Dispatcher started.")

    async def stop(self) -> None:
        if not self._is_running:
            return
        self._is_running = False
        
        for task in [self._worker_task, self._cleanup_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        logger.info("Webhook Dispatcher stopped.")

    async def _periodic_cleanup(self) -> None:
        while self._is_running:
            try:
                await asyncio.sleep(3600)  # Run hourly
                await self.dlq.cleanup_old_events(older_than_days=7)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error during periodic cleanup: {e}")

    async def _process_queue(self) -> None:
        connector = aiohttp.TCPConnector(limit=self.concurrency_limit)
        async with aiohttp.ClientSession(connector=connector) as session:
            while self._is_running:
                try:
                    event = await self.retry_queue.get()
                    
                    now = datetime.now(timezone.utc).timestamp()
                    if event.next_retry_at > now:
                        delay = event.next_retry_at - now
                        await asyncio.sleep(min(delay, 0.5))
                        await self.retry_queue.put(event)
                        self.retry_queue.task_done()
                        continue

                    self.retry_queue.task_done()
                    asyncio.create_task(self._send_with_concurrency_control(session, event))

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.exception(f"Unexpected error in dispatcher loop: {e}")
                    await asyncio.sleep(1.0)

    async def _send_with_concurrency_control(self, session: aiohttp.ClientSession, event: WebhookEvent) -> None:
        async with self._semaphore:
            await self._send_webhook(session, event)

    async def _send_webhook(self, session: aiohttp.ClientSession, event: WebhookEvent) -> None:
        payload_str = json.dumps(event.payload)
        payload_bytes = payload_str.encode("utf-8")
        
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "WebhookDispatcher/1.0",
            "X-Webhook-ID": event.id,
            **event.headers
        }

        if event.secret:
            headers["X-Webhook-Signature"] = self._generate_signature(event.secret, payload_bytes)

        try:
            logger.info(f"Sending webhook {event.id} to {event.url} (Attempt {event.current_retry + 1}/{event.max_retries + 1})")
            async with session.post(
                event.url,
                data=payload_bytes,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.request_timeout)
            ) as response:
                if 200 <= response.status < 300:
                    event.status = WebhookStatus.SUCCESS
                    logger.info(f"Webhook {event.id} delivered successfully with status {response.status}")
                    return
                else:
                    response_text = await response.text()
                    raise Exception(f"HTTP Error {response.status}: {response_text}")

        except Exception as e:
            error_msg = str(e)
            logger.warning(f"Webhook {event.id} failed: {error_msg}")
            event.current_retry += 1

            if event.current_retry > event.max_retries:
                await self.dlq.add(event, f"Max retries exceeded. Last error: {error_msg}")
            else:
                event.status = WebhookStatus.RETRYING
                delay = event.initial_delay * (event.backoff_factor ** (event.current_retry - 1))
                event.next_retry_at = datetime.now(timezone.utc).timestamp() + delay
                logger.info(f"Scheduling retry {event.current_retry} for webhook {event.id} in {delay:.2f} seconds.")
                await self.retry_queue.put(event)

async def main():
    dispatcher = WebhookDispatcher(concurrency_limit=5, request_timeout=5.0)
    await dispatcher.start()

    server_task = asyncio.create_task(_start_mock_receiver())
    await asyncio.sleep(0.5)

    event1 = WebhookEvent(
        url="http://localhost:8080/webhook",
        payload={"message": "Hello, World!"},
        secret="my-super-secret",
        max_retries=3,
        initial_delay=0.1
    )

    event2 = WebhookEvent(
        url="http://localhost:8080/fail-always",
        payload={"message": "Doomed webhook"},
        max_retries=2,
        initial_delay=0.1
    )

    await dispatcher.dispatch(event1)
    await dispatcher.dispatch(event2)

    await asyncio.sleep(5.0)

    await dispatcher.stop()
    server_task.cancel()

async def _start_mock_receiver():
    from aiohttp import web

    app = web.Application()

    async def handle_webhook(request: web.Request):
        body = await request.read()
        sig = request.headers.get("X-Webhook-Signature", "")
        logger.info(f"[Mock Receiver] Received webhook! Sig: {sig}, Body: {body.decode()}")
        return web.Response(text="Accepted", status=200)

    async def handle_fail(request: web.Request):
        return web.Response(text="Internal Server Error", status=500)

    app.router.add_post("/webhook", handle_webhook)
    app.router.add_post("/fail-always", handle_fail)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 8080)
    await site.start()
    
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass