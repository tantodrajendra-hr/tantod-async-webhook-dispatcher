import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
import aiofiles
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any
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
    DEAD = "DEAD"

@dataclass
class WebhookEvent:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    url: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    secret: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    max_retries: int = 5
    initial_backoff: float = 1.0
    backoff_factor: int = 2
    timeout: float = 5.0
    attempts: int = 0
    status: WebhookStatus = WebhookStatus.PENDING
    last_attempt_at: Optional[float] = None
    next_attempt_at: Optional[float] = None
    last_error: Optional[str] = None
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WebhookEvent":
        if "status" in data and isinstance(data["status"], str):
            data["status"] = WebhookStatus(data["status"])
        return cls(**data)

class DeadLetterQueue:
    def __init__(self, persistence_file: Optional[str] = None, max_size: int = 10000, ttl_seconds: Optional[float] = 604800):
        self._queue: List[WebhookEvent] = []
        self._lock = asyncio.Lock()
        self.persistence_file = persistence_file
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds

    async def initialize(self) -> None:
        if self.persistence_file:
            await self._load_from_file()

    async def _load_from_file(self) -> None:
        async with self._lock:
            try:
                async with aiofiles.open(self.persistence_file, 'r') as f:
                    content = await f.read()
                    if content.strip():
                        raw_data = json.loads(content)
                        self._queue = [WebhookEvent.from_dict(item) for item in raw_data]
                        logger.info(f"Loaded {len(self._queue)} events from DLQ persistence file.")
            except FileNotFoundError:
                logger.info("No existing DLQ persistence file found. Starting fresh.")
            except Exception as e:
                logger.exception(f"Failed to load DLQ from file: {e}")

    async def save_to_file(self) -> None:
        if not self.persistence_file:
            return
        async with self._lock:
            try:
                data = [event.to_dict() for event in self._queue]
                async with aiofiles.open(self.persistence_file, 'w') as f:
                    await f.write(json.dumps(data, default=str))
            except Exception as e:
                logger.exception(f"Failed to save DLQ to file: {e}")

    async def add(self, event: WebhookEvent) -> None:
        async with self._lock:
            event.status = WebhookStatus.DEAD
            self._queue.append(event)
            
            # Enforce max size constraint (Memory Leak mitigation)
            if len(self._queue) > self.max_size:
                dropped = self._queue.pop(0)
                logger.warning(f"DLQ exceeded max size. Dropped oldest event {dropped.id}")

            logger.error(f"Event {event.id} moved to DLQ for URL {event.url}. Total failures: {event.attempts}")
        
        await self.save_to_file()

    async def get_all(self) -> List[WebhookEvent]:
        async with self._lock:
            return list(self._queue)

    async def clear(self) -> None:
        async with self._lock:
            self._queue.clear()
        await self.save_to_file()

    async def prune_expired(self) -> None:
        if not self.ttl_seconds:
            return
        async with self._lock:
            now = time.time()
            original_len = len(self._queue)
            self._queue = [
                event for event in self._queue 
                if (now - event.created_at) < self.ttl_seconds
            ]
            pruned_count = original_len - len(self._queue)
            if pruned_count > 0:
                logger.info(f"Pruned {pruned_count} expired events from DLQ.")
        if pruned_count > 0:
            await self.save_to_file()

class WebhookDispatcher:
    def __init__(self, dlq: Optional[DeadLetterQueue] = None, concurrency_limit: int = 10):
        self.dlq = dlq or DeadLetterQueue()
        self.concurrency_limit = concurrency_limit
        self._pending_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._semaphore = asyncio.Semaphore(concurrency_limit)
        self._session: Optional[aiohttp.ClientSession] = None
        self._worker_task: Optional[asyncio.Task] = None
        self._maintenance_task: Optional[asyncio.Task] = None
        self._running = False
        self._state_lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._state_lock:
            if self._running:
                return
            await self.dlq.initialize()
            self._session = aiohttp.ClientSession()
            self._running = True
            self._worker_task = asyncio.create_task(self._dispatch_loop())
            self._maintenance_task = asyncio.create_task(self._maintenance_loop())
            logger.info("WebhookDispatcher started.")

    async def stop(self) -> None:
        async with self._state_lock:
            if not self._running:
                return
            self._running = False

            if self._worker_task:
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass

            if self._maintenance_task:
                self._maintenance_task.cancel()
                try:
                    await self._maintenance_task
                except asyncio.CancelledError:
                    pass

            if self._session:
                await self._session.close()
                self._session = None

            # Clear pending queue to prevent memory retention
            while not self._pending_queue.empty():
                try:
                    self._pending_queue.get_nowait()
                    self._pending_queue.task_done()
                except asyncio.QueueEmpty:
                    break
                except ValueError:
                    pass

            logger.info("WebhookDispatcher stopped.")

    async def enqueue(self, event: WebhookEvent) -> None:
        async with self._state_lock:
            if not self._running:
                raise RuntimeError("Cannot enqueue events on a stopped WebhookDispatcher.")
        event.status = WebhookStatus.PENDING
        event.next_attempt_at = time.time()
        await self._pending_queue.put((event.next_attempt_at, event))
        logger.info(f"Enqueued webhook event {event.id} for {event.url}")

    async def _dispatch_loop(self) -> None:
        while self._running:
            try:
                try:
                    next_attempt_at, event = await asyncio.wait_for(
                        self._pending_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                if event.status == WebhookStatus.DEAD:
                    self._pending_queue.task_done()
                    continue

                now = time.time()
                if next_attempt_at > now:
                    await self._pending_queue.put((next_attempt_at, event))
                    self._pending_queue.task_done()
                    await asyncio.sleep(min(next_attempt_at - now, 0.5))
                    continue

                asyncio.create_task(self._process_event(event))
                self._pending_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error in dispatch loop: {e}")
                await asyncio.sleep(1.0)

    async def _maintenance_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(300)  # Run maintenance every 5 minutes
                await self.dlq.prune_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error in maintenance loop: {e}")

    async def _process_event(self, event: WebhookEvent) -> None:
        async with self._semaphore:
            event.attempts += 1
            event.last_attempt_at = time.time()
            
            success, error_msg = await self._send_request(event)
            
            if success:
                event.status = WebhookStatus.SUCCESS
                logger.info(f"Successfully delivered webhook {event.id} to {event.url} on attempt {event.attempts}")
            else:
                event.last_error = error_msg
                if event.attempts >= event.max_retries:
                    await self.dlq.add(event)
                else:
                    event.status = WebhookStatus.RETRYING
                    backoff = event.initial_backoff * (event.backoff_factor ** (event.attempts - 1))
                    event.next_attempt_at = time.time() + backoff
                    logger.warning(
                        f"Webhook {event.id} failed (Attempt {event.attempts}/{event.max_retries}). "
                        f"Retrying in {backoff:.2f}s. Error: {error_msg}"
                    )
                    await self._pending_queue.put((event.next_attempt_at, event))

    async def _send_request(self, event: WebhookEvent) -> tuple[bool, Optional[str]]:
        async with self._state_lock:
            if not self._session:
                return False, "Session not initialized"
            session = self._session

        body = json.dumps(event.payload)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "WebhookDispatcher/1.0",
            **event.headers
        }

        if event.secret:
            signature = hmac.new(
                event.secret.encode("utf-8"),
                body.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()
            headers["X-Webhook-Signature"] = f"sha256={signature}"

        try:
            async with session.post(
                event.url,
                data=body,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=event.timeout)
            ) as response:
                if 200 <= response.status < 300:
                    return True, None
                else:
                    response_text = await response.text()
                    return False, f"HTTP Status {response.status}: {response_text}"
        except asyncio.TimeoutError:
            logger.error(f"Request timeout for event {event.id} targeting {event.url}")
            return False, "Request timeout"
        except aiohttp.ClientError as e:
            return False, f"Client error: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"

async def main():
    dlq = DeadLetterQueue(persistence_file="dlq_persistence.json")
    dispatcher = WebhookDispatcher(dlq=dlq, concurrency_limit=5)
    await dispatcher.start()

    event1 = WebhookEvent(
        url="http://httpbin.org/status/500",
        payload={"message": "Hello World"},
        secret="my_super_secret_key",
        max_retries=3,
        initial_backoff=0.1
    )

    event2 = WebhookEvent(
        url="http://httpbin.org/status/200",
        payload={"message": "Success path"},
        max_retries=3,
        initial_backoff=0.1
    )

    await dispatcher.enqueue(event1)
    await dispatcher.enqueue(event2)

    await asyncio.sleep(3.0)

    await dispatcher.stop()
    
    dead_events = await dlq.get_all()
    print(f"Total dead-letter events: {len(dead_events)}")

if __name__ == "__main__":
    asyncio.run(main())