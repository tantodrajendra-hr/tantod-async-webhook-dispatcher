import asyncio
import aiohttp
import logging
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("WebhookDispatcher")

@dataclass
class WebhookTask:
    url: str
    payload: Dict[str, Any]
    headers: Optional[Dict[str, str]] = None
    max_retries: int = 5
    initial_backoff: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff: float = 60.0
    attempt: int = 0
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: float = field(default_factory=time.time)

class DeadLetterQueue:
    def __init__(self, storage_path: str = "dlq.json", max_age_seconds: float = 86400.0):
        self._storage: List[WebhookTask] = []
        self._lock = asyncio.Lock()
        self._storage_path = storage_path
        self._max_age_seconds = max_age_seconds

    async def load(self):
        async with self._lock:
            try:
                def _read_file():
                    with open(self._storage_path, "r") as f:
                        return json.load(f)
                
                data = await asyncio.to_thread(_read_file)
                self._storage = [WebhookTask(**d) for d in data]
                logger.info(f"Loaded {len(self._storage)} items from DLQ storage.")
            except FileNotFoundError:
                logger.info("No existing DLQ storage file found. Starting fresh.")
            except Exception as e:
                logger.error(f"Failed to load DLQ storage: {e}")

    async def _save(self):
        try:
            def _write_file():
                with open(self._storage_path, "w") as f:
                    json.dump([task.__dict__ for task in self._storage], f)
            
            await asyncio.to_thread(_write_file)
        except Exception as e:
            logger.error(f"Failed to save DLQ storage: {e}")

    async def add(self, task: WebhookTask, reason: str):
        async with self._lock:
            logger.error(f"Task {task.task_id} moved to DLQ. URL: {task.url}. Reason: {reason}")
            self._storage.append(task)
            await self._save()

    async def get_all(self) -> List[WebhookTask]:
        async with self._lock:
            return list(self._storage)

    async def cleanup(self):
        async with self._lock:
            now = time.time()
            initial_count = len(self._storage)
            self._storage = [
                task for task in self._storage
                if now - task.created_at < self._max_age_seconds
            ]
            if len(self._storage) < initial_count:
                logger.info(f"Cleaned up {initial_count - len(self._storage)} expired tasks from DLQ.")
                await self._save()

class WebhookDispatcher:
    def __init__(self, concurrency_limit: int = 10, dlq_storage_path: str = "dlq.json"):
        self.queue: asyncio.Queue[WebhookTask] = asyncio.Queue()
        self.dlq = DeadLetterQueue(storage_path=dlq_storage_path)
        self.concurrency_limit = concurrency_limit
        self._workers: List[asyncio.Task] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._task_lock = asyncio.Lock()

    async def start(self):
        if self._running:
            return
        self._running = True
        
        await self.dlq.load()
        
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        )
        for i in range(self.concurrency_limit):
            worker = asyncio.create_task(self._worker(f"Worker-{i}"))
            self._workers.append(worker)
        logger.info(f"WebhookDispatcher started with {self.concurrency_limit} workers.")

    async def stop(self):
        if not self._running:
            return
        self._running = False
        
        for _ in self._workers:
            await self.queue.put(None) # Sentinel to wake up workers
            
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

        if self._session:
            await self._session.close()
            self._session = None
            
        logger.info("WebhookDispatcher stopped.")

    async def dispatch(self, url: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None, **kwargs):
        task = WebhookTask(url=url, payload=payload, headers=headers, **kwargs)
        await self.queue.put(task)
        logger.debug(f"Enqueued webhook task {task.task_id} for {url}")

    async def _worker(self, name: str):
        logger.debug(f"Dispatcher worker {name} started.")
        while self._running:
            task = await self.queue.get()
            if task is None:
                self.queue.task_done()
                break

            try:
                await self._process_task(task)
            except Exception as e:
                logger.exception(f"Unexpected error in worker {name} processing task {task.task_id}: {e}")
            finally:
                self.queue.task_done()
        logger.debug(f"Dispatcher worker {name} stopped.")

    async def _process_task(self, task: WebhookTask):
        assert self._session is not None, "Session is not initialized"
        
        headers = {"Content-Type": "application/json"}
        if task.headers:
            headers.update(task.headers)

        while task.attempt < task.max_retries:
            async with self._task_lock:
                task.attempt += 1
                
            try:
                async with self._session.post(task.url, json=task.payload, headers=headers) as response:
                    if 200 <= response.status < 300:
                        logger.info(f"Successfully delivered webhook {task.task_id} to {task.url} on attempt {task.attempt}")
                        return
                    else:
                        response_body = await response.text()
                        logger.warning(
                            f"Webhook {task.task_id} failed with status {response.status} "
                            f"on attempt {task.attempt}/{task.max_retries}. Response: {response_body[:200]}"
                        )
                        if 400 <= response.status < 500 and response.status != 429:
                            await self.dlq.add(task, f"Client error HTTP {response.status}")
                            return
            
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(
                    f"Webhook {task.task_id} encountered network error '{str(e)}' "
                    f"on attempt {task.attempt}/{task.max_retries}"
                )

            if task.attempt >= task.max_retries:
                break

            backoff = min(
                task.initial_backoff * (task.backoff_multiplier ** (task.attempt - 1)),
                task.max_backoff
            )
            logger.info(f"Retrying webhook {task.task_id} in {backoff:.2f} seconds...")
            await asyncio.sleep(backoff)

        await self.dlq.add(task, f"Exhausted max retries ({task.max_retries})")

async def main():
    dispatcher = WebhookDispatcher(concurrency_limit=5)
    await dispatcher.start()

    server = await asyncio.start_server(
        handle_mock_webhook, '127.0.0.1', 8080
    )
    
    server_task = asyncio.create_task(server.serve_forever())

    target_url = "http://127.0.0.1:8080/webhook"

    logger.info("Dispatching test webhooks...")
    
    await dispatcher.dispatch(
        url=target_url,
        payload={"event": "user.created", "user_id": 42},
        max_retries=3
    )

    await dispatcher.dispatch(
        url="http://127.0.0.1:8080/fail",
        payload={"event": "system.error"},
        max_retries=2,
        initial_backoff=0.1
    )

    await dispatcher.queue.join()
    
    await asyncio.sleep(1)

    # Trigger periodic DLQ cleanup test
    await dispatcher.dlq.cleanup()

    dlq_items = await dispatcher.dlq.get_all()
    logger.info(f"Items in DLQ: {len(dlq_items)}")
    for item in dlq_items:
        logger.info(f"DLQ Item -> ID: {item.task_id}, URL: {item.url}, Attempts: {item.attempt}")

    server.close()
    await server.wait_closed()
    await server_task
    await dispatcher.stop()

async def handle_mock_webhook(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    request_line = await reader.readline()
    if not request_line:
        writer.close()
        return
        
    method, path, _ = request_line.decode().split()
    
    headers = {}
    while True:
        line = await reader.readline()
        if line == b'\r\n' or not line:
            break
        key, value = line.decode().split(':', 1)
        headers[key.strip().lower()] = value.strip()
        
    content_length = int(headers.get('content-length', 0))
    body = await reader.read(content_length)
    
    if path == "/webhook":
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
    elif path == "/fail":
        writer.write(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 5\r\n\r\nError")
    else:
        writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found")
        
    await writer.drain()
    writer.close()
    await writer.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())