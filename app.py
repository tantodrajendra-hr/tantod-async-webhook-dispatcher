import asyncio
import dataclasses
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import aiohttp

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("WebhookDispatcher")

@dataclass(frozen=True)
class WebhookTask:
    url: str
    payload: dict
    headers: dict = field(default_factory=dict)
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    attempt: int = 0
    next_attempt_at: float = field(default_factory=time.time)
    ttl: float = 86400.0  # 24 hours TTL for DLQ persistence cleanup

class DeadLetterQueue:
    def __init__(self, persistence_file: str = "dlq.json"):
        self._dlq: List[WebhookTask] = []
        self._lock = asyncio.Lock()
        self._persistence_file = persistence_file
        self._load_from_file()

    def _load_from_file(self):
        if not os.path.exists(self._persistence_file):
            return
        try:
            with open(self._persistence_file, "r") as f:
                data = json.load(f)
                current_time = time.time()
                loaded_tasks = []
                for item in data:
                    task = WebhookTask(**item)
                    # Filter out tasks that have exceeded their TTL
                    if current_time - task.next_attempt_at < task.ttl:
                        loaded_tasks.append(task)
                self._dlq = loaded_tasks
                logger.info(f"Loaded {len(self._dlq)} tasks from DLQ storage.")
        except Exception as e:
            logger.error(f"Failed to load DLQ from file: {e}")

    def _save_to_file_unlocked(self):
        try:
            current_time = time.time()
            # Clean expired tasks before saving
            self._dlq = [task for task in self._dlq if current_time - task.next_attempt_at < task.ttl]
            data = [dataclasses.asdict(task) for task in self._dlq]
            with open(self._persistence_file, "w") as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Failed to save DLQ to file: {e}")

    async def add(self, task: WebhookTask, reason: str):
        async with self._lock:
            logger.error(f"Task {task.id} moved to DLQ. Reason: {reason}. URL: {task.url}")
            self._dlq.append(task)
            self._save_to_file_unlocked()

    async def get_all(self) -> List[WebhookTask]:
        async with self._lock:
            current_time = time.time()
            self._dlq = [task for task in self._dlq if current_time - task.next_attempt_at < task.ttl]
            return list(self._dlq)

class WebhookDispatcher:
    def __init__(self, concurrency: int = 10, max_queue_size: int = 1000):
        # Bounded PriorityQueue to prevent unbounded memory growth
        self.queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_queue_size)
        self.dlq = DeadLetterQueue()
        self.concurrency = concurrency
        self._workers: List[asyncio.Task] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False

    async def start(self):
        if self._running:
            return
        self._running = True
        self._session = aiohttp.ClientSession()
        self._workers = [
            asyncio.create_task(self._worker(f"Worker-{i}"))
            for i in range(self.concurrency)
        ]
        logger.info(f"WebhookDispatcher started with {self.concurrency} workers.")

    async def stop(self):
        if not self._running:
            return
        self._running = False
        
        for worker in self._workers:
            worker.cancel()
        
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
            
        if self._session:
            await self._session.close()
            
        logger.info("WebhookDispatcher stopped.")

    async def dispatch(self, task: WebhookTask):
        await self.queue.put((task.next_attempt_at, task))
        logger.debug(f"Disqueued task {task.id} for {task.url}")

    async def _worker(self, name: str):
        logger.debug(f"{name} started.")
        while self._running:
            task = None
            try:
                try:
                    next_attempt_at, task = await asyncio.wait_for(
                        self.queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                now = time.time()
                if now < next_attempt_at:
                    sleep_duration = next_attempt_at - now
                    await asyncio.sleep(sleep_duration)

                success = await self._send_request(task)
                
                if success:
                    self.queue.task_done()
                    logger.info(f"Task {task.id} successfully delivered to {task.url}")
                else:
                    new_attempt = task.attempt + 1
                    if new_attempt > task.max_retries:
                        self.queue.task_done()
                        await self.dlq.add(task, f"Exceeded max retries ({task.max_retries})")
                    else:
                        delay = min(task.base_delay * (2 ** (new_attempt - 1)), task.max_delay)
                        next_time = time.time() + delay
                        # Create an immutable new task object for retry to prevent shared state/race conditions
                        new_task = dataclasses.replace(
                            task,
                            attempt=new_attempt,
                            next_attempt_at=next_time
                        )
                        logger.warning(
                            f"Task {task.id} failed attempt {new_task.attempt}. "
                            f"Retrying in {delay:.2f}s."
                        )
                        await self.queue.put((new_task.next_attempt_at, new_task))
                        self.queue.task_done()

            except asyncio.CancelledError:
                if task is not None:
                    try:
                        self.queue.task_done()
                    except ValueError:
                        pass
                break
            except Exception as e:
                if task is not None:
                    try:
                        self.queue.task_done()
                    except ValueError:
                        pass
                logger.exception(f"{name} encountered an unexpected error: {e}")

    async def _send_request(self, task: WebhookTask) -> bool:
        if not self._session:
            return False
        
        default_headers = {"Content-Type": "application/json", "User-Agent": "WebhookDispatcher/1.0"}
        headers = {**default_headers, **task.headers}

        try:
            async with self._session.post(
                task.url, json=task.payload, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if 200 <= response.status < 300:
                    return True
                else:
                    logger.warning(
                        f"Task {task.id} received non-success status {response.status} from {task.url}"
                    )
                    return False
        except asyncio.TimeoutError:
            logger.warning(f"Task {task.id} timed out connecting to {task.url}")
            return False
        except aiohttp.ClientError as e:
            logger.warning(f"Task {task.id} network error connecting to {task.url}: {e}")
            return False
        except Exception as e:
            logger.error(f"Task {task.id} unexpected error connecting to {task.url}: {e}")
            return False

async def main():
    dispatcher = WebhookDispatcher(concurrency=5)
    await dispatcher.start()

    server_runner = None
    try:
        try:
            from aiohttp import web
            async def handle(request):
                data = await request.json()
                if data.get("fail_count", 0) > 0:
                    return web.Response(status=500, text="Internal Error")
                return web.Response(status=200, text="OK")

            app = web.Application()
            app.router.add_post("/webhook", handle)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 8080)
            await site.start()
            server_runner = runner
        except Exception:
            server_runner = None

        target_url = "http://127.0.0.1:8080/webhook"
        
        task1 = WebhookTask(url=target_url, payload={"message": "hello world"}, max_retries=2, base_delay=0.1)
        await dispatcher.dispatch(task1)

        task2 = WebhookTask(url="http://127.0.0.1:8080/nonexistent", payload={"message": "will fail completely"}, max_retries=1, base_delay=0.1)
        await dispatcher.dispatch(task2)

        await asyncio.sleep(2.0)

        dlq_items = await dispatcher.dlq.get_all()
        logger.info(f"Total items in DLQ: {len(dlq_items)}")

        if server_runner:
            await server_runner.cleanup()

    finally:
        await dispatcher.stop()
        # Clean up temporary persistence file after run if desired
        if os.path.exists("dlq.json"):
            try:
                os.remove("dlq.json")
            except OSError:
                pass

if __name__ == "__main__":
    asyncio.run(main())