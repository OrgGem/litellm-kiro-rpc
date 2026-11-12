"""One event loop owns subprocesses and admission limits across sync/async calls."""

import asyncio
import threading
from dataclasses import dataclass, field

from .contracts import GatewayError


@dataclass(eq=False)
class Job:
    queue: asyncio.Queue
    task: asyncio.Task | None = None
    terminal: object = None
    error: Exception | None = None
    sent_terminal: bool = False
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    delivered: asyncio.Event = field(default_factory=asyncio.Event)


class Runtime:
    def __init__(self, backend):
        self.backend = backend
        self.jobs = set()
        self.closed = False
        self.state_lock = threading.Lock()
        self.started = threading.Event()
        self.thread = threading.Thread(
            target=self._run, name="acp-runtime", daemon=True
        )
        self.thread.start()
        self.started.wait()

    def _run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.started.set()
        try:
            self.loop.run_forever()
        finally:
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()

    def submit(self, coroutine):
        with self.state_lock:
            if self.closed:
                coroutine.close()
                raise GatewayError("shutting_down", 503)
            return asyncio.run_coroutine_threadsafe(coroutine, self.loop)

    async def start(self, request):
        if len(self.jobs) >= self.backend.config.max_pending:
            raise GatewayError("queue_full", 429)
        job = Job(asyncio.Queue(self.backend.config.queue_size))
        self.jobs.add(job)

        async def produce():
            try:
                async with self.backend.turn(request) as turn:
                    async for event in turn:
                        if event.done:
                            job.terminal = event
                        else:
                            try:
                                job.queue.put_nowait(event)
                            except asyncio.QueueFull:
                                raise GatewayError("slow_consumer") from None
                    if job.terminal is None:
                        raise GatewayError("missing_completion")
                    job.finished.set()
                    await job.delivered.wait()
                    turn.commit()
            except asyncio.CancelledError:
                job.error = GatewayError("request_cancelled", 499)
            except GatewayError as exc:
                job.error = exc
            except Exception:
                job.error = GatewayError("internal_error", 500)
            finally:
                job.finished.set()

        job.task = asyncio.create_task(produce())
        return job

    async def next(self, job):
        if job.error:
            raise job.error
        if not job.queue.empty():
            return job.queue.get_nowait()
        if job.finished.is_set():
            if not job.sent_terminal:
                job.sent_terminal = True
                return job.terminal
            job.delivered.set()
            await job.task
            if job.error:
                raise job.error
            return None
        get = asyncio.create_task(job.queue.get())
        finish = asyncio.create_task(job.finished.wait())
        try:
            done, _ = await asyncio.wait(
                {get, finish}, return_when=asyncio.FIRST_COMPLETED
            )
            if job.error:
                raise job.error
            if get in done:
                return get.result()
            return await self.next(job)
        finally:
            get.cancel()
            finish.cancel()
            await asyncio.gather(get, finish, return_exceptions=True)

    async def cancel(self, job):
        if job.task and not job.task.done():
            job.task.cancel()
        if job.task:
            await asyncio.gather(job.task, return_exceptions=True)
        self.jobs.discard(job)

    async def _shutdown(self):
        await asyncio.gather(*(self.cancel(job) for job in list(self.jobs)))
        await self.backend.close()

    def release(self, job):
        # A caller unwinding during shutdown waits for the cleanup already in flight.
        with self.state_lock:
            if self.closed:
                return self.shutdown
            return asyncio.run_coroutine_threadsafe(self.cancel(job), self.loop)

    def close(self):
        with self.state_lock:
            if self.closed:
                return
            self.closed = True
            self.shutdown = asyncio.run_coroutine_threadsafe(
                self._shutdown(), self.loop
            )
        try:
            self.shutdown.result(timeout=self.backend.config.cleanup_timeout * 5 + 5)
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(timeout=5)
