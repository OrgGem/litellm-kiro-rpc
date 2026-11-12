import asyncio
import contextlib

from litellm import CustomLLM, ModelResponse
from litellm.llms.custom_llm import CustomLLMError

from .contracts import GatewayError, Request


class ProviderError(CustomLLMError):
    def __init__(self, error):
        self.gateway_error = error
        super().__init__(status_code=error.status, message=error.code)


class KiroLLM(CustomLLM):
    def __init__(self, runtime):
        super().__init__()
        self.runtime = runtime

    def _request(self, model, messages, optional_params=None, request_id=None):
        params = dict(optional_params or {})
        if params.pop("max_retries", 0) != 0:
            raise GatewayError("retries_not_supported", 400)
        cid = params.pop("conversation_id", None)
        return Request.parse(
            {"model": model, "messages": messages, **params},
            cid,
            self.runtime.backend.config,
        )

    def events(self, request):
        job = self.runtime.submit(self.runtime.start(request)).result()
        try:
            while (
                event := self.runtime.submit(self.runtime.next(job)).result()
            ) is not None:
                yield event
        finally:
            self.runtime.release(job).result()

    async def aevents(self, request):
        # If cancelled while start is completing, recover and cancel that job too.
        start = self.runtime.submit(self.runtime.start(request))
        try:
            job = await asyncio.shield(asyncio.wrap_future(start))
        except asyncio.CancelledError:
            job = await asyncio.wrap_future(start)
            await asyncio.wrap_future(self.runtime.release(job))
            raise
        try:
            while (
                event := await asyncio.wrap_future(
                    self.runtime.submit(self.runtime.next(job))
                )
            ) is not None:
                yield event
        finally:
            await asyncio.wrap_future(self.runtime.release(job))

    @staticmethod
    def response(model, text, terminal):
        return ModelResponse(
            model=model,
            choices=[
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": text},
                    "finish_reason": "stop",
                }
            ],
            usage=terminal.usage,
        )

    def completion(self, model, messages, *args, optional_params=None, **kwargs):
        try:
            request = self._request(model, messages, optional_params)
            text, terminal = [], None
            with contextlib.closing(self.events(request)) as events:
                for event in events:
                    if event.done:
                        terminal = event
                    else:
                        text.append(event.text)
            return self.response(model, "".join(text), terminal)
        except GatewayError as exc:
            raise ProviderError(exc) from None

    async def acompletion(self, model, messages, *args, optional_params=None, **kwargs):
        try:
            request = self._request(model, messages, optional_params)
            text, terminal = [], None
            async with contextlib.aclosing(self.aevents(request)) as events:
                async for event in events:
                    if event.done:
                        terminal = event
                    else:
                        text.append(event.text)
            return self.response(model, "".join(text), terminal)
        except GatewayError as exc:
            raise ProviderError(exc) from None

    @staticmethod
    def chunk(event):
        return {
            "text": event.text,
            "is_finished": event.done,
            "finish_reason": "stop" if event.done else "",
            "index": 0,
            "usage": event.usage,
        }

    def _streaming(self, model, messages, *args, optional_params=None, **kwargs):
        try:
            request = self._request(model, messages, optional_params)
            with contextlib.closing(self.events(request)) as events:
                for event in events:
                    yield self.chunk(event)
        except GatewayError as exc:
            raise ProviderError(exc) from None

    async def _astreaming(self, model, messages, *args, optional_params=None, **kwargs):
        try:
            request = self._request(model, messages, optional_params)
            async with contextlib.aclosing(self.aevents(request)) as events:
                async for event in events:
                    yield self.chunk(event)
        except GatewayError as exc:
            raise ProviderError(exc) from None

    def streaming(self, *args, **kwargs):
        self._suppress_estimated_usage(kwargs)
        return self._streaming(*args, **kwargs)

    def astreaming(self, *args, **kwargs):
        self._suppress_estimated_usage(kwargs)
        return self._astreaming(*args, **kwargs)

    @staticmethod
    def _suppress_estimated_usage(kwargs):
        # LiteLLM 1.109 synthesizes a token estimate on stream exhaustion when
        # include_usage is true. Disable that wrapper feature before it is built.
        # Direct provider events still carry real counters; HTTP host is preferred
        # when explicit null usage chunks are needed.
        logging_obj = kwargs.get("logging_obj")
        if logging_obj is not None:
            logging_obj.stream_options = {"include_usage": False}
