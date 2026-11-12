import asyncio

import pytest
from test_sessions import Agent

from litellm_kiro_rpc.contracts import Config
from litellm_kiro_rpc.provider import KiroLLM
from litellm_kiro_rpc.runtime import Runtime
from litellm_kiro_rpc.sessions import SessionPool


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_provider_entrypoints(stream, asynchronous):
    runtime = Runtime(SessionPool(Config(), Agent))
    provider = KiroLLM(runtime)
    args = {
        "model": "kiro-default",
        "messages": [{"role": "user", "content": "hello"}],
    }
    try:
        if asynchronous:

            async def run():
                if stream:
                    return [chunk async for chunk in provider.astreaming(**args)]
                return await provider.acompletion(**args)

            result = asyncio.run(run())
        else:
            result = (
                list(provider.streaming(**args))
                if stream
                else provider.completion(**args)
            )
        if stream:
            assert result[-1]["is_finished"] is True
            assert result[0]["text"].startswith("turn-1:")
        else:
            assert result.choices[0].message.content.startswith("turn-1:")
    finally:
        runtime.close()


@pytest.mark.parametrize("asynchronous", [False, True])
def test_real_litellm_does_not_estimate_unknown_stream_usage(asynchronous):
    import litellm

    runtime = Runtime(SessionPool(Config(), Agent))
    name = "fixture_kiro_rpc"
    previous = list(litellm.custom_provider_map)
    litellm.custom_provider_map.append(
        {"provider": name, "custom_handler": KiroLLM(runtime)}
    )
    litellm._custom_providers.append(name)
    try:
        kwargs = {
            "model": name + "/kiro-default",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        if asynchronous:

            async def run():
                response = await litellm.acompletion(**kwargs)
                return [chunk async for chunk in response]

            chunks = asyncio.run(run())
        else:
            chunks = list(litellm.completion(**kwargs))
        assert chunks
        assert all(getattr(chunk, "usage", None) is None for chunk in chunks)
    finally:
        litellm.custom_provider_map = previous
        litellm._custom_providers.remove(name)
        runtime.close()
