import asyncio
import os
import sys

import pytest

from litellm_kiro_rpc.contracts import Config, GatewayError
from litellm_kiro_rpc.transport import ACPClient


@pytest.mark.parametrize("bad", [False, True])
async def test_real_stdio_correlation_and_process_cleanup(tmp_path, bad):
    fixture = tmp_path / "peer.py"
    fixture.write_text(
        """import sys, json, time
request = json.loads(sys.stdin.readline())
print(json.dumps({"jsonrpc":"2.0","id":request["id"] + BAD,"result":{"protocolVersion":1}}), flush=True)
time.sleep(120)
""".replace("BAD", "1" if bad else "0")
    )
    client = ACPClient(Config(cleanup_timeout=1), lambda message: None)
    try:
        await client.start(
            [sys.executable, "-u", str(fixture)], tmp_path, os.environ.copy()
        )
        proc = client.proc
        async with asyncio.timeout(3):
            if bad:
                with pytest.raises(GatewayError, match="unexpected_response_id"):
                    await client.request("initialize", {})
            else:
                assert await client.request("initialize", {}) == {"protocolVersion": 1}
    finally:
        if client.tree:
            client.tree.kill()
        await client.close()
    assert proc.returncode is not None
    assert client.tree.handle is None
