import pytest

from litellm_kiro_rpc.contracts import Config, GatewayError, Request, strict_json


def test_text_blocks_normalize_without_trimming():
    request = Request.parse(
        {
            "model": "kiro-default",
            "messages": [
                {"role": "system", "content": "system"},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "  Xin "},
                        {"type": "text", "text": "chào\n"},
                    ],
                },
            ],
        },
        "a",
        Config(),
    )
    assert request.messages[-1][1] == "  Xin chào\n"
    assert request.conversation != "a"


@pytest.mark.parametrize(
    "extra",
    [
        {"temperature": 0},
        {"tools": []},
        {"api_base": "http://evil"},
        {"headers": {}},
        {"max_tokens": 1},
    ],
)
def test_unsupported_parameters_rejected(extra):
    with pytest.raises(GatewayError):
        Request.parse(
            {
                "model": "kiro-default",
                "messages": [{"role": "user", "content": "x"}],
                **extra,
            },
            None,
            Config(),
        )


@pytest.mark.parametrize("raw", ['{"x":1,"x":2}', '{"x":NaN}'])
def test_ambiguous_json_rejected(raw):
    with pytest.raises(ValueError):
        strict_json(raw)
