import asyncio
import json
import socket
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest
import uvicorn
from fastapi import FastAPI

from demo.server import DemoSettings, EventHub, create_app, read_settings, stream_events

READ_TIMEOUT_SECONDS = 5.0


def make_settings(**overrides: Any) -> DemoSettings:
    values: dict[str, Any] = {
        "api_base_url": "http://upstream.test",
        "default_api_key": "demo-key",
        "public_api_url": "http://localhost:8000",
        "rabbitmq_ui_url": "http://localhost:15672",
        "hook_base_url": "http://demo:9100",
        "event_buffer_size": 200,
    }
    return DemoSettings(**{**values, **overrides})


def echo_request(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        201,
        json={
            "method": request.method,
            "url": str(request.url),
            "body": request.content.decode(),
            "headers": {name.lower(): value for name, value in request.headers.items()},
        },
    )


def refuse_connection(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("connection refused", request=request)


async def request_demo(
    method: str,
    url: str,
    *,
    handler: Callable[[httpx.Request], httpx.Response] = echo_request,
    settings: DemoSettings | None = None,
    **kwargs: Any,
) -> httpx.Response:
    app = create_app(settings or make_settings(), httpx.MockTransport(handler))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://demo.test") as client:
            return await client.request(method, url, **kwargs)


class LiveDemo:
    """The demo app served by a real uvicorn, because ASGITransport cannot stream."""

    def __init__(self, app: FastAPI, base_url: str) -> None:
        self.app = app
        self.base_url = base_url

    def open_stream(self, client: httpx.AsyncClient) -> Any:
        return client.stream("GET", f"{self.base_url}/events", timeout=READ_TIMEOUT_SECONDS)


@pytest.fixture
async def live_demo() -> AsyncIterator[LiveDemo]:
    app = create_app(make_settings(), httpx.MockTransport(echo_request))
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="on"))
    # A pre-bound socket queues connections until the server starts accepting them.
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    yield LiveDemo(app, f"http://127.0.0.1:{port}")
    server.should_exit = True
    await asyncio.wait_for(serving, timeout=READ_TIMEOUT_SECONDS)


async def read_message(lines: AsyncIterator[str]) -> str:
    """Reads one SSE message (lines up to the blank separator) and returns it joined by newlines."""
    collected: list[str] = []
    async for line in lines:
        if line == "":
            return "\n".join(collected)
        collected.append(line)
    raise AssertionError("stream ended before a full message")


async def next_message(lines: AsyncIterator[str]) -> str:
    return await asyncio.wait_for(read_message(lines), timeout=READ_TIMEOUT_SECONDS)


def event_from(message: str) -> dict[str, Any]:
    assert message.startswith("data: ")
    event: dict[str, Any] = json.loads(message.removeprefix("data: "))
    return event


async def test_health_returns_ok() -> None:
    response = await request_demo("GET", "/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_config_returns_urls_built_from_settings() -> None:
    settings = make_settings(
        default_api_key="secret-from-env",
        hook_base_url="http://hooks:9100",
        public_api_url="http://localhost:8080",
        rabbitmq_ui_url="http://localhost:15999",
    )

    response = await request_demo("GET", "/config", settings=settings)

    assert response.json() == {
        "api_key": "secret-from-env",
        "webhook_url_ok": "http://hooks:9100/hook",
        "webhook_url_fail": "http://hooks:9100/hook/fail",
        "api_url": "http://localhost:8080",
        "api_docs_url": None,
        "rabbitmq_ui_url": "http://localhost:15999",
    }


async def test_config_returns_the_docs_url_only_when_api_docs_are_enabled() -> None:
    settings = make_settings(public_api_url="http://localhost:8080", api_docs_enabled=True)

    response = await request_demo("GET", "/config", settings=settings)

    assert response.json()["api_docs_url"] == "http://localhost:8080/docs"


def test_read_settings_uses_defaults_when_environment_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "API_BASE_URL",
        "DEFAULT_API_KEY",
        "PUBLIC_API_URL",
        "RABBITMQ_UI_URL",
        "DEMO_HOOK_BASE_URL",
        "EVENT_BUFFER_SIZE",
        "API_DOCS_ENABLED",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = read_settings()

    assert settings == DemoSettings(
        api_base_url="http://api:8000",
        default_api_key="change-me",
        public_api_url="http://localhost:8000",
        rabbitmq_ui_url="http://localhost:15672",
        hook_base_url="http://demo:9100",
        event_buffer_size=200,
    )


@pytest.mark.parametrize(("raw", "expected"), [("true", True), ("false", False)])
def test_read_settings_reads_api_docs_enabled_flag(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: bool
) -> None:
    monkeypatch.setenv("API_DOCS_ENABLED", raw)

    assert read_settings().api_docs_enabled is expected


@pytest.mark.parametrize("raw", ["TRUE", "True", "1", "yes", "0", "no", " true", ""])
def test_read_settings_rejects_api_docs_enabled_other_than_true_or_false(
    monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    monkeypatch.setenv("API_DOCS_ENABLED", raw)

    with pytest.raises(ValueError, match="API_DOCS_ENABLED must be 'true' or 'false'"):
        read_settings()


def test_read_settings_reads_values_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_BASE_URL", "http://other:1")
    monkeypatch.setenv("EVENT_BUFFER_SIZE", "7")

    settings = read_settings()

    assert settings.api_base_url == "http://other:1"
    assert settings.event_buffer_size == 7


async def test_index_serves_the_static_page() -> None:
    response = await request_demo("GET", "/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")


async def test_proxy_forwards_method_path_query_and_body() -> None:
    response = await request_demo(
        "POST",
        "/api/v1/payments?limit=5&status=pending",
        content=b'{"amount": "10.00"}',
        headers={"Content-Type": "application/json"},
    )

    forwarded = response.json()
    assert forwarded["method"] == "POST"
    assert forwarded["url"] == "http://upstream.test/api/v1/payments?limit=5&status=pending"
    assert forwarded["body"] == '{"amount": "10.00"}'


async def test_proxy_forwards_only_whitelisted_headers() -> None:
    response = await request_demo(
        "POST",
        "/api/v1/payments",
        content=b"{}",
        headers={
            "X-API-Key": "key-1",
            "Idempotency-Key": "idem-1",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "Bearer leak",
            "Cookie": "session=leak",
            "X-Custom": "leak",
        },
    )

    forwarded = response.json()["headers"]
    assert forwarded["x-api-key"] == "key-1"
    assert forwarded["idempotency-key"] == "idem-1"
    assert forwarded["content-type"] == "application/json"
    assert forwarded["accept"] == "application/json"
    assert "authorization" not in forwarded
    assert "cookie" not in forwarded
    assert "x-custom" not in forwarded


async def test_proxy_returns_upstream_status_body_and_content_type() -> None:
    def upstream_not_found(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            content=b"nothing here",
            headers={"content-type": "text/plain; charset=utf-8"},
        )

    response = await request_demo("GET", "/api/v1/payments/x", handler=upstream_not_found)

    assert response.status_code == 404
    assert response.content == b"nothing here"
    assert response.headers["content-type"] == "text/plain; charset=utf-8"


async def test_proxy_omits_content_type_when_upstream_sends_none() -> None:
    def upstream_no_content(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    response = await request_demo("DELETE", "/api/v1/things", handler=upstream_no_content)

    assert response.status_code == 204
    assert "content-type" not in response.headers


async def test_proxy_returns_502_when_upstream_is_unreachable() -> None:
    response = await request_demo("GET", "/api/v1/payments", handler=refuse_connection)

    assert response.status_code == 502
    assert response.json() == {
        "error": {
            "code": "upstream_unavailable",
            "message": "The payment API is not reachable from the demo service.",
        }
    }


async def test_hook_records_event_with_payment_id_and_returns_200() -> None:
    app = create_app(make_settings(), httpx.MockTransport(echo_request))
    body = {"payment_id": "p-1", "status": "succeeded"}

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://demo.test") as client:
        response = await client.post("/hook", json=body)

    [event] = list(app.state.hub._events)
    assert response.status_code == 200
    assert response.json() == {}
    assert event.pop("received_at").endswith("+00:00")
    assert event == {
        "id": 1,
        "path": "/hook",
        "ok": True,
        "status_returned": 200,
        "payment_id": "p-1",
        "body": body,
    }


async def test_failing_hook_records_event_marked_not_ok_and_returns_500() -> None:
    app = create_app(make_settings(), httpx.MockTransport(echo_request))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://demo.test") as client:
        response = await client.post("/hook/fail", json={"payment_id": "p-2"})

    [event] = list(app.state.hub._events)
    assert response.status_code == 500
    assert response.json() == {"error": "demo receiver is failing on purpose"}
    assert event["path"] == "/hook/fail"
    assert event["ok"] is False
    assert event["status_returned"] == 500
    assert event["payment_id"] == "p-2"


@pytest.mark.parametrize(
    ("raw_body", "expected_body"),
    [
        (b"not json at all", "not json at all"),
        (b"", ""),
        (b'["a", "b"]', ["a", "b"]),
        (b'{"status": "succeeded"}', {"status": "succeeded"}),
    ],
)
async def test_hook_keeps_non_object_bodies_and_leaves_payment_id_null(
    raw_body: bytes, expected_body: Any
) -> None:
    app = create_app(make_settings(), httpx.MockTransport(echo_request))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://demo.test") as client:
        await client.post("/hook", content=raw_body)

    [event] = list(app.state.hub._events)
    assert event["body"] == expected_body
    assert event["payment_id"] is None


async def test_events_replays_buffer_then_streams_new_event(live_demo: LiveDemo) -> None:
    async with httpx.AsyncClient() as client:
        await client.post(f"{live_demo.base_url}/hook", json={"payment_id": "old"})

        async with live_demo.open_stream(client) as stream:
            lines = stream.aiter_lines()
            replayed = event_from(await next_message(lines))
            await client.post(f"{live_demo.base_url}/hook/fail", json={"payment_id": "new"})
            live = event_from(await next_message(lines))

    assert stream.headers["content-type"].startswith("text/event-stream")
    assert (replayed["id"], replayed["payment_id"], replayed["ok"]) == (1, "old", True)
    assert (live["id"], live["payment_id"], live["ok"]) == (2, "new", False)


async def test_events_delivers_each_event_to_every_subscriber(live_demo: LiveDemo) -> None:
    async with (
        httpx.AsyncClient() as client,
        live_demo.open_stream(client) as first,
        live_demo.open_stream(client) as second,
    ):
        first_lines = first.aiter_lines()
        second_lines = second.aiter_lines()
        await client.post(f"{live_demo.base_url}/hook", json={"payment_id": "shared"})
        first_event = event_from(await next_message(first_lines))
        second_event = event_from(await next_message(second_lines))

    assert first_event["payment_id"] == "shared"
    assert second_event == first_event


async def test_delete_events_clears_buffer_and_notifies_subscribers(live_demo: LiveDemo) -> None:
    async with httpx.AsyncClient() as client:
        await client.post(f"{live_demo.base_url}/hook", json={"payment_id": "p"})
        async with live_demo.open_stream(client) as stream:
            lines = stream.aiter_lines()
            await next_message(lines)
            deleted = await client.delete(f"{live_demo.base_url}/events")
            cleared = await next_message(lines)

        async with live_demo.open_stream(client) as fresh:
            await client.post(f"{live_demo.base_url}/hook", json={"payment_id": "after"})
            after_clear = event_from(await next_message(fresh.aiter_lines()))

    assert deleted.status_code == 204
    assert cleared == "event: cleared\ndata: {}"
    assert after_clear["payment_id"] == "after"
    assert after_clear["id"] == 2


async def test_stream_sends_keepalive_comment_when_idle() -> None:
    hub = EventHub(size=5)
    messages = stream_events(hub, keepalive_seconds=0.01)

    message = await asyncio.wait_for(anext(messages), timeout=READ_TIMEOUT_SECONDS)
    await messages.aclose()

    assert message == ": keepalive\n\n"


async def test_stream_removes_subscriber_when_closed() -> None:
    hub = EventHub(size=5)
    messages = stream_events(hub, keepalive_seconds=0.01)
    await asyncio.wait_for(anext(messages), timeout=READ_TIMEOUT_SECONDS)
    assert hub.subscriber_count == 1

    await messages.aclose()

    assert hub.subscriber_count == 0


def test_buffer_keeps_only_the_latest_events_up_to_its_size() -> None:
    hub = EventHub(size=3)

    for number in range(5):
        hub.record("/hook", ok=True, body={"payment_id": str(number)})
    _, replay = hub.subscribe()

    assert [event["payment_id"] for event in replay] == ["2", "3", "4"]
    assert [event["id"] for event in replay] == [3, 4, 5]


async def test_slow_subscriber_queue_drops_oldest_messages_when_full() -> None:
    hub = EventHub(size=2)
    queue, _ = hub.subscribe()

    for number in range(4):
        hub.record("/hook", ok=True, body={"payment_id": str(number)})
    received = [json.loads(queue.get_nowait().removeprefix("data: ")) for _ in range(2)]

    assert [event["payment_id"] for event in received] == ["2", "3"]
    assert queue.empty()
