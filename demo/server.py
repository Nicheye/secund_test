"""Manual-testing companion: serves the demo UI, proxies the API and streams received webhooks."""

import asyncio
import json
import os
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

STATIC_DIRECTORY = Path(__file__).resolve().parent / "static"
FORWARDED_REQUEST_HEADERS = ("X-API-Key", "Idempotency-Key", "Content-Type", "Accept")
UPSTREAM_TIMEOUT_SECONDS = 30.0
HOOK_PATH = "/hook"
FAILING_HOOK_PATH = "/hook/fail"
CLEARED_MESSAGE = "event: cleared\ndata: {}\n\n"
KEEPALIVE_MESSAGE = ": keepalive\n\n"


@dataclass(frozen=True)
class DemoSettings:
    api_base_url: str
    default_api_key: str
    public_api_url: str
    rabbitmq_ui_url: str
    hook_base_url: str
    event_buffer_size: int
    api_docs_enabled: bool = False
    keepalive_seconds: float = 15.0


def read_api_docs_enabled() -> bool:
    # Same contract as the API (app/config.py): only the exact values true and false are valid.
    raw = os.environ.get("API_DOCS_ENABLED", "false")
    if raw not in ("true", "false"):
        raise ValueError(f"API_DOCS_ENABLED must be 'true' or 'false', got {raw!r}")
    return raw == "true"


def read_settings() -> DemoSettings:
    return DemoSettings(
        api_base_url=os.environ.get("API_BASE_URL", "http://api:8000"),
        default_api_key=os.environ.get("DEFAULT_API_KEY", "change-me"),
        public_api_url=os.environ.get("PUBLIC_API_URL", "http://localhost:8000"),
        rabbitmq_ui_url=os.environ.get("RABBITMQ_UI_URL", "http://localhost:15672"),
        hook_base_url=os.environ.get("DEMO_HOOK_BASE_URL", "http://demo:9100"),
        event_buffer_size=int(os.environ.get("EVENT_BUFFER_SIZE", "200")),
        api_docs_enabled=read_api_docs_enabled(),
    )


class EventHub:
    """Bounded replay buffer plus per-subscriber queues that drop their oldest message."""

    def __init__(self, size: int) -> None:
        self._size = size
        self._events: deque[dict[str, Any]] = deque(maxlen=size)
        self._subscribers: set[asyncio.Queue[str]] = set()
        self._last_id = 0

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def record(self, path: str, ok: bool, body: Any) -> dict[str, Any]:
        self._last_id += 1
        event = {
            "id": self._last_id,
            "received_at": datetime.now(UTC).isoformat(),
            "path": path,
            "ok": ok,
            "status_returned": HTTPStatus.OK if ok else HTTPStatus.INTERNAL_SERVER_ERROR,
            "payment_id": body.get("payment_id") if isinstance(body, dict) else None,
            "body": body,
        }
        self._events.append(event)
        self._broadcast(f"data: {json.dumps(event)}\n\n")
        return event

    def clear(self) -> None:
        self._events.clear()
        self._broadcast(CLEARED_MESSAGE)

    def subscribe(self) -> tuple[asyncio.Queue[str], list[dict[str, Any]]]:
        # Snapshot and registration happen without an await, so no event is missed or doubled.
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=self._size)
        self._subscribers.add(queue)
        return queue, list(self._events)

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        self._subscribers.discard(queue)

    def _broadcast(self, message: str) -> None:
        for queue in self._subscribers:
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(message)


async def stream_events(hub: EventHub, keepalive_seconds: float) -> AsyncGenerator[str]:
    queue, replay = hub.subscribe()
    try:
        for event in replay:
            yield f"data: {json.dumps(event)}\n\n"
        while True:
            try:
                yield await asyncio.wait_for(queue.get(), timeout=keepalive_seconds)
            except TimeoutError:
                yield KEEPALIVE_MESSAGE
    finally:
        hub.unsubscribe(queue)


def parse_body(raw: bytes) -> Any:
    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except ValueError:
        return text


def create_app(
    settings: DemoSettings | None = None,
    upstream_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    demo_settings = settings or read_settings()
    hub = EventHub(demo_settings.event_buffer_size)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        async with httpx.AsyncClient(
            base_url=demo_settings.api_base_url,
            timeout=UPSTREAM_TIMEOUT_SECONDS,
            transport=upstream_transport,
        ) as client:
            app.state.upstream = client
            yield

    app = FastAPI(title="Payment service demo", lifespan=lifespan)
    app.state.hub = hub

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIRECTORY / "index.html")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/config")
    async def config() -> dict[str, str | None]:
        return {
            "api_key": demo_settings.default_api_key,
            "webhook_url_ok": f"{demo_settings.hook_base_url}{HOOK_PATH}",
            "webhook_url_fail": f"{demo_settings.hook_base_url}{FAILING_HOOK_PATH}",
            "api_url": demo_settings.public_api_url,
            "api_docs_url": (
                f"{demo_settings.public_api_url}/docs" if demo_settings.api_docs_enabled else None
            ),
            "rabbitmq_ui_url": demo_settings.rabbitmq_ui_url,
        }

    @app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(path: str, request: Request) -> Response:
        client: httpx.AsyncClient = app.state.upstream
        headers = {
            name: request.headers[name]
            for name in FORWARDED_REQUEST_HEADERS
            if name in request.headers
        }
        upstream_request = client.build_request(
            request.method,
            f"/api/{path}",
            params=request.query_params,
            content=await request.body(),
            headers=headers,
        )
        try:
            upstream = await client.send(upstream_request)
        except httpx.TransportError:
            return JSONResponse(
                status_code=HTTPStatus.BAD_GATEWAY,
                content={
                    "error": {
                        "code": "upstream_unavailable",
                        "message": "The payment API is not reachable from the demo service.",
                    }
                },
            )
        response_headers = {}
        if "content-type" in upstream.headers:
            response_headers["content-type"] = upstream.headers["content-type"]
        return Response(
            content=upstream.content, status_code=upstream.status_code, headers=response_headers
        )

    @app.post(HOOK_PATH)
    async def receive_webhook(request: Request) -> dict[str, str]:
        hub.record(HOOK_PATH, ok=True, body=parse_body(await request.body()))
        return {}

    @app.post(FAILING_HOOK_PATH)
    async def receive_webhook_and_fail(request: Request) -> JSONResponse:
        hub.record(FAILING_HOOK_PATH, ok=False, body=parse_body(await request.body()))
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={"error": "demo receiver is failing on purpose"},
        )

    @app.get("/events")
    async def stream() -> StreamingResponse:
        return StreamingResponse(
            stream_events(hub, demo_settings.keepalive_seconds),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.delete("/events", status_code=HTTPStatus.NO_CONTENT)
    async def clear_events() -> Response:
        hub.clear()
        return Response(status_code=HTTPStatus.NO_CONTENT)

    return app


app = create_app()
