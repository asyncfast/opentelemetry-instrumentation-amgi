from __future__ import annotations

import logging
from collections.abc import Callable
from collections.abc import Collection
from collections.abc import Sequence
from functools import partial
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from typing import Any
from typing import cast

from amgi_types import AMGIApplication
from amgi_types import AMGIReceiveCallable
from amgi_types import AMGISendCallable
from amgi_types import AMGISendEvent
from amgi_types import MessageSendEvent
from amgi_types import Scope
from asyncfast import AsyncFast
from asyncfast.middleware.errors import ServerErrorMiddleware
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.propagate import extract
from opentelemetry.propagate import inject
from opentelemetry.propagators.textmap import Getter
from opentelemetry.propagators.textmap import Setter
from opentelemetry.trace import Link
from opentelemetry.trace import Span
from opentelemetry.trace import SpanKind
from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode
from opentelemetry.util.types import AttributeValue

try:
    package_version: str | None = version("opentelemetry-instrumentation-asyncfast")
except PackageNotFoundError:
    package_version = None


logger = logging.getLogger("opentelemetry-instrumentation-asyncfast.error")


original_build_middleware_stack: Callable[[AsyncFast], AMGIApplication] | None = None


class HeaderGetter(Getter[Sequence[tuple[bytes, bytes]]]):
    def get(self, carrier: Sequence[tuple[bytes, bytes]] | None, key: str) -> list[str]:
        if not carrier:
            return []
        key_bytes = key.encode()
        values: list[str] = []
        for header_key, header_value in carrier:
            if header_key == key_bytes:
                values.append(header_value.decode(errors="ignore"))
        return values

    def keys(self, carrier: Sequence[tuple[bytes, bytes]] | None) -> list[str]:
        if not carrier:
            return []
        return [header_key.decode(errors="ignore") for header_key, _ in carrier]


class HeaderSetter(Setter[list[tuple[bytes, bytes]]]):
    def set(
        self, carrier: list[tuple[bytes, bytes]] | None, key: str, value: str
    ) -> None:
        if carrier is None:
            return
        carrier.append((key.encode(), value.encode()))


HEADER_GETTER = HeaderGetter()
HEADER_SETTER = HeaderSetter()

TRACE_HEADERS = (b"traceparent", b"tracestate")


def span_name(operation_name: str, destination: str) -> str:
    return f"{operation_name} {destination}"


def set_if(span: Span, key: str, value: AttributeValue | None) -> None:
    if value is None:
        return
    span.set_attribute(key, value)


def payload_size(payload: bytes | None) -> int | None:
    if payload is None:
        return None
    return len(payload)


def headers_envelope_size(headers: Sequence[tuple[bytes, bytes]]) -> int:
    return sum(len(k) + len(v) for k, v in headers)


class OpenTelemetryMiddleware:
    def __init__(
        self,
        app: AMGIApplication,
        tracer: trace.Tracer,
    ) -> None:
        self.app = app
        self.tracer = tracer

    async def traced_send(self, send: AMGISendCallable, message: AMGISendEvent) -> None:
        if message["type"] != "message.send":
            await send(message)
            return

        destination = message["address"]
        headers = list(message["headers"])
        payload = message.get("payload")

        operation_name = "publish"

        with self.tracer.start_as_current_span(
            span_name(operation_name, destination), kind=SpanKind.PRODUCER
        ) as publish_span:
            set_if(publish_span, "messaging.operation.type", "send")
            set_if(publish_span, "messaging.operation.name", operation_name)

            set_if(publish_span, "messaging.destination.name", destination)

            body_size = payload_size(payload)
            set_if(publish_span, "messaging.message.body.size", body_size)

            envelope_size = headers_envelope_size(headers) + (body_size or 0)
            set_if(publish_span, "messaging.message.envelope.size", envelope_size)

            inject(headers, setter=HEADER_SETTER)

            new_message: MessageSendEvent = {
                **message,
                "headers": headers,
            }
            try:
                await send(new_message)
            except Exception as e:
                publish_span.record_exception(e)
                publish_span.set_status(Status(StatusCode.ERROR))
                raise

    async def __call__(
        self, scope: Scope, receive: AMGIReceiveCallable, send: AMGISendCallable
    ) -> None:
        if scope["type"] != "message":
            await self.app(scope, receive, send)
            return

        destination_name = scope["address"]
        headers = scope["headers"]
        payload = scope.get("payload")

        message_context = extract(headers, getter=HEADER_GETTER)
        parent_span = trace.get_current_span(message_context)
        parent_span_context = parent_span.get_span_context()
        links = [Link(parent_span_context)] if parent_span_context.is_valid else []

        operation_name = "consume"

        with self.tracer.start_as_current_span(
            span_name(operation_name, destination_name),
            links=links,
            kind=SpanKind.CONSUMER,
        ) as span:
            set_if(span, "messaging.operation.type", "process")
            set_if(span, "messaging.operation.name", operation_name)

            set_if(span, "messaging.destination.name", destination_name)

            body_size = payload_size(payload)
            set_if(span, "messaging.message.body.size", body_size)

            envelope_size = headers_envelope_size(headers) + (body_size or 0)
            set_if(span, "messaging.message.envelope.size", envelope_size)

            await self.app(scope, receive, partial(self.traced_send, send))


class AsyncFastInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]:
        return ["asyncfast>=0.38.0"]

    def _instrument(self, **kwargs: Any) -> None:
        global original_build_middleware_stack
        if original_build_middleware_stack is not None:
            return

        tracer_provider = kwargs.get("tracer_provider")
        tracer = trace.get_tracer(
            "opentelemetry.instrumentation.asyncfast",
            package_version,
            tracer_provider=tracer_provider,
        )

        original_build_middleware_stack = AsyncFast.build_middleware_stack

        def build_middleware_stack(self: AsyncFast) -> AMGIApplication:
            internal_app = original_build_middleware_stack(self)
            if not isinstance(internal_app, ServerErrorMiddleware):
                logger.error(
                    "Skipping AsyncFast instrumentation due to unexpected middleware stack: expected %s, got %s",
                    ServerErrorMiddleware.__name__,
                    type(internal_app),
                )
                return internal_app

            open_telemetry_middleware = OpenTelemetryMiddleware(
                internal_app.app,
                tracer,
            )
            return ServerErrorMiddleware(open_telemetry_middleware)

        cast(Any, AsyncFast).build_middleware_stack = build_middleware_stack

    def _uninstrument(self, **kwargs: Any) -> None:
        global original_build_middleware_stack
        if original_build_middleware_stack is None:
            return

        cast(Any, AsyncFast).build_middleware_stack = original_build_middleware_stack
        original_build_middleware_stack = None
