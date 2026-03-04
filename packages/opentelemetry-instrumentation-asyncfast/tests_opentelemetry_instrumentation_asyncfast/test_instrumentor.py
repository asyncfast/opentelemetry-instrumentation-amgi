from collections.abc import AsyncGenerator
from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import Mock

import pytest
from amgi_types import AMGISendEvent
from amgi_types import MessageScope
from asyncfast import AsyncFast
from opentelemetry import trace
from opentelemetry.instrumentation.asyncfast import AsyncFastInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
)
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from opentelemetry.trace import StatusCode
from pydantic import BaseModel


@pytest.fixture
def in_memory_span_exporter() -> InMemorySpanExporter:
    return InMemorySpanExporter()


@pytest.fixture
def tracer_provider(in_memory_span_exporter: InMemorySpanExporter) -> TracerProvider:
    trace_provider = TracerProvider()
    trace_provider.add_span_processor(SimpleSpanProcessor(in_memory_span_exporter))
    return trace_provider


@pytest.fixture(autouse=True)
def asyncfast_instrumentor(
    tracer_provider: TracerProvider,
) -> Generator[AsyncFastInstrumentor, None, None]:
    instrumentor = AsyncFastInstrumentor()
    instrumentor.instrument(tracer_provider=tracer_provider)

    yield instrumentor

    instrumentor.uninstrument()


async def test_asyncfast_instrumentation_consume(
    in_memory_span_exporter: InMemorySpanExporter,
) -> None:
    app = AsyncFast()

    class MessagePayload(BaseModel):
        id: int

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(payload: MessagePayload) -> None:
        test_mock(payload)

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "2.0", "spec_version": "2.0"},
        "address": "topic",
        "headers": [(b"Id", b"10")],
        "payload": b'{"id":1}',
    }

    await app(message_scope, AsyncMock(), AsyncMock())

    test_mock.assert_called_once()

    spans = in_memory_span_exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]

    assert span.name == "consume topic"
    assert span.kind == SpanKind.CONSUMER
    assert span.attributes == {
        "messaging.destination.name": "topic",
        "messaging.message.body.size": 8,
        "messaging.message.envelope.size": 12,
        "messaging.operation.name": "consume",
        "messaging.operation.type": "process",
    }


async def test_asyncfast_instrumentation_consume_parent(
    in_memory_span_exporter: InMemorySpanExporter, tracer_provider: TracerProvider
) -> None:
    app = AsyncFast()

    class MessagePayload(BaseModel):
        id: int

    test_mock = Mock()

    @app.channel("topic")
    async def topic_handler(payload: MessagePayload) -> None:
        test_mock(payload)

    tracer = trace.get_tracer("test", tracer_provider=tracer_provider)

    with tracer.start_span("send topic") as span:
        span_context = span.get_span_context()
        traceparent = f"00-{span_context.trace_id:032x}-{span_context.span_id:016x}-{span_context.trace_flags:02x}"

        message_scope: MessageScope = {
            "type": "message",
            "amgi": {"version": "2.0", "spec_version": "2.0"},
            "address": "topic",
            "headers": [(b"traceparent", traceparent.encode())],
            "payload": b'{"id":1}',
        }

        await app(message_scope, AsyncMock(), AsyncMock())

    test_mock.assert_called_once()

    spans = in_memory_span_exporter.get_finished_spans()
    assert len(spans) == 2
    exported_span = next(span for span in spans if span.name == "consume topic")

    assert exported_span.name == "consume topic"
    assert len(exported_span.links) == 1
    link = exported_span.links[0]
    assert link.context.trace_id == span_context.trace_id
    assert link.context.span_id == span_context.span_id
    assert exported_span.kind == SpanKind.CONSUMER
    assert exported_span.attributes == {
        "messaging.destination.name": "topic",
        "messaging.message.body.size": 8,
        "messaging.message.envelope.size": 74,
        "messaging.operation.name": "consume",
        "messaging.operation.type": "process",
    }


async def test_asyncfast_instrumentation_publish(
    in_memory_span_exporter: InMemorySpanExporter,
) -> None:
    app = AsyncFast()
    mock_send = AsyncMock()

    @app.channel("topic")
    async def topic_handler() -> AsyncGenerator[dict[str, Any], None]:
        yield {
            "address": "send_topic",
            "payload": b'{"key": "KEY-001"}',
            "headers": [(b"Id", b"10")],
        }

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "2.0", "spec_version": "2.0"},
        "address": "topic",
        "headers": [],
        "payload": b"",
    }

    await app(message_scope, AsyncMock(), mock_send)

    spans = in_memory_span_exporter.get_finished_spans()
    assert len(spans) == 2
    send_span, consume_span = spans

    assert send_span.name == "publish send_topic"
    assert send_span.kind == SpanKind.PRODUCER
    assert send_span.attributes == {
        "messaging.destination.name": "send_topic",
        "messaging.message.body.size": 18,
        "messaging.message.envelope.size": 22,
        "messaging.operation.name": "publish",
        "messaging.operation.type": "send",
    }

    span_context = send_span.get_span_context()

    traceparent = f"00-{span_context.trace_id:032x}-{span_context.span_id:016x}-{span_context.trace_flags:02x}"

    mock_send.assert_has_awaits(
        [
            call(
                {
                    "type": "message.send",
                    "address": "send_topic",
                    "headers": [
                        (b"Id", b"10"),
                        (b"traceparent", traceparent.encode()),
                    ],
                    "payload": b'{"key": "KEY-001"}',
                }
            ),
            call({"type": "message.ack"}),
        ]
    )


async def test_asyncfast_instrumentation_publish_exception(
    in_memory_span_exporter: InMemorySpanExporter,
) -> None:
    app = AsyncFast()

    def _send(event: AMGISendEvent) -> None:
        if event["type"] == "message.send":
            raise OSError("Error sending")

    mock_send = AsyncMock(side_effect=_send)

    @app.channel("topic")
    async def topic_handler() -> AsyncGenerator[dict[str, Any], None]:
        yield {
            "address": "send_topic",
            "payload": b'{"key": "KEY-001"}',
            "headers": [(b"Id", b"10")],
        }

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "2.0", "spec_version": "2.0"},
        "address": "topic",
        "headers": [],
        "payload": b"",
    }

    await app(message_scope, AsyncMock(), mock_send)

    spans = in_memory_span_exporter.get_finished_spans()
    assert len(spans) == 2
    span = next(span for span in spans if span.name == "publish send_topic")

    assert span.name == "publish send_topic"
    assert span.kind == SpanKind.PRODUCER
    assert span.status.status_code == StatusCode.ERROR
    assert span.attributes == {
        "messaging.destination.name": "send_topic",
        "messaging.message.body.size": 18,
        "messaging.message.envelope.size": 22,
        "messaging.operation.name": "publish",
        "messaging.operation.type": "send",
    }

    span_context = span.get_span_context()

    traceparent = f"00-{span_context.trace_id:032x}-{span_context.span_id:016x}-{span_context.trace_flags:02x}"

    mock_send.assert_has_awaits(
        [
            call(
                {
                    "type": "message.send",
                    "address": "send_topic",
                    "headers": [
                        (b"Id", b"10"),
                        (b"traceparent", traceparent.encode()),
                    ],
                    "payload": b'{"key": "KEY-001"}',
                }
            ),
            call({"type": "message.nack", "message": "Error sending"}),
        ]
    )


async def test_asyncfast_instrumentation_publish_no_payload(
    in_memory_span_exporter: InMemorySpanExporter,
) -> None:
    app = AsyncFast()
    mock_send = AsyncMock()

    @app.channel("topic")
    async def topic_handler() -> AsyncGenerator[dict[str, Any], None]:
        yield {
            "address": "send_topic",
            "headers": [(b"Id", b"10")],
        }

    message_scope: MessageScope = {
        "type": "message",
        "amgi": {"version": "2.0", "spec_version": "2.0"},
        "address": "topic",
        "headers": [],
    }

    await app(message_scope, AsyncMock(), mock_send)

    spans = in_memory_span_exporter.get_finished_spans()
    assert len(spans) == 2
    span = next(span for span in spans if span.name == "publish send_topic")

    assert span.name == "publish send_topic"
    assert span.kind == SpanKind.PRODUCER
    assert span.attributes == {
        "messaging.destination.name": "send_topic",
        "messaging.message.envelope.size": 4,
        "messaging.operation.name": "publish",
        "messaging.operation.type": "send",
    }

    span_context = span.get_span_context()

    traceparent = f"00-{span_context.trace_id:032x}-{span_context.span_id:016x}-{span_context.trace_flags:02x}"

    mock_send.assert_has_awaits(
        [
            call(
                {
                    "type": "message.send",
                    "address": "send_topic",
                    "headers": [
                        (b"Id", b"10"),
                        (b"traceparent", traceparent.encode()),
                    ],
                    "payload": None,
                }
            ),
            call({"type": "message.ack"}),
        ]
    )
