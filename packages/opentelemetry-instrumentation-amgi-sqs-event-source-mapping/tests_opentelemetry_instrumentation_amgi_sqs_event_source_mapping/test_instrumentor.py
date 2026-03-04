import logging
from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

import opentelemetry._logs as _logs
import opentelemetry.metrics as metrics
import opentelemetry.trace as trace
import pytest
from amgi_sqs_event_source_mapping import SqsEventSourceMappingHandler
from opentelemetry.instrumentation.amgi_sqs_event_source_mapping import (
    SqsEventSourceMappingInstrumentor,
)


@pytest.fixture
def mock_tracer_provider() -> Generator[Mock, None, None]:
    with patch.object(trace, "get_tracer_provider") as mock_tracer_provider:
        yield mock_tracer_provider.return_value


@pytest.fixture
def mock_meter_provider() -> Generator[Mock, None, None]:
    with patch.object(metrics, "get_meter_provider") as mock_meter_provider:
        yield mock_meter_provider.return_value


@pytest.fixture
def mock_logger_provider() -> Generator[Mock, None, None]:
    with patch.object(_logs, "get_logger_provider") as mock_logger_provider:
        yield mock_logger_provider.return_value


@pytest.fixture(autouse=True)
def sqs_event_source_mapping_instrumentor_instrumentor(
    mock_tracer_provider: Mock, mock_meter_provider: Mock, mock_logger_provider: Mock
) -> Generator[SqsEventSourceMappingInstrumentor, None, None]:
    instrumentor = SqsEventSourceMappingInstrumentor()
    instrumentor.instrument()

    yield instrumentor

    instrumentor.uninstrument()


@pytest.fixture
def event() -> Any:
    return {
        "Records": [
            {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": "Test message.",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082649183",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082649185",
                },
                "messageAttributes": {
                    "myAttribute": {
                        "stringValue": "myValue",
                        "stringListValues": [],
                        "binaryListValues": [],
                        "dataType": "String",
                    }
                },
                "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                "awsRegion": "us-east-2",
            }
        ]
    }


def test_force_flush_called(
    event: Any,
    mock_tracer_provider: Mock,
    mock_meter_provider: Mock,
    mock_logger_provider: Mock,
) -> None:
    mock_app = AsyncMock()
    handler = SqsEventSourceMappingHandler(mock_app, lifespan=False)

    handler(event, Mock())

    mock_tracer_provider.force_flush.assert_called_once()
    mock_meter_provider.force_flush.assert_called_once()
    mock_logger_provider.force_flush.assert_called_once()


def test_invocation_hook_is_wrapped(
    event: Any,
    mock_tracer_provider: Mock,
    mock_meter_provider: Mock,
    mock_logger_provider: Mock,
) -> None:
    mock_app = AsyncMock()
    mock_context = Mock()
    mock_invocation_hook = Mock(return_value=AsyncMock())
    handler = SqsEventSourceMappingHandler(
        mock_app, lifespan=False, invocation_hook=mock_invocation_hook
    )

    handler(event, mock_context)

    mock_invocation_hook.assert_called_once()
    mock_invocation_hook.return_value.__aenter__.assert_awaited_once()
    mock_invocation_hook.return_value.__aexit__.assert_awaited_once()
    mock_tracer_provider.force_flush.assert_called_once()
    mock_meter_provider.force_flush.assert_called_once()
    mock_logger_provider.force_flush.assert_called_once()


def test_force_flush_exceptions_are_swallowed(
    event: Any,
    mock_tracer_provider: Mock,
    mock_meter_provider: Mock,
    mock_logger_provider: Mock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    mock_app = AsyncMock()

    mock_tracer_provider.force_flush.side_effect = Exception()
    mock_meter_provider.force_flush.side_effect = Exception()
    mock_logger_provider.force_flush.side_effect = Exception()

    handler = SqsEventSourceMappingHandler(mock_app, lifespan=False)

    with caplog.at_level(
        logging.ERROR,
        logger="opentelemetry-instrumentation-amgi-sqs-event-source-mapping.error",
    ):
        handler(event, Mock())

        assert all(
            record.msg == "Exception in force_flush" for record in caplog.records
        )

    mock_tracer_provider.force_flush.assert_called_once()
    mock_meter_provider.force_flush.assert_called_once()
    mock_logger_provider.force_flush.assert_called_once()
