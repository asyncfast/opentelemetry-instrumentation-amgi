from __future__ import annotations

import logging
from collections.abc import Callable
from collections.abc import Collection
from contextlib import asynccontextmanager
from types import ModuleType
from typing import Any
from typing import cast

import opentelemetry.metrics as metrics
import opentelemetry.trace as trace
from amgi_sqs_event_source_mapping import SqsEventSourceMappingHandler
from amgi_types import AMGIApplication
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor

logs: ModuleType | None
try:
    import opentelemetry._logs as _logs

    logs = _logs
except ImportError:  # pragma: no cover
    logs = None

original_init: Callable[..., None] | None = None

logger = logging.getLogger(
    "opentelemetry-instrumentation-amgi-sqs-event-source-mapping.error"
)


def force_flush_provider(provider: Any) -> None:
    force_flush = getattr(provider, "force_flush", lambda: None)
    try:
        force_flush()
    except Exception:
        logger.exception("Exception in force_flush", exc_info=True)


def flush_telemetry() -> None:
    force_flush_provider(trace.get_tracer_provider())
    force_flush_provider(metrics.get_meter_provider())
    if logs is not None:
        force_flush_provider(logs.get_logger_provider())


@asynccontextmanager
async def _hook(event: Any, context: Any) -> Any:
    try:
        yield
    finally:
        flush_telemetry()


def compose_invocation_hook(
    base_hook: Callable[..., Any] | None,
) -> Callable[..., Any]:
    if base_hook is None:
        return _hook

    @asynccontextmanager
    async def _wrapped_hook(event: Any, context: Any) -> Any:
        try:
            async with base_hook(event, context):
                yield
        finally:
            flush_telemetry()

    return _wrapped_hook


class SqsEventSourceMappingInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]:
        return ["amgi-sqs-event-source-mapping>=0.37.0"]

    def _instrument(self, **kwargs: Any) -> None:
        global original_init
        if original_init is not None:
            return

        original_init = SqsEventSourceMappingHandler.__init__

        def _init_wrapper(
            self: SqsEventSourceMappingHandler, app: AMGIApplication, **init_kwargs: Any
        ) -> None:
            composed_hook = compose_invocation_hook(init_kwargs.get("invocation_hook"))
            original_init(
                self, app, **{**init_kwargs, "invocation_hook": composed_hook}
            )

        cast(Any, SqsEventSourceMappingHandler).__init__ = _init_wrapper

    def _uninstrument(self, **kwargs: Any) -> None:
        global original_init
        if original_init is None:
            return

        cast(Any, SqsEventSourceMappingHandler).__init__ = original_init
        original_init = None
