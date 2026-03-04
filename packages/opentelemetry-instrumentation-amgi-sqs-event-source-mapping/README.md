# opentelemetry-instrumentation-amgi-sqs-event-source-mapping

OpenTelemetry instrumentation for `amgi-sqs-event-source-mapping`.

This instrumentor does not create spans. It uses the handler's
`invocation_hook` to flush traces, logs, and metrics after each Lambda
invocation.

## Usage

```python
from opentelemetry.instrumentation.amgi_sqs_event_source_mapping import (
    SqsEventSourceMappingInstrumentor,
)

SqsEventSourceMappingInstrumentor().instrument()
```
