# opentelemetry-instrumentation-asyncfast

opentelemetry-instrumentation-asyncfast provides OpenTelemetry tracing for
[AsyncFast](https://pypi.org/project/asyncfast/).

## Installation

```
pip install opentelemetry-instrumentation-asyncfast
```

## Example

```python
from asyncfast import AsyncFast
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.instrumentation.asyncfast import AsyncFastInstrumentor

tracer_provider = TracerProvider()
tracer_provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))

trace.set_tracer_provider(tracer_provider)

AsyncFastInstrumentor().instrument()

app = AsyncFast()


@app.channel("orders")
async def orders(payload: dict) -> None: ...
```

## Contact

For questions or suggestions, please contact [jack.burridge@mail.com](mailto:jack.burridge@mail.com).

## License

Copyright 2026 AMGI
