from collections.abc import AsyncGenerator
from dataclasses import dataclass

from amgi_aiokafka import run
from asyncfast import AsyncFast
from asyncfast import Message
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncfast import AsyncFastInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from pydantic import BaseModel

resource = Resource.create({"service.name": "amgi-kafka-example"})
tracer_provider = TracerProvider(resource=resource)
trace.set_tracer_provider(tracer_provider)

otlp_exporter = OTLPSpanExporter(
    endpoint="localhost:4317",
    insecure=True,
)

tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

AsyncFastInstrumentor().instrument()


class CounterPayload(BaseModel):
    count: int
    error: str | None = None


app = AsyncFast()


@dataclass
class TopicMessage(Message, address="topic.{number}"):
    number: int
    payload: CounterPayload


@app.channel("topic.{number}")
async def topic_handler(
    payload: CounterPayload, number: str
) -> AsyncGenerator[TopicMessage, None]:
    next_number = int(number) + 1
    if payload.count > 0:
        yield TopicMessage(
            number=next_number,
            payload=CounterPayload(count=payload.count - 1, error=payload.error),
        )
    elif payload.error:
        raise Exception(payload.error)


if __name__ == "__main__":
    topics = [f"topic.{i}" for i in range(100)]

    run(
        app,
        *topics,
        bootstrap_servers=["localhost:9092"],
        group_id="amgi-kafka-example",
    )
