# AsyncFast OpenTelemetry

Minimal example showing AsyncFast OpenTelemetry tracing for an AMGI Kafka app using `amgi-aiokafka`, with traces exported via OTLP to an OpenTelemetry Collector and visualized in Jaeger.

## What it does

- Subscribes to `topic.0` … `topic.99`
- Each message decrements a `count` and forwards to the next topic
- If `count` reaches 0 it stops sending. If `error` is set, the handler raises an exception
- Traces are exported via OTLP to the local Collector and appear in Jaeger

## Run

1. Start infra (Kafka, Collector, Jaeger, Redpanda Console):

```bash
docker compose up -d
```

2. Run the app:

```bash
python main.py
```

3. Open Redpanda Console (http://localhost:8080), and send the following message to `topic.0`:

```json
{
 "count": 10,
 "error": "woops"
}
```

4. Open Jaeger UI (http://localhost:16686), you should now have traces!
