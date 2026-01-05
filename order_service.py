import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import Resource

# Configure OpenTelemetry tracing
resource = Resource.create({"service.name": "order-service"})
trace.set_tracer_provider(TracerProvider(resource=resource))
# This is going to export the tracing data to Jaeger
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))

# Instrument logging to automatically inject trace context into all log records
def log_hook(span, record):
    if not hasattr(record, "tags"):
        record.tags = {}
    record.tags["service_name"] = resource.attributes["service.name"]
    record.tags["trace_id"] = format(span.get_span_context().trace_id, "032x")
LoggingInstrumentor().instrument(log_hook=log_hook)

app = FastAPI(title="Order Service")

# This is going to hook into FastAPI and automatically create traces for all HTTP requests
FastAPIInstrumentor.instrument_app(app, exclude_spans=["receive", "send"])
# This is going to hook into HTTPX to automatically create traces for all outgoing HTTP requests and to
# connect traces between services with each other
HTTPXClientInstrumentor().instrument()

class Order(BaseModel):
    flavor: str

@app.post("/order")
async def create_order(order: Order):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                "http://localhost:8001/prepare",
                json={"flavor": order.flavor}
            )
            response.raise_for_status()
            return {"status": "completed", "kitchen_response": response.json()}
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail="Kitchen failed to process order")
        except httpx.RequestError:
            raise HTTPException(status_code=503, detail="Kitchen unavailable")
