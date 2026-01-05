import asyncio
import random
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import Resource


logger = logging.getLogger(__name__)

# Configure OpenTelemetry tracing
resource = Resource.create({"service.name": "kitchen-service"})
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

app = FastAPI(title="Kitchen Service")

# Initialize Prometheus metrics instrumentation
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)

# This is going to hook into FastAPI and automatically create traces for all HTTP requests
FastAPIInstrumentor.instrument_app(app, exclude_spans=["receive", "send"])
# This is going to hook into HTTPX to automatically create traces for all outgoing HTTP requests and to
# connect traces between services with each other
HTTPXClientInstrumentor().instrument()

# Custom metric: Count smoothies ordered by flavor
from prometheus_client import Counter
smoothies_ordered = Counter(
    'smoothies_ordered_total',
    'Total number of smoothies ordered',
    ['flavor']
)
NUM_COOKS = 1
cook_semaphore = asyncio.Semaphore(NUM_COOKS)

class SmoothieOrder(BaseModel):
    flavor: str

@app.post("/prepare")
async def prepare_smoothie(order: SmoothieOrder):
    logger.info(f"Received order to prepare a smoothie with flavor {order.flavor}")

    # Increment the counter for this flavor
    smoothies_ordered.labels(flavor=order.flavor).inc()

    tracer = trace.get_tracer(__name__)

    # Custom span: Waiting for cook to become available
    with tracer.start_as_current_span("wait_for_cook") as wait_span:
        wait_span.set_attribute("flavor", order.flavor)
        wait_span.set_attribute("num_cooks", NUM_COOKS)
        try:
            logger.debug(f"Waiting for a cook to become available")
            await asyncio.wait_for(cook_semaphore.acquire(), timeout=2.0)
        except asyncio.TimeoutError:
            logger.error(f"Can't process the order: {NUM_COOKS} cooks are currently busy. Consider increasing NUM_COOKS.")
            raise HTTPException(status_code=503, detail="All cooks are currently busy")

    try:
        # Custom span: Preparing the smoothie
        with tracer.start_as_current_span("prepare_smoothie") as prep_span:
            prep_span.set_attribute("flavor", order.flavor)
            preparation_time = random.uniform(1.5, 2.5)
            await asyncio.sleep(preparation_time)
            logger.debug(f"Smoothie with flavor {order.flavor} prepared")

        return {"status": "done", "flavor": order.flavor}
    finally:
        cook_semaphore.release()