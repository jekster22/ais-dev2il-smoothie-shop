import asyncio
import random
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging
from prometheus_fastapi_instrumentator import Instrumentator
# Custom metric: Count smoothies ordered by flavor
from prometheus_client import Counter
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource


resource = Resource.create({"service.name": "kitchen-service"})
trace.set_tracer_provider(TracerProvider(resource=resource))
# This is going to export the tracing data to Jaeger
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))

smoothies_ordered = Counter(
    'smoothies_ordered_total',
    'Total number of smoothies ordered',
    ['flavor']
)

logger = logging.getLogger(__name__)
# Create the FastAPI application
app = FastAPI(title="Kitchen Service")

# This is going to hook into FastAPI and automatically create traces for all HTTP requests
# We exclude "receive" and "send" spans because they are not relevant for us and just add noise to the traces
FastAPIInstrumentor.instrument_app(app, exclude_spans=["receive", "send"])
# This is going to hook into HTTPX to automatically create traces for all outgoing HTTP requests and to
# connect traces between services with each other
HTTPXClientInstrumentor().instrument()

# Initialize Prometheus metrics instrumentation
Instrumentator().instrument(app).expose(app)

# Configuration: How many cooks are available in the kitchen
NUM_COOKS = 1

# Semaphore: Controls how many smoothies can be prepared at the same time
# (one per cook). If all cooks are busy, new orders must wait.
cook_semaphore = asyncio.Semaphore(NUM_COOKS)

# Data model: Defines what a smoothie order looks like
class SmoothieOrder(BaseModel):
    flavor: str

# message on initalization
logger.info(f"Iniatilized {__name__} with {NUM_COOKS} cook(s) available.")

# Endpoint: Receives requests to prepare a smoothie
@app.post("/prepare")
async def prepare_smoothie(order: SmoothieOrder):
    smoothies_ordered.labels(flavor=order.flavor).inc()

    logger.info(f"Preparing smoothie", extra={"tags":{"flavor": order.flavor, "num_cooks":  str(NUM_COOKS)}})
    try:
        # Try to get a cook (wait max 2 seconds)
        logger.debug(f"Waiting for a cook to be available")
        await asyncio.wait_for(cook_semaphore.acquire(), timeout=2.0)
    except asyncio.TimeoutError:
        # All cooks are busy and timeout reached -> reject the order
        logger.error(f"No cook available. {NUM_COOKS} cooks are currently busy. Consider increasing the number of cooks.")
        raise HTTPException(status_code=503, detail="All cooks are currently busy")
    try:
        # Simulate preparing the smoothie (takes 1.5 to 2.5 seconds)
        await asyncio.sleep(random.uniform(1.5, 2.5))
        logger.info(f"Finished preparing {order.flavor} smoothie")
        return {"status": "done", "flavor": order.flavor}
    finally:
        # Release the cook so they can prepare the next smoothie
        cook_semaphore.release()