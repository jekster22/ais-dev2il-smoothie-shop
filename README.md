# DEV2IL: Observability

## The Smoothie Shop

The smoothie shop application, allows users to order delicious smoothies. It consists of two microservices:
- The Order Service: Accepts smoothie orders
- The Kitchen Service: Prepares the smoothies

To open your personal smoothie shop
- Open a terminal and run `uv run uvicorn order_service:app --port 8000 --reload`. 
- Open another terminal and run: `uv run uvicorn kitchen_service:app --port 8001 --reload`.

## Operating the Smoothie Shop in Blind Mode

Let's start to buy some smoothies. Open a terminal and run `uv run buy_smoothies.py`. 
Look at the console output. You should see that your smoothie shop is working fine.

Let's start to send some more customers to your smoothie shop. Open another terminal and run 
`uv run buy_smoothies.py`. Look at the console output again.

It looks like your shop is having some troubles from time to time. Try to figure out what is going wrong by
looking at the outputs of all the started services. **You are not allowed to look at the code!** 
Could you figure it out and fix it ?

Most likely, you've been unable to tell why the application failed from time to time. The only way to 
find out is to ask the developers. If you look into the code of `kitchen_service.py`, you will notice
that the kitchen rejects a request to prepare a smoothie with a status code of 503 if all cooks are
so busy that the work on the requested smoothie can't be started in time. In this case, the fix would 
have been easy, as the kitchen already contains a configuration parameter to increase the number of cooks
(`NUM_COOKS`).

## Providing More Insights Through Log Outputs

We are now providing more insights into the smoothie shop by adding logging to the application. Remember 
these hints on which logging level to choose from the [Python logging HOWTO](https://docs.python.org/3/howto/logging.html#when-to-use-logging): 

| Level     | When it’s used                                                                                       |
|-----------|------------------------------------------------------------------------------------------------------|
| DEBUG     | Detailed information, typically of interest only when diagnosing problems.                           |
| INFO      | Confirmation that things are working as expected.                                                    |
| WARNING   | An indication that something unexpected happened, or indicative of some problem in the near future. (e.g. ‘disk space low’). The software is still working as expected. |
| ERROR     | Due to a more serious problem, the software has not been able to perform some function.              |
| CRITICAL  | A serious error, indicating that the program itself may be unable to continue running.               |


Modify `kitchen_service.py`

- After the existing imports create a logger for the module
```python
import logging
logger = logging.getLogger(__name__)
``` 
- Add these log messages to the `prepare_smoothie` function. Find the right places to add them on your own. 
```python
logger.info(f"Received order to prepare a smoothie with flavor {order.flavor}")
logger.debug(f"Waiting for a cook to become available")
logger.error(f"Can't process the order: {NUM_COOKS} cooks are currently busy. Consider increasing NUM_COOKS.")
logger.info(f"Smoothie with flavor {order.flavor} prepared")
```

We want all our logging messages to contain the logging level, a timestamp when the message was logged and 
the message itself. In addition, we want to be able to define the logging level for each logger individually.
- Download the file [logging_config.yaml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/logging/logging_config.yaml)
and store it in the root directory of the project. 
- Stop the kitchen service and start it again using 
`uv run uvicorn kitchen_service:app --port 8001 --reload --log-config logging_config.yaml`.  

You can now adjust the log levels, by setting the level of detail that you want to see in `logging_config.yaml`.
Make sure that you understand `logging_config.yaml` and how it works before you continue.

# TODO - add a picture of the logging configuration here

## Collecting Logs in a Central Place

In order to be able to analyze logs, you need to collect the logs of all your services in a central place and
make them searchable. We use [Loki](https://grafana.com/oss/loki/) to store logs and [Grafana](https://grafana.com/)
to query and visualize them.

# TODO - add a picture of the logging configuration here

- Download the file [logging_config_loki.yaml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/logging/logging_config_loki.yaml)
and store it in the root directory of the project.
- Download the file [docker-compose.yml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/logging/docker-compose.yml)
and store it in the root directory of the project.
- Start Grafana and Loki by running `docker-compose up -d`.
- Stop the kitchen service and start it again using 
`uv run uvicorn kitchen_service:app --port 8001 --reload --log-config logging_config_loki.yaml`.

Your logs are now sent to Loki in addition to the console output. You can now use Grafana to explore the logs.
1. Open Grafana at http://localhost:3000
1. Navigate to _Menu > Connections > Add new connection_ 
1. Search for the _Loki_ data source and add it
1. Set the connection URL to: `http://loki:3100`
1. Click _Save & Test_
1. Navigate to _Menu > Explore_ and make sure that the _Loki_ data source is selected

You can either use the _Builder_  or _Code_ view to query your logs. Start off with the 
builder, but later on get familiar with the code view as well, as this is the quickest and most 
powerful way to explore logs. You can first build a query and then switch to the _Code_ 
view to see the query that was generated.

- Find all logs created in the last 5 minutes from the kitchen service that contain the word _cook_ 
- Find all 503 HTTP errors across services that occurred in the last 5 minutes

## Collecting Metrics with Prometheus

Logs are great for understanding what happened in your application. Metrics help you understand
how your application is performing. We use Prometheus to collect, store and query metrics.

- Download the file [docker-compose.yml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/metrics/docker-compose.yml)
and overwrite the existing one in the root directory of the project.
- Download the file [prometheus.yml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/metrics/prometheus.yml)
and store it in the root directory of the project.
- Start Prometheus by running `docker-compose up -d`
- Add the following lines to the `kitchen_service.py`, right after the creation of the `FastAPI` instance
```python
from prometheus_fastapi_instrumentator import Instrumentator
# Initialize Prometheus metrics instrumentation
Instrumentator().instrument(app).expose(app)
```

The smoothie shop now exposes HTTP metrics automatically using the `prometheus-fastapi-instrumentator`
library. This library automatically instruments all HTTP endpoints and provides metrics like:
- `http_requests_total` - Total number of HTTP requests
- `http_request_duration_seconds` - Duration of HTTP requests
- `http_requests_in_progress` - Number of requests currently being processed

To view the metrics:
1. Make sure that both services are reloaded and generate some traffic
1. Open Prometheus at http://localhost:9090
1. Try these queries and have a look at the table and graph results:
   - Request rate per second: `rate(http_requests_total[1m])` -
   - Average request duration: `http_request_duration_seconds_sum / http_request_duration_seconds_count` 
   - Average request duration calls to `/prepare`: `http_request_duration_seconds_sum{handler="/prepare"} / http_request_duration_seconds_count{handler="/prepare"}`
   
You can also view the raw metrics at:
- Kitchen Service: http://localhost:8001/metrics

We are now going to add a business level metric. Consider, we want to know how many smoothies
are ordered per flavour. We can add a custom Prometheus counter metric to provide this information. 

In `kitchen_service.py` add the following right after the creation of the `Instrumentator`:
```python
# Custom metric: Count smoothies ordered by flavor
from prometheus_client import Counter
smoothies_ordered = Counter(
    'smoothies_ordered_total',
    'Total number of smoothies ordered',
    ['flavor']
)
```
Then add the following to the start of the `prepare_smoothie` function:
```python
# Increment the counter for this flavor
smoothies_ordered.labels(flavor=order.flavor).inc()
```

Make sure that the kitchen service is reloaded, generate some traffic and head over to 
Prometheus to answer the following questions:
- Total smoothies ordered by flavor: `smoothies_ordered_total`
- Most popular flavor: `topk(1, smoothies_ordered_total)`
- Rate of smoothies ordered per flavor: `rate(smoothies_ordered_total[5m])`

## Distributed Tracing with Jaeger

Logs show you what happened. Metrics show you how your system performs. Traces help you understand
the flow of requests across multiple services. When a customer orders a smoothie, the request flows from
the order service to the kitchen service. Distributed tracing helps you see this entire journey.

We use OpenTelemetry to automatically create traces for all HTTP requests and also to connect 
traces between services with each other. We use Jaeger to collect and visualize these traces.

- Download the file [docker-compose.yml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/tracing/docker-compose.yml)
and overwrite the existing one in the root directory of the project.
- Start Jaeger by running `docker-compose up -d`
- Add the following code to the top of `kitchen_service.py`
```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource

# Configure OpenTelemetry tracing
resource = Resource.create({"service.name": "kitchen-service"})
trace.set_tracer_provider(TracerProvider(resource=resource))
# This is going to export the tracing data to Jaeger
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))
```
- Add the following code to `kitchen_service.py` after the creation of the `FastAPI` instance
```python
# This is going to hook into FastAPI and automatically create traces for all HTTP requests
FastAPIInstrumentor.instrument_app(app, exclude_spans=["receive", "send"])
# This is going to hook into HTTPX to automatically create traces for all outgoing HTTP requests and to 
# connect traces between services with each other
HTTPXClientInstrumentor().instrument()
```
- Insert the same code blocks into `order_service.py` as well. Make sure to change the `service.name` to `order-service`.

The smoothie shop now uses OpenTelemetry to automatically create traces for all HTTP requests and 
connect traces between services. This means you can follow a single order from the moment it
arrives at the order service through to when the kitchen prepares it.

To view traces:
1. Make sure that both services are reloaded and generate some traffic
1. Open Jaeger UI at http://localhost:16686
1. Select `order-service` from the _Service_ dropdown and `POST /order` from the _Operation_ dropdown
1. Click _Find Traces_ to see all traces
1. Click on any trace to see the detailed view

When you open a trace, you'll see:
- **Timeline**: Visual representation of when each span started and how long it took
- **Service Dependencies**: How the order service calls the kitchen service
- **Error Traces**: If the kitchen is busy (503 error), you'll see red spans indicating failures
- **Tags**: Additional information about the span, e.g. the HTTP method or status code

Try to answer these questions using Jaeger:
- How many services are involved in a single smoothie order?
- Can you identify the slowest part of the request flow?
- When the kitchen returns a 503 error, how much time was spent before the error occurred?

The used instrumentors hook into libaries automatically and create spans. You can also create spans manually
using the OpenTelemetry API. Consider, we want to know how long we have to wait for a cook to become available
and how long it actually takes to prepare a smoothie. In `kitchen_service.py`, replace the `prepare_smoothie` function with:
```python
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
```

The kitchen service now includes two custom spans that show internal operations:
- **wait_for_cook**: Shows how long an order waits for an available cook
- **prepare_smoothie**: Shows the actual smoothie preparation time

These custom spans help you understand:
- **Queue time vs work time**: Is most time spent waiting or working?
- **Bottlenecks**: If wait_for_cook is long, you need more cooks (increase NUM_COOKS)
- **Business metrics**: Each span includes the flavor as an attribute, which would allow you to spot differences in preparation times between flavors

To view traces:
1. Make sure that both services are reloaded and generate some traffic
1. Find traces in the Jager UI and inspect the span hierarchy

## Correlating Logs and Traces

You will get the most out of all this information if you can correlate different signals together. Poor man's 
corellation can be done using timestamps, but this is not very useful if there are multiple requests going
on at the same time. We will therefore now correlate logs and traces using trace IDs.

Add the following code block to both, `order_service.py` and `kitchen_service.py`:
```python
from opentelemetry.instrumentation.logging import LoggingInstrumentor
# Instrument logging to automatically inject trace context into all log records
def log_hook(span, record):
    if not hasattr(record, "tags"):
        record.tags = {}
    record.tags["service_name"] = resource.attributes["service.name"]
    record.tags["trace_id"] = format(span.get_span_context().trace_id, "032x")
LoggingInstrumentor().instrument(log_hook=log_hook)
```

This is going to add two labels called `service_name` and `trace_id` to all your log records, which you can 
use to search for all log outputs across services that belong to a specific HTTP call. 

1. Make sure that both services are reloaded and generate some traffic
1. Open Jaeger UI at http://localhost:16686
1. Copy the trace id from the URL
1. Go to Grafana and search for the label `trace_id` with the value you copied 

### Further Readings and Exercises

TODOs -

logging instrumentation

merge into master
configure loki to keep the logs in the project directory
make sure that grafana keeps the logs
open telemetry collector instead of sending to loki directly
PromQL
Metric types, e.g. counters, gauges, histograms, etc. 
Volumes might cause problems with Windows. Needs to be investigated.
OpenTelemetry Traces, Spans, etc.

- Read through the [Python Logging HOWTO](https://docs.python.org/3/howto/logging.html)
- https://docs.python.org/3/library/logging.html#
- Introduce proper logging in the order service
- Correlate log messages
- Add a Grafana dashboard to monitor HTTP requests. Make sure that it's filterable by service

https://grafana.com/docs/loki/latest/query/