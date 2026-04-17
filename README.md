# DEV2IL: Observability

## 🛫 Getting Started

We will build on the codebase of this repository. Fork this repository and make sure you're 
on the `main` branch.

And just a reminder: The easiest way to get going is to use PyCharm! Clone the repository
from within PyCharm or open it in PyCharm if you clone from the console. Use the PyCharm editor to create
and edit files, and the built-in terminal to run commands. When opening the PyCharm built-in terminal
make sure to use the WSL if you are on Windows.

## The Smoothie Shop

The smoothie shop application allows users to order delicious smoothies. It consists of two microservices:
- The Order Service: Accepts smoothie orders
- The Kitchen Service: Prepares the smoothies

![Smoothie Shop](smoothie-shop.png)

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

## Logging

### Providing More Insights Through Log Outputs

Before we are going to start changing our services, let's get familiar with the code. 
Have a closer look at `kitchen_service.py` and `order_service.py`. Discuss these questions with your partner:
- How do the services work? 
- How are are they connected to each other? 
- Why does the kitchen sometimes returns a 503 error?

We are now providing more insights into the smoothie shop by adding logging to the application. 

Python's [logging module](https://docs.python.org/3/howto/logging.html) provides a flexible way 
to record what your application is doing. A **logger** is an object that you use to write log messages. You can call 
methods like `logger.info()`, `logger.debug()`, `logger.error()` to record messages at different severity levels. 

Logging is **configurable**: you can decide where logs go (console, file, network), how they're formatted (timestamps, 
colors), and which messages to show (e.g., only `INFO` and above, or include `DEBUG` for troubleshooting).

Here are some hints on which logging level to choose from 
the [Python logging HOWTO](https://docs.python.org/3/howto/logging.html#when-to-use-logging): 

| Level     | When it’s used                                                                                       |
|-----------|------------------------------------------------------------------------------------------------------|
| DEBUG     | Detailed information, typically of interest only when diagnosing problems.                           |
| INFO      | Confirmation that things are working as expected.                                                    |
| WARNING   | An indication that something unexpected happened, or indicative of some problem in the near future. (e.g. ‘disk space low’). The software is still working as expected. |
| ERROR     | Due to a more serious problem, the software has not been able to perform some function.              |
| CRITICAL  | A serious error, indicating that the program itself may be unable to continue running.               |

Let's get started to modify `kitchen_service.py`.

After the existing imports create a logger for the module
```python
import logging
logger = logging.getLogger(__name__)
``` 

💡 Using `logging.getLogger(__name__)` is a best practice. It creates a logger named after your module 
(e.g., `kitchen_service`). This makes it easy to identify where log messages come from and allows you 
to configure logging levels differently for each Python module.

Add these log messages to the `prepare_smoothie` function. Find the right places to add them on your own. 
```python
logger.info(f"Received order to prepare a smoothie with flavor {order.flavor}")
logger.debug(f"Waiting for a cook to become available")
logger.error(f"Can't process the order: {NUM_COOKS} cooks are currently busy. Consider increasing NUM_COOKS.")
logger.info(f"Smoothie with flavor {order.flavor} prepared")
```

**Configuring Logging:** To control how logging works, we need to configure it. Python's logging system has three main parts:
- **Logger**: Creates log messages (what you use in your code with `logger.info()`, `logger.debug()`, etc.)
- **Handler**: Decides where log messages go (e.g., console, file, network)
- **Formatter**: Defines how log messages look like (e.g., add timestamp, log level, message)

The `logging_config.yaml` file is a YAML format that Python's logging module understands. It lets you configure 
loggers, handlers, and formatters without changing your code.

![logging_config.yaml](logging-config.png)

We want all our logging messages to contain the logging level, a timestamp when the message was logged and 
the message itself. In addition, we want to be able to define the logging level for each logger individually. 

Download the file [logging_config.yaml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/logging/logging_config.yaml)
and store it in the root directory of the project.

Stop the kitchen service and start it again using 
`uv run uvicorn kitchen_service:app --port 8001 --reload --log-config logging_config.yaml`.  

🎉 You are now able to get more insights into what your application is actually doing and
you should thus be able to figure out what is going wrong and how to fix it.

💡 Make sure that you understand `logging_config.yaml` and how it works before you continue. Match it 
to the structural picture from above.

### 🚀 Level Up

#### Challenge 1: Log the Service Startup

Add a log message that is written when the kitchen service starts. Include helpful configuration information such as
the service name and the value of `NUM_COOKS`.

This helps you understand that logs are not only useful while handling requests, but also when checking how a service
was started.

#### Challenge 2: Introduce Logging in the Order Service

Introduce a logger in `order_service.py` and add meaningful log messages for the most important steps of handling an 
order. For example, you could log when an order is received, when the order service calls the kitchen service, and 
whether the smoothie order was completed successfully or failed.

Choose log levels that make sense for the different messages. Keep the logging simple and focus on messages that help 
you understand the flow of a single order.

#### Challenge 3: Experiment with Log Levels

Change the log levels in `logging_config.yaml` and compare what you can see with `INFO` and `DEBUG`. Remember to 
restart the kitchen service after you changed the logging configuration.

Try to answer these questions:
- Which messages do you only see with `DEBUG`?
- Which messages are useful during normal operation?
- Which messages are mainly helpful for troubleshooting?

### Collecting Logs in a Central Place

In order to be able to analyze logs, you need to collect the logs of all your services in a central place and
make them searchable. We use [Loki](https://grafana.com/oss/loki/) to store logs and [Grafana](https://grafana.com/)
to query and visualize them.

**Loki** is a log aggregation system designed to store and index logs efficiently. Think of it as a database 
specifically built for logs. It collects log messages from all your services and stores them in one place, 
making it easy to search through logs from multiple services at once.

**Grafana** is a visualization and analytics platform. It provides a user-friendly web interface where you can 
search, filter, and visualize your logs. While Loki stores the logs, Grafana helps you explore and understand them.

![logging_config.yaml](logging-loki.png)

The image above shows the updated logging configuration. Notice that we now have **two handlers**:
1. **Console handler**: Continues to display logs in your terminal (like before)
2. **Loki handler**: A new handler that sends the same log records to Loki over the network

When a log message is created, both handlers receive it. This means your logs appear both in your terminal and in Loki.

- Download the file [logging_config_loki.yaml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/logging/logging_config_loki.yaml)
and store it in the root directory of the project. This is the same as the previous logging configuration, but with an additional handler that sends logs to Loki.
- Download the file [docker-compose.yml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/logging/docker-compose.yml)
and store it in the root directory of the project. Make sure you understand it!
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
view to see the query that was generated. Learn more about Loki queries in the 
[LogQL documentation](https://grafana.com/docs/loki/latest/query/).

- Find all logs created in the last 5 minutes from the kitchen service that contain the word _cook_ 
- Find all 503 HTTP errors across services that occurred in the last 5 minutes

### 🚀 Level Up

#### Challenge 1: Use Structured Information in Logs

Adding structured information (extra attributes) to your log messages makes them much more 
powerful. Instead of just text, you can attach key-value pairs like `num_cooks=3` or `flavor="Mango"`. This structured 
data allows you to filter and query logs more precisely in Grafana/Loki. For example, you can find all logs where 
`num_cooks < 2` or all smoothie orders for a specific flavor.

Attach more information, such as the number of cooks and the number of busy cooks in the kitchen service log outputs. 
Use extra attributes with log messages.

**Example:** Instead of `logger.info(f"Processing order for {flavor}")`, use:
```python
logger.info("Processing order", extra={"flavor": flavor, "num_cooks": NUM_COOKS})
```

Then inspect the logs in Grafana/Loki and search for them. You can filter by these attributes using LogQL queries like 
`{logger="kitchen_service"} | flavor="Mango"` to find all logs for Mango smoothies.

You can always find more information on how to use loggers by contacting the 
[Python logging documentation](https://docs.python.org/3/library/logging.html#logging.Logger.debug)).

#### Challenge 2: Store Logs in a File

Logging to files gives you persistence - even if your application crashes or restarts, the logs 
remain on disk for later inspection.

Let's modify the `logging_config_loki.yaml` file to add a third handler that writes logs to a file.

**Step 1:** Add a file handler to the `handlers` section:

```yaml
handlers:
  console:
    # ... existing console handler ...
  
  loki:
    # ... existing loki handler ...
  
  file:
    class: logging.FileHandler
    formatter: console
    filename: logs/smoothie-shop.log
```

**Step 2:** Add the `file` handler to the root logger's handler list:

```yaml
root:
  handlers:
    - console
    - loki
    - file  # Add this line
```

**Step 3:** Create the `logs/` directory and restart your service:

```bash
mkdir -p logs
uv run uvicorn kitchen_service:app --port 8001 --reload --log-config logging_config_loki.yaml
```

**Test it:** Generate some traffic and check that `logs/smoothie-shop.log` contains your log messages:

```bash
cat logs/smoothie-shop.log
``` 

#### Pro Tip: Use a Log Collector for Reliable Log Shipping

Sending log messages directly from your application to Loki has several problems:
- If the network is down or your application crashes, logs are lost
- If Loki is slow when accepting logs, your application will be slow as well
- Your application becomes tightly coupled to your logging infrastructure

**The better approach:** Log to standard output (stdout) or files, and use a separate log collector process that reads 
those logs and forwards them to your logging system. This decouples your application from the logging infrastructure and 
ensures logs are preserved even if the network fails.

**Real-world cloud setups:** In production cloud environments (AWS, Azure, GCP), there are typically pre-configured tools 
that automatically collect JSON log lines from stdout and send them to cloud-specific logging services (like Google Cloud Logging). 
You simply write logs to stdout in JSON format, and the infrastructure handles the rest.

**Open source option:** The [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) is an open source tool 
that can collect logs from files or stdout and forward them to various destinations including Loki. It's vendor-neutral 
and widely used.

💡 **Note:** Setting up a log collector is beyond the scope of this lecture. This is provided 
as informational background to show you how logging works in real production systems.

## Collecting Metrics with Prometheus

Logs are great for understanding what happened in your application. Metrics help you understand
how your application is performing. We use Prometheus to collect, store and query metrics.

- Download the file [docker-compose.yml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/metrics/docker-compose.yml)
and overwrite the existing one in the root directory of the project. Make sure you understand it!
- Download the file [prometheus.yml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/metrics/prometheus.yml)
and store it in the root directory of the project. Make sure you understand it!
- Start Prometheus by running `docker-compose up -d`
- Add the following lines to the `kitchen_service.py`, right after the creation of the `FastAPI` instance
```python
from prometheus_fastapi_instrumentator import Instrumentator
# Initialize Prometheus metrics instrumentation
Instrumentator().instrument(app).expose(app)
```

![Prometheus](prometheus.png)

The smoothie shop now exposes HTTP metrics automatically using the `prometheus-fastapi-instrumentator`
library. This library automatically instruments all HTTP endpoints and provides metrics like:
- `http_requests_total` - Total number of HTTP requests (counter)
- `http_request_duration_seconds_sum` - Duration of HTTP requests (counter)

Make sure that both services are reloaded and generate some traffic.

Make sure that both services are reloaded and generate some traffic. You can look at the latest state
of the kitchen service's metrics by visiting: http://localhost:8001/metrics. This is the page that 
Prometheus scrapes regularly to collect metrics data. Search for `http_requests_total` to find the total 
number of HTTP requests that the kitchen service has received up until the point in time you loaded the page.

To view the metrics:
1. Open Prometheus at http://localhost:9090
1. Enter the query `http_requests_total` and inspect the table results. The table shows all available time series for this metric.
1. Filter down to one dedicated time series through a label, for example: `http_requests_total{handler="/prepare"}`
1. Use `sum(http_requests_total)` to get the total number of requests across all time series
1. Try these queries and have a look at the table and graph results:
   - Request rate per minute: `rate(http_requests_total[1m])`
   - Average request duration: `http_request_duration_seconds_sum / http_request_duration_seconds_count` 
   - Average request duration calls to `/prepare`: `http_request_duration_seconds_sum{handler="/prepare"} / http_request_duration_seconds_count{handler="/prepare"}`
   
The query language that you are just using is called "PromQL". It is a powerful language to query and 
aggregate metrics data. You can find more information about it in the 
[Prometheus documentation](https://prometheus.io/docs/prometheus/latest/querying/basics/).

We are now going to add our own business level metric. Consider, we want to know how many smoothies
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

### 🚀 Level Up

#### Challenge 1: Understand `increase()`

The metric `smoothies_ordered_total` is a **counter**. A counter only goes up. It tells you the
total number of smoothies ordered since the service started.

Sometimes this total number is not what you want. Often, you want to know:

> How many new smoothies were ordered in the last 1 minute?

This is exactly what `increase()` does.

Try it out in Prometheus:
- Show the total number of ordered smoothies: `smoothies_ordered_total`
- Order a few smoothies and run: `increase(smoothies_ordered_total[1m])`
- Group the result by flavor: `sum by (flavor) (increase(smoothies_ordered_total[1m]))`

If the counter for Mango went from 12 to 17 during the last 1 minute, then 
`increase(smoothies_ordered_total{flavor="Mango"}[1m])` is `5`.

Try to answer these questions:
- What is the difference between `smoothies_ordered_total` and `increase(smoothies_ordered_total[1m])`?
- Which query would you use to show all smoothies ever ordered?
- Which query would you use to show only recent smoothie orders?

#### Challenge 2: Build Your Own Grafana Dashboard

Create your own Grafana dashboard for smoothie orders. Try to work you through Grafana intuitively or
use an assistant like Geminin in order to point you to the right places in Grafana.

Before you start building the dashboard, make sure that Grafana can talk to Prometheus. You are already
able to create a new connection with Prometheus as you already did it for Loki and you have the 
`docker-compose.yml` file that contains all the relevant information. 

<details>
<summary>Show hints for connecting Grafana to Prometheus</summary>
<ol>
  <li>Open Grafana at <a href="http://localhost:3000">http://localhost:3000</a></li>
  <li>Navigate to <em>Menu &gt; Connections &gt; Add new connection</em></li>
  <li>Search for the <em>Prometheus</em> data source and add it</li>
  <li>Set the connection URL to <code>http://prometheus:9090</code></li>
  <li>Click <em>Save &amp; Test</em></li>
</ol>
</details>

Your dashboard should contain:
1. A **Stat** panel that shows how many smoothies were ordered in the currently selected time range
1. A **Time series** panel that shows smoothie orders over time
1. A dashboard **variable** called `flavor` so that users can filter the dashboard

Useful PromQL queries:
- Variable query: `label_values(smoothies_ordered_total, flavor)`
- Stat panel: `sum(increase(smoothies_ordered_total{flavor=~"$flavor"}[$__range]))`
- Time series panel: `sum(increase(smoothies_ordered_total{flavor=~"$flavor"}[1m]))`
- Optional: show one line per flavor with `sum by (flavor) (increase(smoothies_ordered_total[1m]))`

Hints:
- Enable an **All** option for the `flavor` variable
- Use `increase(...)` for recent activity instead of the raw counter
- Check whether changing the `flavor` variable updates both panels

Try to answer these questions:
- Why is a **Stat** panel a good fit for a single important number?
- Why is a **Time series** panel a better fit for changes over time?
- Why is `increase(...)` easier to understand for a user in a dashboard than the raw counter value?

## Distributed Tracing with Jaeger

Logs show you what happened. Metrics show you how your system performs. Traces help you understand
the flow of requests across multiple services. When a customer orders a smoothie, the request flows from
the order service to the kitchen service. Distributed tracing helps you see this entire journey.

We use OpenTelemetry to automatically create traces for all HTTP requests and also to connect 
traces between services with each other. We use Jaeger to collect and visualize these traces.

- Download the file [docker-compose.yml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/tracing/docker-compose.yml)
and overwrite the existing one in the root directory of the project. Make sure you understand it!
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

![Jaeger](jaeger.png)

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
- **Bottlenecks**: If `wait_for_cook` is long, you need more cooks (increase NUM_COOKS)
- **Business metrics**: Each span includes the flavor as an attribute, which would allow you to spot differences in preparation times between flavors

To view traces:
1. Make sure that both services are reloaded and generate some traffic
1. Find traces in the Jager UI and inspect the span hierarchy

### 🚀 Level Up

#### Challenge 1: Jump from a Jaeger Trace to the Matching Logs (Trace ID)

You will get the most out of all this information if you can correlate different signals together.
Correlating by timestamps is possible, but it breaks down quickly when multiple requests happen at the same time.

**Goal:** When you see a slow or failing trace in Jaeger, instantly find the **exact log lines** for that same request.

**Step 1: Put `trace_id` into every log line**

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

This adds two labels to all your log records:
- `service_name`
- `trace_id`

**Step 2: Search for Logs of a Trace**

1. Reload both services and generate some traffic
2. Open Jaeger UI at http://localhost:16686 and open any trace
3. Copy the trace id from the URL
4. Go to Grafana and search for logs with `trace_id=<the value you copied>`

#### Challenge 2: Zero-code Tracing via CLI Auto-Instrumentation

In this challenge, you will add tracing **without changing a single line of application code**.

**Setup**
- Commit all your changes, so you don't lose any work. You can also stash your changes if you want to keep them but don't want to commit them yet.
- Keep your current `docker-compose` services running (Jaeger, Grafana, etc.)
- Checkout the `metrics` branch

**Important:** The `metrics` branch does **not** contain any tracing code.

**Step 1: Install auto-instrumentation libraries**

```bash
uv add opentelemetry-distro opentelemetry-exporter-otlp opentelemetry-instrumentation-fastapi
```
**Step 2: Restart the services using `opentelemetry-instrument`**

> 💡 Hint: remove the `--reload` flag. Auto-instrumentation + reload often does not behave well.

In one terminal:

```bash
export OTEL_SERVICE_NAME=order-service
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
uv run opentelemetry-instrument uvicorn order_service:app --port 8000 --log-config logging_config.yaml
```

In a second terminal:

```bash
export OTEL_SERVICE_NAME=kitchen-service
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
uv run opentelemetry-instrument uvicorn kitchen_service:app --port 8001 --log-config logging_config.yaml
```

**Step 3: Verify**
1. Generate traffic (order a few smoothies)
2. Open Jaeger again (http://localhost:16686)
3. You should now see traces even though the code contains no tracing

More information:
- https://opentelemetry.io/docs/zero-code/python/

## Further Readings

Logging
- https://docs.python.org/3/howto/logging.html
- https://docs.python.org/3/library/logging.html
- https://grafana.com/docs/loki/latest/query

Metrics
- https://prometheus.io/docs/concepts/metric_types/
- https://prometheus.io/docs/prometheus/latest/querying/basics/
- https://prometheus.io/docs/alerting/latest/overview/

Tracing
- https://opentelemetry.io/docs/concepts/
- https://opentelemetry.io/docs/concepts/instrumentation/zero-code/