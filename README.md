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
`uv run uvicorn kitchen_service:app --port 8001 --reload --log-config logging_config.yaml`.

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

Logs are great for understanding what happened in your application, but metrics help you understand
how your application is performing. We use Prometheus to collect, store and query metrics.

- Download the file [docker-compose.yml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/metrics/docker-compose.yml)
and overwrite the existing one in the root directory of the project.
- Download the file [prometheus.yml](https://github.com/peterrietzler/ais-dev2il-smoothie-shop/blob/metrics/prometheus.yml)
and store it in the root directory of the project.
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
1. Make sure Prometheus is running: `docker-compose up -d`
2. Make sure that both services are reloaded and generate some traffic
4. Open Prometheus at http://localhost:9090
5. Try these queries and have a look at the table and graph results:
   - Request rate per second: `rate(http_requests_total[1m])` -
   - Average request duration: `http_request_duration_seconds_sum / http_request_duration_seconds_count` 
   - Average request duration calls to `/prepare`: `http_request_duration_seconds_sum{handler="/prepare"} / http_request_duration_seconds_count{handler="/prepare"}`
   
You can also view the raw metrics at:
- Kitchen Service: http://localhost:8001/metrics

We are now going to add a business level metrics. Consider, we want to know how many smoothies
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

### Further Readings and Exercises

TODOs -

merge into master
configure loki to keep the logs in the project directory
make sure that grafana keeps the logs
open telemetry collector instead of sending to loki directly
PromQL
Metric types, e.g. counters, gauges, histograms, etc. 
Volumes might cause problems with Windows. Needs to be investigated.

- Read through the [Python Logging HOWTO](https://docs.python.org/3/howto/logging.html)
- https://docs.python.org/3/library/logging.html#
- Introduce proper logging in the order service
- Correlate log messages
- Add a Grafana dashboard to monitor HTTP requests. Make sure that it's filterable by service

https://grafana.com/docs/loki/latest/query/