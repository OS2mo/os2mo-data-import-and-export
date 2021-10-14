# Inspired by:
# https://github.com/prometheus/client_python#exporting-to-a-pushgateway
from prometheus_client import CollectorRegistry
from prometheus_client import Gauge
from prometheus_client import push_to_gateway


def export_metrics(metrics: dict):
    for job, value in metrics.items():
        export_metric(job, value)


def export_metric(job: str, value: int):
    registry = CollectorRegistry()
    g = Gauge("os2mint", "OS2MO integration metric", registry=registry)
    g.set(value)
    # TODO: Consider changing to read from settings
    push_to_gateway("localhost:9091", job=job, registry=registry)


if __name__ == "__main__":
    export_metric("test", 1)
