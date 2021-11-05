import re

import click
import requests
from more_itertools import flatten


class MetricCleaner:
    def __init__(self, baseurl):
        self.baseurl = baseurl

    def read_metrics(self) -> list:
        """Reads all metrics from pushgateway"""
        r = requests.get(self.baseurl)
        r.raise_for_status()
        return r.text.split("\n")

    def filter_metrics(self, all_metrics: list, prefix: str) -> set:
        """Filters metrics based on the prefix input"""
        pattern = re.compile(f'job="({prefix}\w*)')
        filtered_metrics = filter(pattern.findall, all_metrics)
        filtered_metrics = map(pattern.findall, filtered_metrics)
        return set(flatten(filtered_metrics))

    def delete_metric(self, metric: str) -> None:
        """ Deletes the metric from pushgateway """
        url = f"{self.baseurl}/job/{metric}"
        r = requests.delete(url)
        r.raise_for_status()

    def run(self, prefix: str) -> None:
        all_metrics = self.read_metrics()
        filtered = self.filter_metrics(all_metrics, prefix)
        list(map(self.delete_metric, filtered))


@click.command()
@click.option(
    "--prefix",
    required=True,
    help="Remove all metrics with given prefix in job.",
)
@click.option(
    "--baseurl", help="Url of pushgateway", default="http://localhost:9091/metrics"
)
def cli(prefix: str, baseurl: str) -> None:
    """Remove metrics with given prefix in job."""

    cleaner = MetricCleaner(baseurl=baseurl)
    cleaner.run(prefix)


if __name__ == "__main__":
    cli()
