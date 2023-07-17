from grafanalib.core import (
    Row, Target, YAxis, YAxes, Tooltip, RED, GREEN,
)

from common.python.grafana.core import DEFAULT_LOOKBACK_WINDOW
from common.python.grafana.panels import graph
from common.python.grafana.query import format_params, Sum, InstantVector, RangeVector, Rate, Min


def information_row(
        rate_fn="rate",
        lookback_window=DEFAULT_LOOKBACK_WINDOW,
        deployment_name="[[deployment]]",
        title="Deployment Information",
):
    """
    Returns a Grafana Row containing three Panels.
    1) CPU Usage.
    2) Memory usage.
    3) Available and Unavailable replicas.
    """
    # Define param.
    params = format_params({
        "pod": f"~{deployment_name}-[a-z0-9]+[-]?[a-z0-9]*$",
        "container": "!",
    })

    # Define CPU usage expression.
    cpu_usage = Sum(
        vector=Rate(
            vector=RangeVector(
                metric="container_cpu_usage_seconds_total",
                params=params,
                lookback_window=lookback_window,
            ),
            rate_function=rate_fn,
        ),
        by=["pod"],
    )
    cpu_params = format_params({
        "pod": f"~{deployment_name}-[a-z0-9]+[-]?[a-z0-9]*$",
        "container": "!",
        "resource": "cpu",
    })
    cpu_requests = Min(
        vector=InstantVector(
            metric="kube_pod_container_resource_requests",
            params=cpu_params,
        ),
    )
    cpu_limits = Min(
        vector=InstantVector(
            metric="kube_pod_container_resource_limits",
            params=cpu_params,
        ),
    )

    # Define Memory usage expression.
    memory_usage = Sum(
        vector=InstantVector(
            metric="container_memory_working_set_bytes",
            params=params,
        ),
        by=["pod"],
    )
    mem_params = format_params({
        "pod": f"~{deployment_name}-[a-z0-9]+[-]?[a-z0-9]*$",
        "container": "!",
        "resource": "memory",
    })
    memory_requests = Min(
        vector=InstantVector(
            metric="kube_pod_container_resource_requests",
            params=mem_params,
        ),
    )
    memory_limits = Min(
        vector=InstantVector(
            metric="kube_pod_container_resource_limits",
            params=mem_params,
        ),
    )

    # Define replica status expressions.
    params = format_params({"pod": f"~{deployment_name}.*"})
    pod_status = Sum(
        vector=InstantVector(
            metric="kube_pod_status_phase",
            params=params,
        ),
        by=["phase"],
    )

    return Row(
        title=title,
        panels=[
            graph(
                title=f"CPU Usage [{lookback_window}]",
                description="CPU usage of the deployment.",
                seriesOverrides=[
                    {
                        "alias": "request",
                        "dashes": True,
                        "linewidth": 2,
                        "color": GREEN,
                    },
                    {
                        "alias": "limit",
                        "dashes": True,
                        "linewidth": 2,
                        "color": RED,
                    },
                ],
                targets=[
                    Target(
                        expr=str(cpu_usage),
                        legendFormat="{{pod}}",
                    ),
                    Target(
                        expr=str(cpu_requests),
                        legendFormat="request",
                    ),
                    Target(
                        expr=str(cpu_limits),
                        legendFormat="limit",
                    ),
                ],
                yAxes=YAxes(left=YAxis(label="CPU cores", format="short")),
                tooltip=Tooltip(sort=2),
            ),
            graph(
                title="Memory Usage",
                description="Memory usage of the deployment.",
                seriesOverrides=[
                    {
                        "alias": "request",
                        "dashes": True,
                        "linewidth": 2,
                        "color": GREEN,
                    },
                    {
                        "alias": "limit",
                        "dashes": True,
                        "linewidth": 2,
                        "color": RED,
                    },
                ],
                targets=[
                    Target(
                        expr=str(memory_usage),
                        legendFormat="{{pod}}",
                    ),
                    Target(
                        expr=str(memory_requests),
                        legendFormat="request",
                    ),
                    Target(
                        expr=str(memory_limits),
                        legendFormat="limit",
                    ),
                ],
                yAxes=YAxes(left=YAxis(label="RAM", format="bytes")),
                tooltip=Tooltip(sort=2),
            ),
            graph(
                title="Pod Status",
                description="Shows the status of the various replicas of this deployment.",
                targets=[
                    Target(
                        expr=str(pod_status),
                        legendFormat="{{phase}}",
                    ),
                ],
                yAxes=YAxes(left=YAxis(label="pods", format="short", decimals=0)),
            ),
        ],
    )
