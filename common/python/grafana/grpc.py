from grafanalib.core import Dashboard, Row, Target, YAxis, YAxes, Legend

from common.python.grafana.core import (
    templating,
    template,
    DEFAULT_PERCENTILES,
    DEFAULT_LOOKBACK_WINDOW,
)
from common.python.grafana.deployment import information_row
from common.python.grafana.query import format_params, Sum, RangeVector, Rate, HistogramQuantile
from common.python.grafana.panels import graph


def server_row_template(server_deployment_name: str):
    """
    Returns a Grafana Template for a gRPC server.
    Provides required template for using server_row()
    @server_deployment_name: The kubernetes deployment name of the gRPC server host.
    """
    return template(
        metric="grpc_server_handled_total",
        display_name="grpc_method",
        metric_label="grpc_method",
        params={
            "kubernetes_name": server_deployment_name,
        },
        all_value=".*",
        include_all=True,
    )


def server_row(
    server_deployment_name: str,
    rate_fn: str = "rate",
    lookback_window: str = DEFAULT_LOOKBACK_WINDOW,
    percentiles=DEFAULT_PERCENTILES,
):
    """
    Returns a Grafana Row for a gRPC server.
    @server_deployment_name: The kubernetes deployment name.
    """

    # Define RPS expression.
    params = format_params(
        {
            "kubernetes_name": server_deployment_name,
            "grpc_method": "~$grpc_method",
        }
    )
    rps = Sum(
        vector=Rate(
            vector=RangeVector(
                metric="grpc_server_handled_total",
                params=params,
                lookback_window=lookback_window,
            ),
            rate_function=rate_fn,
        ),
        by=["grpc_method"],
    )

    # Define processing time expression.
    processing_time = Sum(
        vector=Rate(
            vector=RangeVector(
                metric="grpc_server_handling_seconds_bucket",
                params=params,
                lookback_window=lookback_window,
            ),
            rate_function=rate_fn,
        ),
        by=["le"],
    )

    # Define error breakdown.
    params1 = format_params(
        {
            "kubernetes_name": server_deployment_name,
            "grpc_method": "~$grpc_method",
            "grpc_code": "!OK",
        }
    )
    params2 = format_params(
        {
            "kubernetes_name": server_deployment_name,
            "grpc_method": "~$grpc_method",
        }
    )
    grpc_handled = Rate(
        vector=RangeVector(
            metric="grpc_server_handled_total",
            params=params1,
            lookback_window=lookback_window,
        ),
        rate_function=rate_fn,
    )
    grpc_started = Rate(
        vector=RangeVector(
            metric="grpc_server_started_total",
            params=params2,
            lookback_window=lookback_window,
        ),
        rate_function=rate_fn,
    )
    grpc_handled_by_method = Sum(
        vector=grpc_handled,
        by=["grpc_method"],
    )
    grpc_started_by_method = Sum(
        vector=grpc_started,
        by=["grpc_method"],
    )
    grpc_handled_by_method_and_code = Sum(
        vector=grpc_handled,
        by=["grpc_method", "grpc_code"],
    )

    return Row(
        title=f"gRPC Server ({server_deployment_name})",
        panels=[
            graph(
                title=f"RPS [{lookback_window}]",
                description="Requests/s as perceived by the server.",
                span=6,
                legend=Legend(show=True, hideZero=True),
                targets=[
                    Target(
                        expr=str(rps),
                        legendFormat="{{grpc_method}}",
                    ),
                ],
                yAxes=YAxes(left=YAxis(label="requests/s", format="short")),
            ),
            graph(
                title=f"Processing time [{lookback_window}]",
                description="Quantile distribution of RPC processing time "
                "as perceived by the server.",
                span=6,
                legend=Legend(show=False, hideZero=True),
                targets=[
                    Target(
                        expr=str(HistogramQuantile(vector=processing_time, quantile=percentile)),
                        legendFormat="%.1f%%ile" % (percentile * 100),
                    )
                    for percentile in percentiles
                ],
                yAxes=YAxes(left=YAxis(label="duration", format="s")),
            ),
            graph(
                title=f"Request error percentage by method [{lookback_window}]",
                description="Error percentage breakdown by method as perceived by the "
                "server. i.e. 30% for method A means method A failed 30% of all requests "
                "during that window.",
                span=6,
                legend=Legend(show=True, hideZero=True),
                targets=[
                    Target(
                        expr=f"{str(grpc_handled_by_method)}/{str(grpc_started_by_method)}*100",
                        legendFormat="{{grpc_method}}",
                    ),
                ],
                yAxes=YAxes(left=YAxis(label="% of requests", format="percent")),
            ),
            graph(
                title=f"Request error percentage breakdown by (method, code) [{lookback_window}]",
                description="Error percentage breakdown by method and code as perceived by "
                "the server. i.e. 30% for method A and code NotFound means method A failed "
                "30% of all requests with error code NotFound durnig that window.",
                span=6,
                legend=Legend(show=True, hideZero=True),
                targets=[
                    Target(
                        expr=f"{str(grpc_handled_by_method_and_code)}"
                        f"/ignoring(grpc_code)group_left {str(grpc_started_by_method)}*100",
                        legendFormat="{{grpc_method}}[{{grpc_code}}]",
                    ),
                ],
                yAxes=YAxes(left=YAxis(label="% of requests", format="percent")),
            ),
        ],
    )


def client_row_template(grpc_service: str):
    """
    Returns a Grafana Template for a gRPC client.
    Provides required template for using client_row()
     @server_deployment_name: The kubernetes deployment name of the gRPC server host.
    """
    return template(
        metric="grpc_client_handled_total",
        display_name="grpc_client",
        metric_label="kubernetes_name",
        params={
            "grpc_service": grpc_service,
            "grpc_method": "~$grpc_method",
        },
        all_value=".*",
        include_all=True,
    )


def client_row(
    grpc_service: str,
    rate_fn: str = "rate",
    lookback_window: str = DEFAULT_LOOKBACK_WINDOW,
    percentiles=DEFAULT_PERCENTILES,
):
    """
    Returns a Grafana Row for a gRPC client.
    @server_deployment_name: The kubernetes deployment name of the gRPC server host.
    """

    # Define RPS expression.
        # Define RPS expression.
    params = format_params(
        {
            "grpc_service": grpc_service,
            "grpc_method": "~$grpc_method",
            "kubernetes_name": "~$grpc_client",
        }
    )
    rps = Sum(
        vector=Rate(
            vector=RangeVector(
                metric="grpc_client_handled_total",
                params=params,
                lookback_window=lookback_window,
            ),
            rate_function=rate_fn,
        ),
        by=["grpc_method"],
    )

    # Define processing time expression.
    processing_time = Sum(
        vector=Rate(
            vector=RangeVector(
                metric="grpc_client_handling_seconds_bucket",
                params=params,
                lookback_window=lookback_window,
            ),
            rate_function=rate_fn,
        ),
        by=["le"],
    )

    return Row(
        title="Client Side Metrics",
        panels=[
            graph(
                title=f"RPS [{lookback_window}]",
                description="Requests/s as perceived by client(s).",
                span=6,
                legend=Legend(show=True, hideZero=True),
                targets=[
                    Target(
                        expr=str(rps),
                        legendFormat="{{grpc_method}}",
                    ),
                ],
                yAxes=YAxes(left=YAxis(label="requests/s", format="short")),
            ),
            graph(
                title=f"Processing time [{lookback_window}]",
                description="Quantile distribution of RPC processing time "
                "as perceived by the client(s).",
                span=6,
                legend=Legend(show=False, hideZero=True),
                targets=[
                    Target(
                        expr=str(HistogramQuantile(vector=processing_time, quantile=percentile)),
                        legendFormat="%.1f%%ile" % (percentile * 100),
                    )
                    for percentile in percentiles
                ],
                yAxes=YAxes(left=YAxis(label="duration", format="s")),
            ),
        ],
    )


def new_dashboard(
    title: str,
    server_deployment_name: str,
    grpc_service: str,
    description="",
    lookback_window=DEFAULT_LOOKBACK_WINDOW,
    rate_fn="rate",
    percentiles=DEFAULT_PERCENTILES,
    tags=[],
    additional_templates=[],
    additional_rows=[],
    shared_crosshair=False,
):
    """
    Returns a Grafana Dashboard with the following rows: gRPC server, gRPC client, deployment info.
    @title: The title of the Dashboard
    @server_deployment_name: The kubernetes deployment name of the gRPC server host.
    @description: The description of the dashboard, included in client-facing documentation.
    @additional_templates: A list of Grafana Templates to be added on top of the default ones.
    @additional_rows: A list of Rows to be added on top of the default ones.
    @shared_crosshair: Hovering over a panel will show the same crosshair duplicated on all panels.
    """
    return Dashboard(
        title=title,
        description=description,
        tags=tags,
        sharedCrosshair=shared_crosshair,
        templating=templating(
            metric = 'grpc_server_handled_total',
            additional_templates=[
                server_row_template(server_deployment_name),
                client_row_template(grpc_service),
            ]
            + additional_templates,
        ),
        refresh="2m",
        rows=[
            server_row(
                server_deployment_name=server_deployment_name,
                rate_fn=rate_fn,
                lookback_window=lookback_window,
                percentiles=percentiles,
            ),
            client_row(
                grpc_service=grpc_service,
                rate_fn=rate_fn,
                lookback_window=lookback_window,
                percentiles=percentiles,
            ),
            information_row(
                deployment_name=server_deployment_name,
                lookback_window=lookback_window,
            ),
        ] + additional_rows,
    ).auto_panel_ids()
