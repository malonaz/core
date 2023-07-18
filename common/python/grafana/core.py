from grafanalib.core import (
    SORT_ALPHA_IGNORE_CASE_ASC, Template, SHOW, HIDE_LABEL, Templating
)
from grafanalib._gen import print_dashboard as _print_dashboard
from typing import Dict
from .query import format_params

# Default percentile distribution used in common library.
DEFAULT_PERCENTILES = [0.5, 0.90, 0.95, 0.99, 1.00]

# Defaults the lookback window to the default template variable.
DEFAULT_LOOKBACK_WINDOW = "$lookback_window"

def print_dashboard(dashboard):
    """ Prints dashboard to stdout."""
    _print_dashboard(dashboard)

def template(
        metric: str,
        display_name: str,
        metric_label: str,
        params: Dict[str, str]={},
        multi: bool=False,
        default_value: str=None,
        include_all: bool=False,
        all_value: str=None,
        regex: str=None,
        hide: bool=False,
        sort: int=SORT_ALPHA_IGNORE_CASE_ASC,
):
    """
    Returns a Grafana template object.
    @metric: metric from which you wish to query values.
    @display_name: name of the template box. This is purely visual.
    @metric_label: this is the metric label where values are fetched. For example , if you have
    a metric `cpu_seconds`, with a label `server_name`, the template will be populated with all
    the values that exist  for `server_name`.
    @params: you may constrain the values you seek to be populated by further specifying params.
    This allows you to chain templates, i.e. you can can restrict the values offered for template
    'b', if you specify params = {'a' = 'some_value'}.
    @default_value: set a default value for this template.
    @include_all: expose an 'all' value for this template variable.
    @regex: filter or capture specific parts of the names return by your data source query.
    """
    template_params = format_params(params) if params else ''
    return Template(
        name=display_name,
        label=display_name,
        default=default_value,
        includeAll=include_all,
        allValue=all_value,
        multi=multi,
        regex=regex,
        query=f"label_values({metric}{template_params}, {metric_label})",
        hide=HIDE_LABEL if hide else SHOW,
        sort=sort,
    )


def templating(
        metric: str = "",
        include_deployment: bool = False,
        additional_templates: list = [],
        lookback_window_values: list = ["2m", "5m", "10m", "30m", "1h", "3h", "6h", "1d", "1w"],
        lookback_window_default: str = "2m",
        include_namespace: bool = False,
        include_pod: bool = False,
        namespace_label: str = 'kubernetes_namespace',
):
    """
    Returns a Grafana Templating object, with some default templates.
    The default templates defined are: Namespace, deployment, Lookback Window.
    These templates are necessary because the common utility functions will
    assume their existence.
    The caller may pass `additional_templates` to add other templates.
    templates.
    @deployment_name: The kubernetes deployment name.
    @ metric: A metric  via which we will query values for the Cluster and Namespace
            variables. This is how the drop-down list is generated. This should only be set if
            deployment_name is empty otherwise deployment_name will be used to generate the list.
    @additional_templates: A list of Graphana Templates to be added on top of the default ones.
    @lookback_window_values: A list of lookback window values. These are the selectable lookback
            periods on the dropdown. If an empty list is provided, the lookback window
            template is omitted.
    @lookback_window_default: The default lookback window value. It will be used when first loading
            the dashboard.
    """

    default_templates = [
        Template(
            name="lookback_window",
            label="Lookback Window",
            default=lookback_window_default,
            type="custom",
            query=",".join(lookback_window_values),
            sort=SORT_ALPHA_IGNORE_CASE_ASC,
        ),
    ]
    if include_namespace:
        default_templates.append(
            Template(
                name="namespace",
                label="Namespace",
                query=f"label_values({metric}{{}},{namespace_label})",
                sort=SORT_ALPHA_IGNORE_CASE_ASC,
                default="default",
            ),
        )

    if include_deployment and include_pod:
        raise ValueError("cannot include both deployment and pod")
    if include_deployment:
        params = {namespace_label: "~[[namespace]]"} if include_namespace else {}
        default_templates.append(
            template(
                metric=metric,
                display_name='deployment',
                metric_label='kubernetes_name',
                params=params,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
        )

    if include_pod:
        params = {namespace_label: "~[[namespace]]"} if include_namespace else {}
        default_templates.append(
            template(
                metric=metric,
                display_name='deployment',
                metric_label='pod',
                regex='^(.*)-(?:[a-z0-9]+-[a-z0-9]+$|[0-9]?$)',
                params=params,
            ),
        )

    return Templating(list= default_templates + additional_templates)
