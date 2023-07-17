from grafanalib.core import (
    Graph, Target, YAxes, Stat, RED, Legend, NULL_CONNECTED, Grid,
    GridPos,
)
from typing import List

def graph(title: str, description: str, targets: List[Target], yAxes: YAxes, **params):
    """
    Defines a Graph. Auto generates refIds for the targets.
    It uses default values, but allows the user to overwrite them if needed.
    """
    params.setdefault('lineWidth', 1)
    params.setdefault('legend', Legend(show=False))
    params.setdefault('transparent', True)
    params.setdefault('nullPointMode', NULL_CONNECTED)
    params.setdefault('steppedLine', True),

    return Graph(
        title=title,
        description=description,
        targets=targets,
        yAxes=yAxes,
        **params,
    )
