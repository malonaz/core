"""
Lib to build prometheus query expression
https://prometheus.io/docs/prometheus/latest/querying/basics/#expression-language-data-types
"""
from typing import List, Optional, Dict

def format_params(params: Dict[str, str]):
    """
    A helper function mapping a string->string map to a grafana compliant parameter
    expression.
    i.e. {'a': '1', 'b': '2'} will return the string '{a="1",b="2"}`
    @params: the params you wish to create. You may prefix a param with `~` to use regex.
    """
    expression_list = []
    for k, v in params.items():
        if v.startswith("~"):
            expression_list.append(f'{k}=~"{v[1:]}"')
        elif v.startswith("!~"):
            expression_list.append(f'{k}!~"{v[2:]}"')
        elif v.startswith("!"):
            expression_list.append(f'{k}!="{v[1:]}"')
        else:
            expression_list.append(f'{k}="{v}"')
    return '{' + ', '.join(expression_list) + '}'


class InstantVector:
    """https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors"""
    def __init__(self, metric: str, params: str = ''):
        self.metric = metric
        self.params = params

    def __str__(self):
        return f"{self.metric}{self.params}"


class RangeVector:
    """https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors"""
    def __init__(self, metric: str, params: str = '', lookback_window: str = '2m'):
        self.metric = metric
        self.params = params
        self.lookback_window = lookback_window

    def __str__(self):
        return f"{self.metric}{self.params}[{self.lookback_window}]"


class HistogramQuantile(InstantVector):
    """https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile"""
    def __init__(self, quantile: float, vector: InstantVector):
        if (quantile < 0 or quantile > 1):
            raise ValueError("quantile should be between 0 and 1")
        if not isinstance(vector, InstantVector):
            raise TypeError("invalid vector type")
        self.quantile = quantile
        self.vector = vector

    def __str__(self):
        return f"histogram_quantile({self.quantile}, {self.vector})"


class Rate(InstantVector):
    """https://prometheus.io/docs/prometheus/latest/querying/functions/#rate"""
    VALID_FUNCTIONS = ['rate', 'irate']

    def __init__(self, vector: RangeVector, rate_function: str = 'rate'):
        if not isinstance(vector, RangeVector):
            raise TypeError("invalid vector type")
        if rate_function not in Rate.VALID_FUNCTIONS:
            raise ValueError(f"Rate function should be one of: {Rate.VALID_FUNCTIONS}")
        self.rate_function = rate_function
        self.vector = vector

    def __str__(self):
        return f"{self.rate_function}({self.vector})"


class Increase(InstantVector):
    """https://prometheus.io/docs/prometheus/latest/querying/functions/#increase"""
    def __init__(self, vector: RangeVector):
        if not isinstance(vector, RangeVector):
            raise TypeError("invalid vector type")
        self.vector = vector

    def __str__(self):
        return f"increase({self.vector})"


class Delta(InstantVector):
    """
    https://prometheus.io/docs/prometheus/latest/querying/functions/#delta
    Delta should only be used with gauges.
    """
    def __init__(self, vector: RangeVector):
        if not isinstance(vector, RangeVector):
            raise TypeError("invalid vector type")
        self.vector = vector

    def __str__(self):
        return f"delta({self.vector})"

class Scalar(InstantVector):
    """https://prometheus.io/docs/prometheus/latest/querying/functions/#scalar"""
    def __init__(self, vector: InstantVector):
        if not isinstance(vector, InstantVector):
            raise TypeError("invalid vector type")
        self.vector = vector

    def __str__(self):
        return f"scalar({self.vector})"

class _Aggregation(InstantVector):
    """https://prometheus.io/docs/prometheus/latest/querying/operators/#aggregation-operators"""
    VALID_OPERATORS = ['sum', 'min', 'max', 'avg', 'count']

    def __init__(
        self,
        operator: str,
        vector: InstantVector,
        by: Optional[List[str]] = None,
        without: Optional[List[str]] = None,
    ):
        if not isinstance(vector, InstantVector):
            raise TypeError("invalid vector type")
        if by is not None and without is not None:
            raise ValueError("clause should be by or without, not both")
        if operator not in _Aggregation.VALID_OPERATORS:
            raise ValueError(f"operator should be one of: {_Aggregation.VALID_OPERATORS}")
        self.dimension = ''
        if by:
            self.dimension = " by ({})".format(', '.join(by))
        if without:
            self.dimension = " without ({})".format(', '.join(without))
        self.vector = vector
        self.operator = operator

    def __str__(self):
        return f"{self.operator}({self.vector}){self.dimension}"


class Count(_Aggregation):
    """https://prometheus.io/docs/prometheus/latest/querying/operators/#aggregation-operators"""
    def __init__(
        self,
        vector: InstantVector,
        by: Optional[List[str]] = None,
        without: Optional[List[str]] = None,
    ):
        super().__init__('count', vector, by, without)


class Sum(_Aggregation):
    """https://prometheus.io/docs/prometheus/latest/querying/operators/#aggregation-operators"""
    def __init__(
        self,
        vector: InstantVector,
        by: Optional[List[str]] = None,
        without: Optional[List[str]] = None,
    ):
        super().__init__('sum', vector, by, without)


class Max(_Aggregation):
    """https://prometheus.io/docs/prometheus/latest/querying/operators/#aggregation-operators"""
    def __init__(
        self,
        vector: InstantVector,
        by: Optional[List[str]] = None,
        without: Optional[List[str]] = None,
    ):
        super().__init__('max', vector, by, without)


class Min(_Aggregation):
    """https://prometheus.io/docs/prometheus/latest/querying/operators/#aggregation-operators"""
    def __init__(
        self,
        vector: InstantVector,
        by: Optional[List[str]] = None,
        without: Optional[List[str]] = None,
    ):
        super().__init__('min', vector, by, without)


class Avg(_Aggregation):
    """https://prometheus.io/docs/prometheus/latest/querying/operators/#aggregation-operators"""
    def __init__(
        self,
        vector: InstantVector,
        by: Optional[List[str]] = None,
        without: Optional[List[str]] = None,
    ):
        super().__init__('avg', vector, by, without)
