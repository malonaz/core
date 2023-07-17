import unittest

import common.python.grafana.query as query


class FormatParamsTest(unittest.TestCase):
    def test_empty_params_including_default(self):
        expected = '{kubernetes_cluster="$cluster", kubernetes_namespace="$namespace"}'
        self.assertEqual(expected, query.format_params({}))

    def test_empty_params_excluding_default(self):
        self.assertEqual('{}', query.format_params({}, exclude_default=True))

    def test_single_param_including_default(self):
        expected = '{a="b", kubernetes_cluster="$cluster", kubernetes_namespace="$namespace"}'
        self.assertEqual(expected, query.format_params({"a": "b"}))

    def test_single_param_excluding_default(self):
        expected = '{a="b"}'
        self.assertEqual(expected, query.format_params({"a": "b"}, exclude_default=True))

    def test_two_param_excluding_default(self):
        expected = '{a="b", c="d"}'
        self.assertEqual(expected, query.format_params({"a": "b", "c": "d"}, exclude_default=True))

    def test_single_regex_param_excluding_default(self):
        expected = '{a=~"b"}'
        self.assertEqual(expected, query.format_params({"a": "~b"}, exclude_default=True))

    def test_not_equal_param_excluding_default(self):
        expected = '{a!="b"}'
        self.assertEqual(expected, query.format_params({"a": "!b"}, exclude_default=True))

    def test_not_equal_regex_param_excluding_default(self):
        expected = '{a!~"b"}'
        self.assertEqual(expected, query.format_params({"a": "!~b"}, exclude_default=True))

    def test_mix_regex_and_non_regex_params_excluding_default(self):
        expected = '{a=~"b", c="d"}'
        self.assertEqual(expected, query.format_params({"a": "~b", "c": "d"}, exclude_default=True))


class InstantVectorTest(unittest.TestCase):
    def test_str(self):
        expected = 'test_total{kubernetes_cluster="$cluster", kubernetes_namespace="$namespace"}'
        actual = query.InstantVector(
            metric='test_total',
            params=query.format_params({}),
        )
        self.assertEqual(expected, str(actual))


class RangeVectorTest(unittest.TestCase):
    def test_str(self):
        actual = query.RangeVector(
            metric='test_total',
            params=query.format_params({}),
        )
        self.assertEqual(
            'test_total{kubernetes_cluster="$cluster", kubernetes_namespace="$namespace"}[2m]',
            str(actual),
        )

    def test_str_custom_lookback_window(self):
        actual = query.RangeVector(
            metric='test_total',
            params=query.format_params({}),
            lookback_window='5d',
        )
        self.assertEqual(
            'test_total{kubernetes_cluster="$cluster", kubernetes_namespace="$namespace"}[5d]',
            str(actual),
        )


class HistogramQuantileTest(unittest.TestCase):
    def test_str(self):
        expected = 'histogram_quantile(0.2, test_total{a="b"})'
        actual = query.HistogramQuantile(
            quantile=0.2,
            vector=query.InstantVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            )
        )
        self.assertEqual(expected, str(actual))

    def test_invalid_quantile(self):
        with self.assertRaises(ValueError) as context:
            query.HistogramQuantile(
                quantile=2,
                vector=query.InstantVector(
                    metric='test_total',
                    params=query.format_params({"a": "b"}, exclude_default=True),
                ),
            )
        self.assertEqual(str(context.exception), "quantile should be between 0 and 1")

    def test_invalid_vector_type(self):
        with self.assertRaises(TypeError) as context:
            query.HistogramQuantile(
                quantile=0.5,
                vector=query.RangeVector(
                    metric='test_total',
                    params=query.format_params({"a": "b"}, exclude_default=True),
                ),
            )
        self.assertEqual(str(context.exception), "invalid vector type")


class RateTest(unittest.TestCase):
    def test_str(self):
        expected = 'rate(test_total{a="b"}[2m])'
        actual = query.Rate(
            vector=query.RangeVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            )
        )
        self.assertEqual(expected, str(actual))
        self.assertIsInstance(actual, query.InstantVector)

    def test_custom_rate_function(self):
        expected = 'irate(test_total{a="b"}[2m])'
        actual = query.Rate(
            vector=query.RangeVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            ),
            rate_function='irate',
        )
        self.assertEqual(expected, str(actual))
        self.assertIsInstance(actual, query.InstantVector)

    def test_invalid_vector_type(self):
        with self.assertRaises(TypeError) as context:
            query.Rate(
                vector=query.InstantVector(
                    metric='test_total',
                    params=query.format_params({"a": "b"}, exclude_default=True),
                ),
            )
        self.assertEqual(str(context.exception), "invalid vector type")

    def test_invalid_function(self):
        with self.assertRaises(ValueError) as context:
            query.Rate(
                vector=query.RangeVector(
                    metric='test_total',
                    params=query.format_params({"a": "b"}, exclude_default=True),
                ),
                rate_function="hello",
            )
        self.assertEqual(
            str(context.exception),
            "Rate function should be one of: ['rate', 'irate']"
        )


class IncreaseTest(unittest.TestCase):
    def test_str(self):
        expected = 'increase(test_total{a="b"}[2m])'
        actual = query.Increase(
            vector=query.RangeVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            )
        )
        self.assertEqual(expected, str(actual))
        self.assertIsInstance(actual, query.InstantVector)

    def test_invalid_vector_type(self):
        with self.assertRaises(TypeError) as context:
            query.Increase(
                vector=query.InstantVector(
                    metric='test_total',
                    params=query.format_params({"a": "b"}, exclude_default=True),
                ),
            )
        self.assertEqual(str(context.exception), "invalid vector type")


class DeltaTest(unittest.TestCase):
    def test_str(self):
        expected = 'delta(test_total{a="b"}[2m])'
        actual = query.Delta(
            vector=query.RangeVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            )
        )
        self.assertEqual(expected, str(actual))
        self.assertIsInstance(actual, query.InstantVector)

    def test_invalid_vector_type(self):
        with self.assertRaises(TypeError) as context:
            query.Increase(
                vector=query.InstantVector(
                    metric='test_total',
                    params=query.format_params({"a": "b"}, exclude_default=True),
                ),
            )
        self.assertEqual(str(context.exception), "invalid vector type")


class AggregationTest(unittest.TestCase):
    def test_invalid_vector_type(self):
        with self.assertRaises(TypeError) as context:
            query._Aggregation(
                operator="max",
                vector=query.RangeVector(
                    metric='test_total',
                    params=query.format_params({"a": "b"}, exclude_default=True),
                ),
            )
        self.assertEqual(str(context.exception), "invalid vector type")

    def test_both_by_and_without(self):
        with self.assertRaises(ValueError) as context:
            query._Aggregation(
                operator="max",
                vector=query.InstantVector(
                    metric='test_total',
                    params=query.format_params({"a": "b"}, exclude_default=True),
                ),
                by=['hello'],
                without=['world'],
            )
        self.assertEqual(str(context.exception), "clause should be by or without, not both")

    def test_invalid_operator(self):
        with self.assertRaises(ValueError) as context:
            query._Aggregation(
                operator="invalid",
                vector=query.InstantVector(
                    metric='test_total',
                    params=query.format_params({"a": "b"}, exclude_default=True),
                ),
            )
        error = f"operator should be one of: {query._Aggregation.VALID_OPERATORS}"
        self.assertEqual(str(context.exception), error)

    def test_single_by_dimension(self):
        expected = 'sum(test_total{a="b"}) by (hi)'
        actual = query._Aggregation(
            operator='sum',
            vector=query.InstantVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            ),
            by=['hi'],
        )
        self.assertEqual(expected, str(actual))

    def test_multiple_by_dimension(self):
        expected = 'sum(test_total{a="b"}) by (hi, hello)'
        actual = query._Aggregation(
            operator='sum',
            vector=query.InstantVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            ),
            by=['hi', 'hello'],
        )
        self.assertEqual(expected, str(actual))

    def test_single_without_dimension(self):
        expected = 'sum(test_total{a="b"}) without (hi)'
        actual = query._Aggregation(
            operator='sum',
            vector=query.InstantVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            ),
            without=['hi'],
        )
        self.assertEqual(expected, str(actual))

    def test_multiple_without_dimension(self):
        expected = 'sum(test_total{a="b"}) without (hi, hello)'
        actual = query._Aggregation(
            operator='sum',
            vector=query.InstantVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            ),
            without=['hi', 'hello'],
        )
        self.assertEqual(expected, str(actual))


class SumTest(unittest.TestCase):
    def test_simple_vector(self):
        expected = 'sum(test_total{a="b"})'
        actual = query.Sum(
            vector=query.InstantVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            ),
        )
        self.assertEqual(expected, str(actual))


class MaxTest(unittest.TestCase):
    def test_simple_vector(self):
        expected = 'max(test_total{a="b"})'
        actual = query.Max(
            vector=query.InstantVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            ),
        )
        self.assertEqual(expected, str(actual))


class MinTest(unittest.TestCase):
    def test_simple_vector(self):
        expected = 'min(test_total{a="b"})'
        actual = query.Min(
            vector=query.InstantVector(
                metric='test_total',
                params=query.format_params({"a": "b"}, exclude_default=True),
            ),
        )
        self.assertEqual(expected, str(actual))
