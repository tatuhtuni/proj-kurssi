from typing import Optional

import sqlglot.expressions as exp

from .qepparser import QEPAnalysis
from .errfmt import ErrorFormatter


class SumDistinctChecker:
    def __init__(self, parsed_sql: exp.Expression, qep_analysis: QEPAnalysis):
        self.parsed_sql = parsed_sql
        self.qep_analysis = qep_analysis

    def check(self) -> bool:
        """
        Returns warning message if the sql has SUM/AVG(DISTINCT ...),
        otherwise None
        """

        sums = self.parsed_sql.find_all(exp.Sum)
        avgs = self.parsed_sql.find_all(exp.Avg)

        def has_distinct(x):
            return type(x.this) == exp.Distinct

        has_sum_distinct = any(filter(has_distinct, sums))
        has_avg_distinct = any(filter(has_distinct, avgs))

        if not (has_sum_distinct or has_avg_distinct):
            return None

        warning = "DISTINCT in SUM or AVG"
        warning_name = type(self).__name__.rstrip("Checker")
        formatter = ErrorFormatter(warning, warning_name)
        warning_msg = formatter.format()

        return warning_msg
