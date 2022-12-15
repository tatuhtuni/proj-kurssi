from typing import Optional

import sqlglot.expressions as exp

from .errfmt import ErrorFormatter
from .qepparser import QEPAnalysis, QEPNode


class InconsistentExpressionChecker:
    """
    Inconsistent expression is some expression that is never true.
    For example: x = 10 AND x = 20

    This checker only finds a small subset of such expression, where postgresql
    itself detects the inconsistent expression in its query optimizer and
    exposes that information via its query execution plan.

    """

    # TODO: Analyze the where expressions of the sql and find more inconsistent
    #       expressions.
    #       - One approach is to transform the expressions with its symbols from
    #         sqlparser representation into a formula and finding whether it is
    #         satisfiable with pysmt.
    #       - This approach could also be used to find tautologies.
    #       - Some preliminary work of this is in the experimental branch
    #         feat/experimental-smt

    def __init__(self, parsed_sql: exp.Expression, qep_analysis: QEPAnalysis):
        self.parsed_sql: exp.Expression = parsed_sql
        self.qep_analysis: QEPAnalysis = qep_analysis

    def check(self) -> Optional[str]:
        """
        Returns warning msg if the sql contains inconsistent expression,
        otherwise None.
        """

        if self.qep_analysis is None:
            return None

        def finder(node: QEPNode) -> bool:
            return node.get("One-Time Filter") is not None

        has_onetime_filter = len(self.qep_analysis.root.rfind(finder)) > 0
        has_inconsistent_expression = has_onetime_filter

        if not has_inconsistent_expression:
            return None

        warning_name = type(self).__name__.rstrip("Checker")
        warning = "Some condition is always false"
        formatter = ErrorFormatter(warning, warning_name)
        warning_msg = formatter.format()

        return warning_msg
