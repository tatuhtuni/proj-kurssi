from typing import Optional

import sqlglot.expressions as exp

from .qepparser import QEPAnalysis
from .errfmt import ErrorFormatter


class EqWildcardChecker:
    def __init__(self, parsed_sql: exp.Expression, qep_analysis: QEPAnalysis):
        self.parsed_sql = parsed_sql
        self.qep_analysis = qep_analysis

    def check(self) -> Optional[str]:
        """
        Returns warning message if the sql has equals operation to a string with
        wild card character (the '%' character), otherwise None.
        """

        eqs = self.parsed_sql.find_all(exp.EQ)

        def is_wildcard_string_eq(eq):
            return self._is_wildcard_string_literal(
                eq.left
            ) or self._is_wildcard_string_literal(eq.right)

        has_eq_wildcard = any(filter(is_wildcard_string_eq, eqs))

        if not has_eq_wildcard:
            return None

        warning = "Possible use of '=' instead of LIKE for wildcard pattern"
        warning_name = type(self).__name__.rstrip("Checker")

        formatter = ErrorFormatter(warning, warning_name)
        warning_msg = formatter.format()

        return warning_msg

    def _is_wildcard_string_literal(self, operand: exp.Expression) -> bool:
        if type(operand) == exp.Literal and operand.is_string:
            return operand.this.find("%") != -1
        return False
