from typing import Optional

import sqlglot.expressions as exp

from .qepparser import QEPAnalysis
from .errfmt import ErrorFormatter


class StrangeHavingChecker:
    def __init__(self, parsed_sql: exp.Expression, qep_analysis: QEPAnalysis):
        self.parsed_sql: exp.Expression = parsed_sql
        self.qep_analysis: QEPAnalysis = qep_analysis

    def check(self) -> Optional[str]:
        """
        Returns warning message if there exists HAVING without a GROUP BY,
        otherwise None.
        """

        has_group_by = len(list(self.parsed_sql.find_all(exp.Group))) != 0
        has_having = len(list(self.parsed_sql.find_all(exp.Having))) != 0
        has_strange_having = has_having and not has_group_by

        if not has_strange_having:
            return None

        warning = "HAVING without GROUP BY"
        warning_name = type(self).__name__.rstrip("Checker")
        formatter = ErrorFormatter(warning, warning_name)
        warning_msg = formatter.format()

        return warning_msg
