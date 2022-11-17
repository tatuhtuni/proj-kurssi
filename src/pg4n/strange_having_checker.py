from typing import Optional

import sqlglot
import sqlglot.expressions as exp

from .qepparser import QEPAnalysis

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

        warning_msg = "Warning: HAVING without GROUP BY [pg4n::StrangeHaving]"

        return warning_msg


def _is_empty(generator) -> bool:
    return not not generator
