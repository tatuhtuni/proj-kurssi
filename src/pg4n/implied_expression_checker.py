from typing import Optional
import copy

from psycopg import Connection
import sqlglot.expressions as exp
from .sqlparser import (
    SqlParser,
    Column,
)
from .qepparser import (
    QEPParser,
    QEPAnalysis,
    QEPNode,
)
from .errfmt import ErrorFormatter


class ImpliedExpressionChecker:
    def __init__(self, parsed_sql: exp.Expression, sql_statement: str,
                 db_connection: Connection):
        self.parsed_sql: exp.Expression = parsed_sql
        self.sql_statement: str = sql_statement
        self.db_connection: Connection = db_connection

    def check(self) -> Optional[str]:
        """
        Returns warning_msg if implied expression is detected, otherwise None.
        """

        def finder(node: QEPNode) -> bool:
            return node.get("One-Time Filter") != None

        qep_parser_with_constraint_exclusion = \
            QEPParser(conn=self.db_connection, constraint_exclusion=True)
        qep_analysis_with_constraint_exclusion = \
            qep_parser_with_constraint_exclusion.parse(self.sql_statement)
        has_onetime_filter_with_constraint_exclusion = \
            len(qep_analysis_with_constraint_exclusion.root.rfind(finder)) > 0

        qep_parser_without_constraint_exclusion = \
            QEPParser(conn=self.db_connection, constraint_exclusion=False)
        qep_analysis_without_constraint_exclusion = \
            qep_parser_without_constraint_exclusion.parse(self.sql_statement)
        has_onetime_filter_without_constraint_exclusion = \
            len(qep_analysis_without_constraint_exclusion.root.rfind(finder)) > 0

        has_implied_expression = \
            has_onetime_filter_with_constraint_exclusion and \
            not has_onetime_filter_without_constraint_exclusion

        if not has_implied_expression:
            return None

        warning = "Found impossible comparison due to column/table constraints"
        warning_name = type(self).__name__.rstrip("Checker")

        formatter = ErrorFormatter(warning, warning_name)
        warning_msg = formatter.format()

        return warning_msg
