from dataclasses import dataclass
from typing import Optional

import sqlglot.expressions as exp

from .sqlparser import SqlParser
from .errfmt import ErrorFormatter

VT100_UNDERLINE = "\x1b[4m"
VT100_RESET = "\x1b[0m"


@dataclass(frozen=True)
class SubquerySelectContext:
    subquery: exp.Expression


class SubquerySelectChecker:
    def __init__(self, parsed_sql: exp.Expression, sql_parser: SqlParser):
        self.parsed_sql: exp.Expression = parsed_sql
        self.sql_parser: SqlParser = sql_parser
        self.nested_condition_contexts: list[SubquerySelectContext] = []

    def check(self) -> Optional[str]:
        """
        Returns warning message if there no column SELECTed in a subquery is
        not used in that subquery of its own columns, otherwise returns None.
        """
        self._detect_suspicious_nested_conditions()

        if len(self.nested_condition_contexts) == 0:
            return None

        warning_msg = ""

        warning = "No column in subquery SELECT references its tables"
        warning_name = type(self).__name__.rstrip("Checker")


        for i, nested_condition_context in enumerate(self.nested_condition_contexts):
            whole_statement = str(self.parsed_sql)
            subquery = str(nested_condition_context.subquery)
            subquery_start_offset = whole_statement.find(subquery)
            subquery_end_offset = subquery_start_offset + len(subquery)

            underlined_query = (
                whole_statement[:subquery_start_offset]
                + VT100_UNDERLINE
                + subquery
                + VT100_RESET
                + whole_statement[subquery_end_offset : len(whole_statement)]
            )

            formatter = ErrorFormatter(warning, warning_name, underlined_query)
            warning_msg += formatter.format()

            if i != len(self.nested_condition_contexts) - 1:
                warning_msg += "\n"

        return warning_msg

    def _detect_suspicious_nested_conditions(self):
        # exp.In is not SubqueryPredicate for some reason
        subquery_predicates = self.parsed_sql.find_all(exp.SubqueryPredicate)
        in_expressions = self.parsed_sql.find_all(exp.In)
        in_subqueries = [x.args.get("query") for x in in_expressions]
        subqueries = list(subquery_predicates) + in_subqueries

        if subqueries is None:
            return

        # We need to find whether the subquery SELECT uses a tuple variable
        # (e.g. FROM statement) of the subquery.
        for subquery in subqueries:
            all_subquery_columns = self.sql_parser.get_query_columns(subquery)
            all_subquery_column_names = [x.name for x in all_subquery_columns]

            select_column_names = []
            select = subquery.find(exp.Select)
            for select_expression in select.expressions:
                column_exps = select_expression.find_all(exp.Column)
                for column_exp in column_exps:
                    column_name = SqlParser.get_column_name_from_column_expression(
                        column_exp
                    )
                    select_column_names.append(column_name)

            if not any(
                filter(lambda x: x in all_subquery_column_names, select_column_names)
            ):
                context = SubquerySelectContext(subquery)
                self.nested_condition_contexts.append(context)
