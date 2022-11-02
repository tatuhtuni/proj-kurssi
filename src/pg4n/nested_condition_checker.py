import sqlglot
import sqlglot.expressions as exp

from .sqlparser import (
        Column,
        SqlParser,
)


class NestedConditionChecker:
    def __init__(self, parsed_sql: exp.Expression):
        self.parsed_sql = parsed_sql

    def check(self) -> bool:
        """
        Returns True if there is something worth in the query, otherwise False.
        """

        return not self._has_suspicious_nested_condition()

    def _has_suspicious_nested_condition(self) -> bool:
        # exp.In is not SubqueryPredicate for some reason
        subquery_expressions = self.parsed_sql.find_all(exp.In, exp.SubqueryPredicate)

        if subquery_expressions == None:
            return False

        subquery_columns = []
        for subquery_expression in subquery_expressions:
            column_expressions = subquery_expression.find_all(exp.Column)
            for column_expression in column_expressions:
                column = \
                    SqlParser.get_column_name_from_column_expression(column_expression)
                subquery_columns.append(column)

        def subquery_remover(node):
            if isinstance(node, (exp.In, exp.SubqueryPredicate)):
                return None
            return node

        statement_without_subqueries = \
                self.parsed_sql.transform(subquery_remover, copy=True)

        toplevel_columns = []
        toplevel_column_expressions = statement_without_subqueries.find_all(exp.Column)
        for column_expression in toplevel_column_expressions:
            column = \
                SqlParser.get_column_name_from_column_expression(column_expression)
            toplevel_columns.append(column)

        unique_toplevel_columns = list(set(toplevel_columns))

        return any(filter(lambda x: x not in unique_toplevel_columns, subquery_columns))
