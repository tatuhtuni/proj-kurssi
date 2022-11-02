import subprocess
import sys
import re
from dataclasses import dataclass
from typing import Optional

import sqlglot.expressions as exp

from . import sqlparser
from .sqlparser import SqlParser
from .sqlparser import Column


class CmpDomainChecker:
    def __init__(self, parsed_sql: exp.Expression, columns: list[Column]):
        self.parsed_sql = parsed_sql
        self.columns = columns

    def check(self) -> bool:
        """
        Returns True if no 'bad' comparisons are done, False otherwise.
        """
        return not self._has_suspicious_comparison(self.parsed_sql, self.columns)

    def _are_from_compatible_domains(self, a: Column, b: Column) -> bool:
        """
        Tests whether the both types a and b are representable by their datatype
        """

        # First we must extract the base type without possible trailing precission or digit
        # counts (which can be retrieved from the Column.type directly).
        type_start_matcher = re.compile(r'^([a-zA-Z]+).*$')
        match_a = type_start_matcher.match(a.type.name)
        match_b = type_start_matcher.match(b.type.name)

        if not (match_a and match_b):
            return True
        elif len(match_a.groups()) != 1 or len(match_b.groups()) != 1:
            return True

        type_start_a = match_a.group(1)
        type_start_b = match_b.group(1)

        # We don't evaluate types of different names
        # TODO: Consider evaluating similar types e.g. VARCHAR and CHAR
        if type_start_a != type_start_b:
            return True

        if a.type.digits == None and b.type.digits == None:
            return True
        elif ((a.type.digits == None and b.type.digits != None) or
              (a.type.digits != None and b.type.digits == None)):
            return False

        if a.type.digits != b.type.digits:
            return False
        else:
            if a.type.precision == None and b.type.precision == None:
                return True
            elif ((a.type.precision == None and b.type.precision != None) or
                  (a.type.precision != None and b.type.precision == None)):
                return False

        return True

    def _is_suspicious_comparison(self, cmp: exp.Predicate,
                                  columns: list[Column]) -> bool:
        """
        Finds wheter 'cmp' has comparison between columns from different domains
        e.g. a: VARCHAR(10) < b: VARCHAR(50)

        Only works for comparasons between 2 variables. In other words, if
        any operand in the comparison is literal this function returns False.

        Ignores any casts used for the operands.
        """

        # TODO: Take casts into account.
        # TODO: Consider comparisons with literals as well.
        # TODO: Investigate if postgresql already errors/warns of these kind of
        #       comparisons
        # FIXME: Stop assuming columns with the same name coming from possibly
        #        different tables have the same datatype
        column_expressions = cmp.find_all(exp.Column)

        cmp_column_names = []
        cmp_columns = []

        for column_expression in column_expressions:
            cmp_column_name = \
                SqlParser.get_column_name_from_column_expression(
                    column_expression)
            cmp_column_names.append(cmp_column_name)

        for cmp_column_name in cmp_column_names:
            tmp = _find_column(cmp_column_name, columns)
            if tmp != None:
                cmp_columns.append(tmp)

        # This the comparisons has atleast 1 literal
        if len(cmp_columns) < 2:
            return False

        return not self._are_from_compatible_domains(cmp_columns[0], cmp_columns[1])

    def _has_suspicious_comparison(self, select_statement: exp.Select,
                                   columns: list[Column]) -> bool:
        predicates = SqlParser.find_where_predicates(select_statement)

        # This filters predicates we are not interested in such as IN or EXISTS
        binary_predicates = list(
            filter(lambda x: isinstance(x, exp.Binary), predicates))

        # for cmp in binary_predicates:
        #     print("'%s' is suspicous?: %s" % (str(cmp), self._is_suspicious_comparison(cmp, columns)))

        return any(filter(lambda x: self._is_suspicious_comparison(x, columns),
                          binary_predicates))


def _find_column(column_name: str, columns: list[Column]) -> Optional[Column]:
    """
    Returns a column matching column name.
    """
    # TODO: Just use dict inside an object

    for column in columns:
        if column_name == column.name:
            return column

    return None
