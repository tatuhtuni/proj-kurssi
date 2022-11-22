import re
from dataclasses import dataclass
from typing import Optional

import sqlglot.expressions as exp

from .sqlparser import SqlParser
from .sqlparser import Column
from .errfmt import ErrorFormatter


VT100_UNDERLINE = "\x1b[4m"
VT100_RESET = "\x1b[0m"


@dataclass(frozen=True)
class CmpContext:
    expression: exp.Predicate
    a: Column
    b: Column


class CmpDomainChecker:
    def __init__(self, parsed_sql: exp.Expression, columns: list[Column]):
        self.parsed_sql: str = parsed_sql
        self.columns: list[Column] = columns
        self.suspicious_cmp_contexts: list[CmpContext] = []
        self.warning_msg: Optional[str] = None

    def check(self) -> Optional[str]:
        """
        Does analysis for suspicous comparisons between different domains.
        e.g., comparing columns off type VARCHAR(20) and VARCHAR(50)
        Returns a warning message if something was found, otherwise None.
        """

        self._detect_suspicious_cmps(self.parsed_sql, self.columns)
        return self.warning_msg

    def _are_from_compatible_domains(self, a: Column, b: Column) -> bool:
        """
        Tests whether the both types a and b are representable by their
        datatype.
        """

        # First we must extract the base type without possible trailing
        # precission or digit counts (which can be retrieved from the
        # Column.type directly).
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

        if a.type.digits is None and b.type.digits is None:
            return True
        elif ((a.type.digits is None and b.type.digits is not None) or
              (a.type.digits is not None and b.type.digits is None)):
            return False

        if a.type.digits != b.type.digits:
            return False
        else:
            if a.type.precision is None and b.type.precision is None:
                return True
            elif ((a.type.precision is None and
                   b.type.precision is not None) or
                  (a.type.precision is not None and
                   b.type.precision is None)):
                return False

        return True

    def _detect_suspicious_cmp(self, cmp: exp.Predicate,
                               columns: list[Column]):
        """
        Detects whether 'cmp' has comparison between columns from different
        domains (e.g. a: VARCHAR(10) < b: VARCHAR(50)).

        Only works for comparisons between 2 variables. In other words, if
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
            if tmp is not None:
                cmp_columns.append(tmp)

        # This the comparisons has atleast 1 literal
        if len(cmp_columns) < 2:
            return False

        if not self._are_from_compatible_domains(cmp_columns[0],
                                                 cmp_columns[1]):
            cmp_context = CmpContext(cmp, cmp_columns[0], cmp_columns[1])
            self.suspicious_cmp_contexts.append(cmp_context)

    def _detect_suspicious_cmps(self, select_statement: exp.Select,
                                columns: list[Column]):
        predicates = SqlParser.find_where_predicates(select_statement)

        # This filters predicates we are not interested in such as IN or EXISTS
        binary_predicates = list(
            filter(lambda x: isinstance(x, exp.Binary), predicates))

        for binary_predicate in binary_predicates:
            self._detect_suspicious_cmp(binary_predicate, columns)

        if len(self.suspicious_cmp_contexts) == 0:
            return

        for i, suspicious_cmp_context in \
                enumerate(self.suspicious_cmp_contexts):
            if self.warning_msg is None:
                self.warning_msg = ""
            whole_statement = str(select_statement)

            domain1 = suspicious_cmp_context.a.type.name
            domain2 = suspicious_cmp_context.b.type.name
            warning = f"Comparison between different domains ({domain1}, {domain2})"
            # TODO: Develop more principled way of matching warning_name with
            #       the options expected in the configuration files.
            warning_name = type(self).__name__.rstrip("Checker")

            cmp_exp = suspicious_cmp_context.expression

            # It does not matter which column's ancestor Where expression we
            # find because both necessarily have the same.
            containing_where = str(cmp_exp.find_ancestor(exp.Where))
            containing_where_start_offset = \
                whole_statement.find(containing_where)

            cmp_start_offset = containing_where.find(str(cmp_exp))
            cmp_end_offset = cmp_start_offset + len(str(cmp_exp))

            total_start_offset = \
                containing_where_start_offset + cmp_start_offset
            total_end_offset = containing_where_start_offset + cmp_end_offset

            underlined_query = \
                whole_statement[:total_start_offset] + \
                VT100_UNDERLINE + \
                whole_statement[total_start_offset:total_end_offset] + \
                VT100_RESET + \
                whole_statement[total_end_offset:len(whole_statement)]

            formatter = ErrorFormatter(warning, warning_name, underlined_query)
            self.warning_msg = formatter.format()
            if i != len(self.suspicious_cmp_contexts) - 1:
                self.warning_msg += '\n'


def _find_column(column_name: str, columns: list[Column]) -> Optional[Column]:
    """
    Returns a column matching column name.
    """
    # TODO: Just use dict inside an object

    for column in columns:
        if column_name == column.name:
            return column

    return None
