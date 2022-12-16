# Written by Tatu Heikkilä, tatu.heikkila@tuni.fi
# Licensed under MIT.
"""Handle semantic analysis modules."""
from typing import Optional, Type, Any
import psycopg
from sqlglot import exp

from .config_values import ConfigValues
from .sqlparser import SqlParser, Column
from .qepparser import QEPAnalysis, QEPParser

# analysis modules
from .cmp_domain_checker import CmpDomainChecker
from .eq_wildcard_checker import EqWildcardChecker
from .implied_expression_checker import ImpliedExpressionChecker
from .inconsistent_expression_checker import InconsistentExpressionChecker
from .strange_having_checker import StrangeHavingChecker
from .subquery_order_by_checker import SubqueryOrderByChecker
from .subquery_select_checker import SubquerySelectChecker
from .sum_distinct_checker import SumDistinctChecker


class SemanticRouter:
    """Analyze given SQL queries via a plethora of analysis modules."""

    def __init__(
        self,
        pg_host: str,
        pg_port: str,
        pg_user: str,
        pg_pass: str,
        pg_name: str,
        config_values: Optional[ConfigValues]
    ):
        """Initialize Postgres connection with given paramaters."""
        self.pg_host: str = pg_host
        self.pg_port: str = pg_port
        self.pg_user: str = pg_user
        self.pg_pass: str = pg_pass
        self.pg_name: str = pg_name
        self.config_values: Optional[ConfigValues] = config_values

    def run_analysis(
        self,
        sql_query: str
    ) -> str:
        """Run analysis modules on SQL query string and get an insightful \
        message in return.

        Semantic router (some day) implements basic heuristics to avoid
        running all the modules on all queries. For now, it is dumb brute
        force router.
        :param sql_query: is a single well-formed query to run analytics on.
        :returns: an insightful message that might include vt100-compatible \
        control codes and newlines (without carriage returns).
        """
        try:
            with psycopg.connect(
                "host=" + self.pg_host +
                " port=" + self.pg_port +
                " dbname=" + self.pg_name +
                " user=" + self.pg_user +
                " password=" + self.pg_pass
            ) as conn:
                sql_parser: SqlParser = \
                    SqlParser(conn)
                sanitized_sql: exp.Expression = \
                    sql_parser.parse_one(sql_query)
                qep_analysis: QEPAnalysis = \
                    QEPParser(conn=conn).parse(sql_query)
                analysis_result: Optional[str] = \
                    None

                columns: list[Column] = \
                    sql_parser.get_query_columns(sanitized_sql)

                def is_disabled_in_config(checker_class: Type[Any]) -> bool:
                    if self.config_values is None:
                        return False
                    check_name = checker_class.__name__.rstrip("Checker")
                    return self.config_values.get(check_name) is False

                # Comparing different domains
                if not is_disabled_in_config(CmpDomainChecker):
                    analysis_result = CmpDomainChecker(
                        sanitized_sql,
                        columns
                    ).check()

                    if analysis_result is not None:
                        return analysis_result

                # ORDER BY in subquery
                if not is_disabled_in_config(SubqueryOrderByChecker):
                    analysis_result = SubqueryOrderByChecker(
                        sanitized_sql,
                        qep_analysis
                    ).check()

                    if analysis_result is not None:
                        return analysis_result

                # SELECT in subquery
                if not is_disabled_in_config(SubquerySelectChecker):
                    analysis_result = SubquerySelectChecker(
                        sanitized_sql,
                        sql_parser
                    ).check()

                    if analysis_result is not None:
                        return analysis_result

                # Implied expression
                if not is_disabled_in_config(ImpliedExpressionChecker):
                    analysis_result = ImpliedExpressionChecker(
                        sanitized_sql,
                        sql_query,
                        conn
                    ).check()

                    if analysis_result is not None:
                        return analysis_result

                # Strange HAVING clause without GROUP BY
                if not is_disabled_in_config(StrangeHavingChecker):
                    analysis_result = StrangeHavingChecker(
                        sanitized_sql,
                        qep_analysis
                    ).check()

                if analysis_result is not None:
                    return analysis_result

                # SUM/AVG(DISTINCT)
                if not is_disabled_in_config(SumDistinctChecker):
                    analysis_result = SumDistinctChecker(
                        sanitized_sql,
                        qep_analysis
                    ).check()

                    if analysis_result is not None:
                        return analysis_result

                # Wildcards without LIKE
                if not is_disabled_in_config(EqWildcardChecker):
                    analysis_result = EqWildcardChecker(
                        sanitized_sql,
                        qep_analysis
                    ).check()

                    if analysis_result is not None:
                        return analysis_result

                # Inconsistent expression
                if not is_disabled_in_config(InconsistentExpressionChecker):
                    analysis_result = InconsistentExpressionChecker(
                        sanitized_sql,
                        qep_analysis
                    ).check()

                    if analysis_result is not None:
                        return analysis_result

            return ""  # No semantic errors found

        # SQL parser, QEP parser, or an analysis module exploded:
        except Exception:  # Matches only program errors (see flake8 rule E722)
            return ""
