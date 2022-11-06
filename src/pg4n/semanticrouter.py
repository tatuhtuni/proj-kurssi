import psycopg
from sqlglot import exp
from typing import Optional

from .sqlparser import SqlParser, Column

# analysis modules
from .cmp_domain_checker import CmpDomainChecker


class SemanticRouter:
    """Analyze given SQL queries via a plethora of analysis modules."""

    def __init__(self,
                 pg_host: str,
                 pg_port: str,
                 pg_user: str,
                 pg_pass: str,
                 pg_name: str
                 ):
        """Initialize Postgres connection with given paramaters."""
        self.pg_host: str = pg_host
        self.pg_port: str = pg_port
        self.pg_user: str = pg_user
        self.pg_pass: str = pg_pass
        self.pg_name: str = pg_name

    def run_analysis(self, sql_query: str) -> str:
        """Run analysis modules on SQL query string and get an insightful \
        message in return.

        Semantic router (some day) implements basic heuristics to avoid
        running all the modules on all queries. For now, it is dumb brute
        force router.
        :param sql_query: is a single well-formed query to run analytics on.
        :returns: an insightful message that might include vt100-compatible \
        control codes. \n is newline (carriage return \r will be added by \
        wrapper).
        """
        with psycopg.connect("host=" + self.pg_host +
                             " port=" + self.pg_port +
                             " dbname=" + self.pg_name +
                             " user=" + self.pg_user +
                             " password=" + self.pg_pass) as conn:
            sql_parser: SqlParser = SqlParser(conn)
            sanitized_sql: exp.Expression = sql_parser.parse_one(sql_query)
            analysis_result: Optional[str] = None

            # Comparing different domains
            columns: list[Column] = sql_parser.get_query_columns(sanitized_sql)
            analysis_result = CmpDomainChecker(sanitized_sql, columns).check()

            if analysis_result is not None:
                return analysis_result
        return ""  # No semantic errors found
