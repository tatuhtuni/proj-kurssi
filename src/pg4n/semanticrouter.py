import psycopg
from typing import Optional

from .sqlparser import SqlParser

# analysis modules
from .cmp_domain_checker import CmpDomainChecker

class SemanticRouter:
    def __init__(self,
                 pg_host: str,
                 pg_port: str,
                 pg_user: str,
                 pg_pass: str,
                 pg_name: str
                 ):
        self.pg_host: str = pg_host
        self.pg_port: str = pg_port
        self.pg_user: str = pg_user
        self.pg_pass: str = pg_pass
        self.pg_name: str = pg_name

    def run_analysis(self, sql_query: str) -> str:
        with psycopg.connect("host=" + self.pg_host +
                             " port=" + self.pg_port +
                             " dbname=" + self.pg_name +
                             " user=" + self.pg_user +
                             " password=" + self.pg_pass) as conn:
            sql_parser = SqlParser(conn)
            sanitized_sql = sql_parser.parse_one(sql_query)
            analysis_result: Optional[str] = None

            # Comparing different domains
            columns = sql_parser.get_query_columns(sanitized_sql)
            analysis_result = CmpDomainChecker(sanitized_sql, columns).check()

            if analysis_result is not None:
                return "" # str(analysis_result)
        return "none found"  # No semantic errors found
