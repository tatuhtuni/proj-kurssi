import sys
import sqlglot
from pprint import pprint

def parse(sql: str) -> sqlglot.exp.Expression:
    return sqlglot.parse_one(sql)

USAGE = "usage: sql_parser.py"

def main():
    if len(sys.argv) != 1:
        print(USAGE, file=sys.stderr)
        exit(1)

    TABLE_NAME = "orders"
    IMPOSSIBLE_STATEMENT = \
        f"SELECT * FROM {TABLE_NAME} " \
         "WHERE order_total_eur = 0 AND order_total_eur = 100;"

    statement = IMPOSSIBLE_STATEMENT
    sql_expression = parse(statement)
    pprint(sql_expression)
    print(80 * '=')

    def get_where_expression(sql: str) -> str:
        parsed = sqlglot.parse_one(sql)
        where_statement = parsed.find(sqlglot.exp.Where)
        return where_statement
    where_expression = get_where_expression(statement)
    pprint(where_expression)

if __name__ == "__main__":
    main()
