import sys
import subprocess
import sqlglot
from pprint import pprint

from pg4n import sqlparser


def has_subquery_order_by(sql: sqlglot.exp.Expression, qep: str) -> bool:
    """
    Does not use the qep so far.
    """

    # Gets all the non-root select statements
    subqueries = \
        filter(lambda x: x.parent != None, sql.find_all(sqlglot.exp.Select))

    return any([subquery.find(sqlglot.exp.Order) for subquery in subqueries])


STATEMENT_WITHOUT_OUTER_ORDER = \
    f"""
SELECT *
FROM customers c
WHERE EXISTS (
    SELECT *
    FROM orders o
    WHERE c.customer_id = o.customer_id
    ORDER BY o.customer_id
    );
"""

STATEMENT_OUTER_ORDER_WITHOUT_INNER_ORDER = \
    f"""
SELECT *
FROM customers c
WHERE EXISTS (
    SELECT *
    FROM orders o
    WHERE c.customer_id = o.customer_id
    )
ORDER BY c.customer_id;
"""

STATEMENT_OUTER_ORDER_WITH_INNER_ORDER = \
    f"""
SELECT *
FROM customers c
WHERE EXISTS (
    SELECT *
    FROM orders o
    WHERE c.customer_id = o.customer_id
    ORDER BY o.customer_id
    )
ORDER BY c.customer_id;
"""

STATEMENT_MULTIPLE_INNER_ORDERS = \
    f"""
SELECT *
FROM customers c
WHERE EXISTS (
    SELECT *
    FROM orders o
    WHERE c.customer_id = o.customer_id
    ORDER BY o.customer_id
    ) OR EXISTS (
    SELECT *
    FROM orders o
    WHERE c.customer_id = o.customer_id
    ORDER BY o.customer_id
    );
"""

STATEMENT_NESTED_SUBQUERY_INNER_ORDER = \
    f"""
SELECT *
FROM customers c
WHERE EXISTS (
    SELECT *
    FROM (SELECT *
          FROM orders o2
          ORDER BY o2.customer_id) AS o
    WHERE c.customer_id = o.customer_id
    );
"""


def _test(expected: bool, sql_statement: str, db_name: str):
    qep = subprocess.check_output(["psql", "-X", "-d", db_name, "-c",
                                   "EXPLAIN ANALYZE " + sql_statement])
    qep = bytes.decode(qep)

    parsed_sql = sqlparser.parse(sql_statement)
    result = has_subquery_order_by(parsed_sql, qep)
    print(f"expected: {expected}, result: {str(result)}")
    if expected != result:
        print(parsed_sql)


USAGE = "usage: orderby.py <db_name>"


def main():
    if len(sys.argv) != 2:
        print(USAGE, file=sys.stderr)
        exit(1)

    db_name = str(sys.argv[1])

    _test(True, STATEMENT_WITHOUT_OUTER_ORDER, db_name)
    _test(False, STATEMENT_OUTER_ORDER_WITHOUT_INNER_ORDER, db_name)
    _test(True, STATEMENT_OUTER_ORDER_WITH_INNER_ORDER, db_name)
    _test(True, STATEMENT_MULTIPLE_INNER_ORDERS, db_name)
    _test(True, STATEMENT_NESTED_SUBQUERY_INNER_ORDER, db_name)


if __name__ == "__main__":
    main()
