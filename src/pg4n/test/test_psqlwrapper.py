"""
Test PsqlWrapper, and secondarily PsqlParser.
"""

from ..psqlwrapper import PsqlWrapper
from ..psqlparser import PsqlParser

from shutil import get_terminal_size


def new_psqlwrapper() -> PsqlWrapper:
    psql = PsqlWrapper("",
                       lambda x: "Test",
                       lambda x: "",
                       PsqlParser())

    case_psql_start = \
        b'psql (14.5)\r\nType "help" for help.\r\n\r\n\x1b[?2004hpgdb=# '
    psql._intercept(case_psql_start)
    return psql

def test_intercept() -> None:
    psql: PsqlWrapper = new_psqlwrapper()

    fresh_prompt_1 = \
        b'\x1b[?2004hpgdb=# '
    return_press_1 = \
        b'\r\n'
    return_press_2 = \
        b'\r\n\x1b[?2004l\r'
    return_press_3 = \
        b'\x08\r\n'

    # simple query with single arrow-up to query
    case_query_1 = \
        b'SELECT * FROM orders WHERE order_total_eur = 0 AND order_total_eur = 100;'
    psql._intercept(case_query_1)
    psql._intercept(return_press_1)
    assert psql.pg4n_message == "Test"
    
    assert psql._intercept(fresh_prompt_1) == \
        b'\r\n' + b'Test' + b'\r\n\r\n' + b'\x1b[?2004hpgdb=# '

    psql = new_psqlwrapper()

    # Something weird going these test case. Pyte understands this case right
    # in live scenarios most of the time, but I cannot reproduce it with this
    # saved stream of characters. Most likely the control streams depend
    # on terminal size, and cannot be just copy-pasted from live tests.
    
    # ctrl-R to previous query with three key presses searching (1..0..0). Press return.
    case_query_2 = \
        b"\x1b[A\rtest_db=# SELECT * FROM orders WHERE   order_total_eur = 0 AND order_total_eur = 100;\x1b[K\x1b[A\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\r\n\r"
    psql._intercept(b"'\r(reverse-i-search)`': ")
    psql._intercept(b"\x08\x08\x08=': SELECT * FROM orders WHERE   order_total_eur = 0 AND order_total_eur \x1b[7m=\x1b[27m 100;\x08\x08\x08\x08\x08\x08")
    psql._intercept(b"\x1b[A\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C1': SELECT * FROM orders WHERE   order_total_eur = 0 AND order_total_eur \x1b[7m= 1\x1b[27m00;\x08\x08\x08\x08\x08\x08")
    psql._intercept(b"\x1b[A\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C0': SELECT * FROM orders WHERE   order_total_eur = 0 AND order_total_eur \x1b[7m= 10\x1b[27m0;\x08\x08\x08\x08\x08\x08")
    psql._intercept(b"\x1b[A\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C0': SELECT * FROM orders WHERE   order_total_eur = 0 AND order_total_eur \x1b[7m= 100\x1b[27m;\x08\x08\x08\x08\x08\x08")
    psql._intercept(b"\x1b[A\rtest_db=# SELECT * FROM orders WHERE   order_total_eur = 0 AND order_total_eur = 100;\x1b[K\x1b[A\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\r\n\r")
    psql._intercept(b"\r\n\x1b[?2004l\r")
    assert psql.pg4n_message == "Test"
    case_query_2_prompt = b" order_id | order_total_eur | customer_id \r\n----------+-----------------+-------------\r\n(0 rows)\r\n\r\n\x1b[?2004htest_db=# "
    assert psql._intercept(case_query_2_prompt) == \
        b" order_id | order_total_eur | customer_id \r\n----------+-----------------+-------------\r\n(0 rows)\r\n\r\n\r\nTest\r\n\r\n\x1b[?2004htest_db=# "
    psql = new_psqlwrapper()

    # Arrow-up to previous query, alt-B until 'orders   WHERE' and remove the
    # extra 2 whitespaces:
    psql._intercept(b'SELECT * FROM orders WHERE   order_total_eur = 0 AND order_total_eur = 100;')
    psql._intercept(b'\r\x1b[C')
    psql._intercept(b'\x1b[A\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C')
    psql._intercept(b'\x08\x08\x08\x08\x08\x08')
    psql._intercept(b'\x08\x08\x08\x08\x08\x08')
    psql._intercept(b'\x08\x08\x08\x08')
    psql._intercept(b'\x08\x08')
    psql._intercept(b'\x08\x08\x08\x08\x08\x08')
    psql._intercept(b'\x08\x08\x08\x08\x08\x08')
    psql._intercept(b'\x08\x08\x08\x08\x08\x08')
    psql._intercept(b'\x08order_total_eur = 0 AND order_total_eur = 1\x1b[1P00;\x1b[A\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C')
    psql._intercept(b'\x08order_total_eur = 0 AND order_total_eur = 10\x1b[C\x1b[1P;\x1b[A\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C\x1b[C')
    psql._intercept(b'\r\n\r\r\n')
    assert psql.pg4n_message == "Test"
    
    assert psql._intercept(b'\x1b[?2004l\r order_id | order_total_eur | customer_id \r\n----------+-----------------+-------------\r\n(0 rows)\r\n\r\n\x1b[?2004htest_db=# ') == \
        b'\x1b[?2004l\r order_id | order_total_eur | customer_id \r\n----------+-----------------+-------------\r\n(0 rows)\r\n\r\n\r\nTest\r\n\r\n\x1b[?2004htest_db=# '

    psql = new_psqlwrapper()

    # multiline query
    psql._intercept(b'SELECT')
    psql._intercept(b'\r\n\x1b[?2004l\r')
    psql._intercept(b'*')
    psql._intercept(b'\r\n\x1b[?2004l\r')
    psql._intercept(b'\x1b[?2004htest_db-# ')
    psql._intercept(b'FROM')
    psql._intercept(b'\r\n')
    psql._intercept(b'\x1b[?2004l\r\x1b[?2004htest_db-# ')
    psql._intercept(b'orders')
    psql._intercept(b'\r\n')
    psql._intercept(b'\x1b[?2004l\r\x1b[?2004htest_db-# ')
    psql._intercept(b';')
    psql._intercept(b'\r\n')
    assert psql.pg4n_message == "Test"
    psql._intercept(b'\x1b[?2004l\r\x1b[?1049h\x1b=\r order_id | order_total_eur | customer_id \x1b[m\r\n----------+-----------------+-------------\x1b[m\r\n        1 |          535.36 |         111\x1b[m\r\n        2 |          409.80 |         217\x1b[m\r\n        3 |          189.43 |          19\x1b[m\r\n        4 |          144.14 |         157\x1b[m\r\n        5 |          582.52 |         172\x1b[m\r\n        6 |          132.85 |         206\x1b[m\r\n        7 |          183.92 |         236\x1b[m\r\n        8 |          424.80 |         244\x1b[m\r\n        9 |          519.43 |         175\x1b[m\r\n       10 |          414.55 |         234\x1b[m\r\n       11 |           88.19 |          50\x1b[m\r\n       12 |          591.72 |         143\x1b[m\r\n       13 |          503.52 |         216\x1b[m\r\n       14 |          586.06 |         181\x1b[m\r\n       15 |           47.79 |         248\x1b[m\r\n       16 |          330.92 |         130\x1b[m\r\n       17 |          302.31 |         225\x1b[m\r\n       18 |          438.38 |          26\x1b[m\r\n       19 |          107.53 |          94\x1b[m\r\n       20 | '
b'         207.60 |           9\x1b[m\r\n       21 |          471.12 |         179\x1b[m\r\n:\x1b[K')
    psql._intercept(b'\r\x1b[K\x1b>\x1b[r\x1b[?1049l')
    assert psql._intercept(b'\x1b[?2004htest_db=# ') == \
        b'\r\nTest\r\n\r\n\x1b[?2004htest_db=# '
    
