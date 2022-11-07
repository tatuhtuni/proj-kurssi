"""
Test PsqlWrapper, and secondarily PsqlParser.
"""

from ..psqlwrapper import PsqlWrapper
from ..psqlparser import PsqlParser


def new_psqlwrapper() -> PsqlWrapper:
    psql = PsqlWrapper("",
                       lambda x: "Test",
                       PsqlParser())

    case_psql_start = \
        b'psql (14.5)\r\nType "help" for help.\r\n\r\n\x1b[?2004hpgdb=# '
    psql.ofilter(case_psql_start)
    return psql


def test_ofilter() -> None:
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
    psql.ofilter(case_query_1)
    psql.ofilter(return_press_1)
    
    assert psql.ofilter(fresh_prompt_1) == \
        b'\r\n' + b'Test' + b'\r\n\r\n' + b'\x1b[?2004hpgdb=# '

    psql = new_psqlwrapper()

    # ctrl-R to previous query. Query includes a return press near end.
    case_query_2 = \
        b'\r\x1b[16Ppgdb=# SELECT * FROM orders  WHERE order_total_eur = 0 AND order_tot\x08\r\n\x1b[?2004l\r'
    psql.ofilter(b'\r(reverse-i-search)`\': ')
    psql.ofilter(b"\x08\x08\x08t': SELECT * FROM orders  WHERE order_total_eur = 0 AND order_to\x1b[7mt\x1b[27mal_eur = 100;\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08\x08")
    psql.ofilter(case_query_2)
    # fresh prompt in this case:
    case_query_2_prompt = \
        b' order_id | order_total_eur | customer_id \r\n----------+-----------------+-------------\r\n(0 rows)\r\n\r\n\x1b[?2004hpgdb=# '

    assert psql.ofilter(case_query_2_prompt) == \
        b' order_id | order_total_eur | customer_id \r\n----------+-----------------+-------------\r\n(0 rows)\r\n\r\n' + b'\r\n' + b'Test' + b'\r\n\r\n' + b'\x1b[?2004hpgdb=# '

    psql = new_psqlwrapper()

    # Arrow-up to previous query, alt-B until 'orders  WHERE' and remove the
    # extra whitespace:
    # Something weird going this test case. Pyte understands this case right
    # in live scenarios most of the time, but I cannot reproduce it with this
    # saved stream of characters.
#    case_query_3 = \
#        b'SELECT * FROM orders  WHERE order_total_eur = 0 AND order_total_eur = 100;'
#    psql.ofilter(case_query_3)
#    psql.ofilter(b'\x08\x08\x08\x08')
#    psql.ofilter(b'\x08\x08\x08\x08\x08\x08')
#    psql.ofilter(b'\x08\x08\x08\x08\x08\x08')
#    psql.ofilter(b'\x08\x08\x08\x08\x08\x08')
#    psql.ofilter(b'\x08\x08\x08\x08')
#    psql.ofilter(b'\x08\x08')
#    psql.ofilter(b'\x08\x08\x08\x08\x08\x08')
#    psql.ofilter(b'\x08\x08\x08\x08\x08\x08')
#    psql.ofilter(b'\x08\x08\x08\x08\x08\x08')
#    psql.ofilter(b'\x08\x1b[1P')
#    psql.ofilter(b'\x08\x1b[1P')
#    psql.ofilter(return_press_1)
#    print(psql.pyte_screen.display)
    # fresh prompt:
#    case_query_3_prompt = b' order_id | order_total_eur | customer_id \r\n----------+-----------------+-------------\r\n(0 rows)\r\n\r\n\x1b[?2004hpgdb=# '
#    assert psql.ofilter(case_query_3_prompt) == \
#        b' order_id | order_total_eur | customer_id \r\n----------+-----------------+-------------\r\n(0 rows)\r\n\r\n\r\nTest\r\n\r\n\x1b[?2004hpgdb=# '

    # TODO: test case for multiline -> queries
