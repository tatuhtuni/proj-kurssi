import pexpect
from typing import Optional, Tuple

from pyparsing import Literal, Word, ParseException, ParserElement, \
    identbodychars, nums


class PsqlConnInfo:
    """Get PostgreSQL server address, port, database name, and user via \
    psql by supplying same arguments as the original psql process. This \
    way we can avoid writing a command-line argument parser."""

    # tok, or token, is parsing element with only single element output,
    # either by only having single element, or using Combine to squash
    # multiple elements into one. These are often combined to build functions
    # for matching.

    tok_pre_database: ParserElement = \
        Literal("You are connected to database \"")
    tok_database: ParserElement = \
        Word(identbodychars)

    tok_pre_user: ParserElement = \
        Literal("\" as user \"")
    tok_user: ParserElement = \
        Word(identbodychars)

    tok_pre_host: ParserElement = \
        Literal("\" on host \"") | \
        Literal("\" via socket in \"")
    tok_host: ParserElement = \
        Word(identbodychars + "/.")

    tok_pre_port: ParserElement = \
        Literal("\" at port \"")
    tok_port: ParserElement = \
        Word(nums)
    tok_end: ParserElement = \
        Literal("\".")

    def __init__(
        self,
        psql_args: str
    ):
        """Use psql child process with exact same command-line arguments \
        to initialize connection info."""
        self.pg_tuple: Optional[Tuple[str, str, str, str, str]] = None
        match_psql_conninfo: ParserElement = (
            self.tok_pre_database + self.tok_database +
            self.tok_pre_user + self.tok_user +
            self.tok_pre_host + self.tok_host +
            self.tok_pre_port + self.tok_port +
            self.tok_end
        )
        pexpect_conninfo = pexpect.spawn("psql -c \"\\conninfo\" " + psql_args)
        pexpect_conninfo.expect(pexpect.EOF)
        conninfo_str = bytes.decode(pexpect_conninfo.before)
        try:
            conninfo_res = \
                match_psql_conninfo.parse_string(conninfo_str).as_list()
        except ParseException:
            return
        self.pg_host = conninfo_res[5]
        self.pg_port = conninfo_res[7]
        self.pg_user = conninfo_res[3]
        self.pg_pass = ""
        self.pg_name = conninfo_res[1]
        self.pg_tuple = (
            self.pg_host,
            self.pg_port,
            self.pg_user,
            self.pg_pass,
            self.pg_name
        )
        pass

    def get(
            self
    ) -> Optional[Tuple[str, str, str, str, str]]:
        """Get 5-tuple that has the PostgreSQL host, port, user, pass, and db \
        name.

        :returns: (hostname, port, user, password, database name) \
        5-tuple or None.
        """
        return self.pg_tuple
