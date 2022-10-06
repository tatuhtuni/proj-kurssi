from typing import TextIO
from pyparsing import CaselessLiteral, Char, MatchFirst, OneOrMore, ParserElement


class PsqlParser:
    """Parses psql output for syntactic analysis."""

    # class debug
    debug: bool = True
    fout: TextIO

    legalSqlStatementChars = \
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"
    # tähti sun muuta vielä tähän; regex-sääntökö tämä on?
    
    # parser tokens:
    # superusers have =# prompt
    tok_prompt: ParserElement = \
        CaselessLiteral("=>") | CaselessLiteral("=#")
    tok_prompt_newline: ParserElement = \
        CaselessLiteral("->") | CaselessLiteral("-#")
    tok_semicolon: ParserElement = \
        Char(';')

    # parser combinations
    match_prompt: ParserElement = \
        MatchFirst([tok_prompt])
#    sql_stmt: ParserElement = \
#        Word(srange("[a-zA-Z]"), srange("[a-zA-Z0-9\*\ ")
#    sql_stmt = prompt_end + stmt_body + semicolon  # TODO: allow multiple statements

    def __init__(self):
        """Plain constructor for PsqlParser."""
        if self.debug:
            self.fout = open('psqlparser.log', 'w')

    def parse_first_found_stmt(psql: str) -> str:
        """Find first statement in psql output.

        Expected to be eventually deprecated.
        """
        return
