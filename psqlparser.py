from typing import TextIO
from pyparsing import CaselessLiteral, Char, MatchFirst, OneOrMore, ParserElement, Word, \
    identbodychars
from string import printable


class PsqlParser:
    """Parses psql output for syntactic analysis."""

    # class debug
    debug: bool = True
    fout: TextIO

    prompt_chars = identbodychars
    stmt_end = ";"
    # SQL statement body can contain all printable characters (incl. whitespace), but not ';'
    stmt_chars = \
        printable.translate(str.maketrans("", "", stmt_end))
    
    # parser tokens:
    # superusers have =# prompt
    tok_prompt: ParserElement = \
        Word(prompt_chars) + CaselessLiteral("=>") | CaselessLiteral("=#")
    # one_of(["=>", "=#"])
    tok_prompt_newline: ParserElement = \
        CaselessLiteral("->") | CaselessLiteral("-#")
    # one_of(["->", "-#"])    
    tok_semicolon: ParserElement = \
        Char(';')

    # parser combinations
    match_prompt: ParserElement = \
        MatchFirst([tok_prompt])
    match_sql_stmt: ParserElement = \
        Word(stmt_chars)
    match_stmt_end: ParserElement = \
        Word(stmt_end)

    # TODO: allow multiple statements
    parse_sql_stmt: ParserElement = \
        match_prompt + match_sql_stmt + match_stmt_end

    def __init__(self):
        """Plain constructor for PsqlParser."""
        if self.debug:
            self.fout = open('psqlparser.log', 'w')

    def parse_first_found_stmt(self, psql: str) -> str:
        """Find first statement in psql output.
    
        Expected to be eventually deprecated.
        """
#        print(psql)
        for res, start, end in self.parse_sql_stmt.scan_string(psql):
            print(res)
            print(start)
            print(end)
        return ""

if __name__ == "__main__":
    p = PsqlParser()
    p.parse_first_found_stmt("pgdb=> SELECT * FROM orders;")
