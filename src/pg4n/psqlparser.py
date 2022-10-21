"""Parse psql output."""

from typing import List, TextIO
from pyparsing import CaselessLiteral, Char, Literal, MatchFirst, OneOrMore, \
    ParseException, ParseResults, ParserElement, SkipTo, Word, \
    identbodychars
from string import printable

ParserElement.enablePackrat(None)  # memoization optimization


class PsqlParser:
    """Parses psql output for syntactic analysis."""

    debug: bool = False

    prompt_chars: str = identbodychars
    control_codes: str = '\x1b' + '\x08'
    stmt_end: str = ";"
    # Naively, SQL statement body can contain all printable characters
    # (incl. whitespace), apart from ';'
    stmt_chars: str = \
        printable.translate(str.maketrans("", "", stmt_end)) + control_codes

    # parser tokens:
    # superusers have =# prompt
    tok_prompt: ParserElement = \
        Word(prompt_chars) + (CaselessLiteral("=>") | CaselessLiteral("=#"))
    tok_prompt_linebreak: ParserElement = \
        CaselessLiteral("->") | CaselessLiteral("-#")
    tok_stmt_end: ParserElement = \
        Char(';')

    # parser combinations
    match_prompt: ParserElement = \
        MatchFirst([tok_prompt])
    match_sql_stmt_start: ParserElement = \
        CaselessLiteral("SELECT ")  # All interesting stmts are 'select'
    match_sql_stmt: ParserElement = \
        Word(stmt_chars)
    match_sql_stmt_end: ParserElement = \
        tok_stmt_end
    match_error: ParserElement = \
        Literal("ERROR:")

    # TODO: allow multiple statements
    parse_sql_stmt: ParserElement = \
        match_prompt + match_sql_stmt + match_sql_stmt_end

    def __init__(self):
        """Plain constructor for PsqlParser."""
        pass

    def parse_for_new_prompt(self, psql: str) -> List[str]:
        """Parse for an empty prompt, usually to detect when a query \
        evaluation has ended.

        Returns an empty list if none found.
        """
        results: List[str] = []
        prompt_res: ParseResults = None
        # TODO optimization: matching trash is expensive
        match_trash_and_then_prompt: ParserElement = \
            ... + self.match_prompt

        try:
            prompt_res = match_trash_and_then_prompt.parse_string(psql)
        except ParseException as e:
            if self.debug:
                print(e.explain())

        if prompt_res:
            results = prompt_res.as_list()[1:]
        return results

    def parse_first_found_stmt(self, psql: str) -> List[str]:
        """Parse for the content between two prompts. Returns an empty \
        list if there is no statement or there was an error.

        Expected to be deprecated when detecting multiple statements is
        implemented.
        """
        match_stmt: ParserElement = \
            ... + self.match_sql_stmt_start + self.match_sql_stmt + \
            self.match_sql_stmt_end
        match_query_error: ParserElement = \
            SkipTo(self.match_error)

        results: List[str] = []
        stmt_res: ParseResults = None
        error_res: ParseResults = None
        try:
            stmt_res = match_stmt.parse_string(psql)
        except ParseException as e:
            if self.debug:
                print(e.explain())
        try:
            error_res = match_query_error.parse_string(psql)
        except ParseException as e:
            if self.debug:
                print(e.explain())

        if stmt_res:
            stmt_res_list = stmt_res.as_list()
            results = [stmt_res_list[1], stmt_res_list[2], stmt_res_list[3]]
        if error_res:
            results = []
        return results
