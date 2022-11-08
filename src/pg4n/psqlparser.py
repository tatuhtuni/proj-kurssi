"""Parse psql output."""

from typing import List
from pyparsing import CaselessLiteral, Char, Literal, MatchFirst, \
    ParseException, ParseResults, ParserElement, StringEnd, Word, ZeroOrMore, \
    identbodychars
from functools import reduce
from string import printable

# Turn on memoization optimization
ParserElement.enablePackrat(None)

# default whitespace rules complicate things needlessly, remove them:
ParserElement.setDefaultWhitespaceChars('')


class PsqlParser:
    """Parses psql output for syntactic analysis."""

    debug: bool = False

    prompt_chars: str = identbodychars
    stmt_end: str = ";"
    # Naively, SQL statement body can contain all printable characters
    # (incl. whitespace), apart from ';'
    stmt_chars: str = \
        printable.translate(str.maketrans("", "", stmt_end))

    # rev means reversed, these are for performance reasons.

    # parser tokens:
    # superusers have =# prompt
    tok_prompt: ParserElement = \
        Char('\n') + Word(prompt_chars) + \
        (CaselessLiteral("=>") | CaselessLiteral("=#"))
    tok_rev_prompt: ParserElement = \
        (CaselessLiteral(">=") | CaselessLiteral("#=")) + \
        Word(prompt_chars) + Char('\n')

    tok_prompt_linebreak: ParserElement = \
        Char('\n') + Word(prompt_chars) + \
        (CaselessLiteral("->") | CaselessLiteral("-#"))
    tok_rev_prompt_linebreak: ParserElement = \
        (CaselessLiteral(">-") | CaselessLiteral("#-")) + \
        Word(prompt_chars) + Char('\n')

    tok_stmt_end: ParserElement = \
        Char(';')

    # parser combinations
    match_prompt: ParserElement = \
        MatchFirst([tok_prompt])
    match_sql_stmt_start: ParserElement = \
        CaselessLiteral("SELECT ")  # All interesting stmts are 'select'
    match_rev_sql_stmt_start: ParserElement = \
        CaselessLiteral(" TCELES") + (CaselessLiteral(" >=") | CaselessLiteral(" #="))
    match_sql_stmt: ParserElement = \
        Word(stmt_chars)
    match_whole_rev_sql_stmt: ParserElement = \
        tok_stmt_end + ... + match_rev_sql_stmt_start
    match_sql_stmt_end: ParserElement = \
        tok_stmt_end
    match_error: ParserElement = \
        Literal("ERROR:")

    # TODO: allow multiple statements
    parse_sql_stmt: ParserElement = \
        match_prompt + match_sql_stmt + match_sql_stmt_end

    # magic strings related to solely to ctrl-R use are
    # "\r\n\x1b[?2004l\r", "\r\n\r\r\n" and "\x08\r\n".
    match_rev_magical_returns: ParserElement = \
        Literal("\r\n\x1b[?2004l\r"[::-1]) \
        | Literal("\r\n\r\r\n"[::-1]) \
        | Literal("\x08\r\n"[::-1])

    def __init__(self):
        """Plain constructor for PsqlParser."""
        pass

    def parse_new_prompt(self, psql: str) -> List[str]:
        """Parse for an empty prompt, to detect when a query \
        evaluation has ended.

        :param psql: Raw console output that includes terminal control codes.
        :returns: an empty list if no prompt found. Otherwise has [' >='] or \
        [' #='].
        """
        # cheaper and easier to reverse & start from the end
        psql_rev: str = psql[::-1]
        results: List[str] = []
        prompt_res: ParseResults = None

        # TODO: For some reason Literal(" >=") does not provide a match,
        # but works fine without the whitespace..
        match_rev_prompt_end: ParserElement = \
            Literal(" >=") | Literal(" #=")

        try:
            prompt_res = match_rev_prompt_end.parse_string(psql_rev)
        except ParseException as e:
            if self.debug:
                f = open("psqlparser.log", "a")
                f.write(str(e.explain()) + "\n")
                f.close()

        if prompt_res:
            results = prompt_res.as_list()

        return results

    def parse_new_prompt_and_rest(self, psql: str) -> List[str]:
        """Parse for an empty prompt and everything preceding it \
        to 2 cells.

        :param psql: Raw console output that includes terminal control codes.
        :returns: a two-part list with everything before the prompt \
        line and then the prompt, to allow easy message injection.
        """
        # cheaper and easier to reverse & start from the end
        psql_rev: str = psql[::-1]
        results: List[str] = []
        prompt_res: ParseResults = None

        match_rev_prompt_and_then_rest: ParserElement = \
            (Literal(" >=") | Literal(" #=")) + \
            Word(self.prompt_chars) + \
            (StringEnd() | (Literal("?[\x1b") + \
                            ... + StringEnd()))
        # ^either stops after dbname or includes \x1b[?2004l...
        try:
            prompt_res = match_rev_prompt_and_then_rest.parse_string(psql_rev)
        except ParseException as e:
            print(e.explain())
            if self.debug:
                f = open("psqlparser.log", "a")
                f.write(str(e.explain()) + "\n")
                f.close()

        if prompt_res:
            res_list = prompt_res.as_list()
            if len(res_list) == 4:  # includes \x1b[?2004l
                results = [res_list[3][::-1],
                           res_list[2][::-1] +
                           res_list[1][::-1] +
                           res_list[0][::-1]
                           ]
            elif len(res_list) == 2:  # stops right after database name
                results = ['',
                           res_list[1][::-1] +
                           res_list[0][::-1]
                           ]

        return results

    def parse_magical_return(self, psql: str) -> List[str]:
        """Parse for weird Return presses.

        :param psql: Raw console output that includes terminal control codes.
        :returns: an empty list if no presses are found. \
        Otherwise has newline text.
        """
        psql_rev: str = psql[::-1]

        results: List[str] = []
        magical_return_res: ParseResults = None

        try:
            magical_return_res = \
                self.match_rev_magical_returns.parse_string(psql_rev)
        except ParseException as e:
            if self.debug:
                f = open("psqlparser.log", "a")
                f.write(str(e.explain()) + "\n")
                f.close()

        if magical_return_res:
            results = magical_return_res.as_list()
        return results

    def parse_last_found_stmt(self, psql: str) -> str:
        """Parse for last SQL query statement in a string.

        :param psql: screenscraped psql string

        :returns: parsed SQL query as plain string.
        """
        # reverse given string, match reversed tokens, pick first match,
        # then reverse the matched string.
        psql_rev = psql[::-1]  # slicing is fastest operation for reversing

        # BUG(?): Assumes only single statement query
        # BUG: Will match until previous SELECT query, if newest is e.g INSERT
        #      Fix it by matching rev_stmt_start also against rev_prompt
        # TODO: strip "->"/"-#" from string to allow multiline queries
        match_last_stmt: ParserElement = \
            ZeroOrMore(Char("\n") | Char("\r")) + self.match_whole_rev_sql_stmt

        results: List[str] = []
        stmt_res: ParseResults = None
        try:
            stmt_res = match_last_stmt.parse_string(psql_rev)
        except ParseException as e:
            if self.debug:
                f = open("psqlparser.log", "a")
                f.write(str(e.explain()) + "\n")
                f.close()

        if stmt_res:
            stmt_res_list = stmt_res.as_list()
            length: int = len(stmt_res_list)

            results = [stmt_res_list[length - 2],
                       stmt_res_list[length - 3],
                       stmt_res_list[length - 4]]  # reverse order

        reversed_flattened_res: str = \
            reduce(lambda x, y: x + y[::-1], results, "")
        return reversed_flattened_res
