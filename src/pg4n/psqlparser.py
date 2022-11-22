"""Parse psql output."""

from typing import List
from pyparsing import \
    CaselessLiteral, Char, Combine, Literal, ParseException, \
    ParseResults, ParserElement, StringEnd, White, Word, ZeroOrMore, \
    identbodychars, nums
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
    # superusers have =# prompt

    tok_stmt_end: ParserElement = \
        Char(';')

    tok_rev_prompt_end: ParserElement = \
        Literal(" >=") | Literal(" #=")

    match_rev_any_sql_stmt: ParserElement = \
        ZeroOrMore(White()) + tok_stmt_end + ... + \
        tok_rev_prompt_end

    # TODO(?): We may have to match for errors
#    match_error: ParserElement = \
#        Literal("ERROR:")

    # magic strings related seemingly solely to ctrl-R use are
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

        match_rev_prompt_end: ParserElement = \
            self.tok_rev_prompt_end

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
            self.tok_rev_prompt_end + \
            Word(self.prompt_chars) + \
            (StringEnd() | (Literal("?[\x1b") +
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
            if len(res_list) == 4:  # results include \x1b[?2004l
                results = [res_list[3][::-1],
                           res_list[2][::-1] +
                           res_list[1][::-1] +
                           res_list[0][::-1]
                           ]
            elif len(res_list) == 2:  # parsing stops right after database name
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
        # reverse string for parsing efficiency
        psql_rev = psql[::-1]  # slicing is fastest operation for reversing

        # Match statement that might have \r\n or whitespace at the end.
        # Parse prompt text at the end, so multiline queries can be cleaned
        # properly.
        match_rev_last_stmt: ParserElement = \
            ZeroOrMore(Char("\n") | Char("\r") | White()) + \
            self.match_rev_any_sql_stmt + Word(self.prompt_chars)

        results: List[str] = []
        stmt_res: ParseResults = None
        db_name: str = ""

        try:
            stmt_res = match_rev_last_stmt.parse_string(psql_rev)
        except ParseException as e:
            if self.debug:
                f = open("psqlparser.log", "a")
                f.write(str(e.explain()) + "\n")
                f.close()

        # If parsing was successful, pick interesting parts.
        if stmt_res:
            stmt_res_list = stmt_res.as_list()
            length: int = len(stmt_res_list)

            results = [stmt_res_list[length - 3],
                       stmt_res_list[length - 4]]
            db_name = stmt_res_list[length - 1][::-1]

        # reverse back, concatenate, and remove \n's

        # TODO/BUG: removing \n's is a tough problem, see:
        # "pgdb=> SELECT * FROM orders WHERE order_tot
        # al_eur = 100;"
        # should parse as \n -> ""
        # "pgdb=> SELECT
        # * FROM orders WHERE order_total_eur = 100;
        # should parse as \n -> " " to avoid "SELECT* FROM ..".
        # replacing \n's with "" maybe has less edge cases. Or actually more.
        reversed_flattened_res: str = \
            reduce(lambda x, y: x + y[::-1], results, "")
        no_newlines_res = reversed_flattened_res.replace('\n', ' ')

        # Is the statement a SELECT statement?
        match_select_stmt: ParserElement = \
            ZeroOrMore(White()) + CaselessLiteral("SELECT")
        is_select: bool = False
        try:
            is_select = \
                match_select_stmt.parse_string(no_newlines_res) is not []
        except ParseException as e:
            if self.debug:
                f = open("psqlparser.log", "a")
                f.write(str(e.explain()) + "\n")
                f.close()

        # If it is SELECT, remove multiline delimiters and then statement is
        # ready for analysis.
        if is_select:
            demultilined_res = no_newlines_res\
                .replace(db_name + "->", "")\
                .replace(db_name + "-#", "")
            return demultilined_res
        else:
            return ""

    def parse_psql_version(self, psql: str) -> str:
        """Parse for psql version and return version number.

        :param psql: psql --version output

        :returns: version string (e.g "14.5")
        """

        match_version_stmt: ParserElement = \
            Literal("psql (PostgreSQL) ") + Combine(Word(nums) + '.' + Word(nums))
        stmt_res: ParseResults = None
        result: str = ""

        try:
            stmt_res = match_version_stmt.parse_string(psql)
        except ParseException as e:
            if self.debug:
                f = open("psqlparser.log", "a")
                f.write(str(e.explain()) + "\n")
                f.close()

        if stmt_res:
            result = stmt_res.as_list()[1]

        return result
