# Written by Tatu Heikkilä, tatu.heikkila@tuni.fi
# Licensed under MIT.
"""Parse psql output."""

from functools import reduce
from string import printable
from typing import Optional

from pyparsing import (
    CaselessLiteral,
    Char,
    Combine,
    Literal,
    Opt,
    ParseException,
    ParseResults,
    ParserElement,
    StringEnd,
    White,
    Word,
    ZeroOrMore,
    identbodychars,
    nums
)

# Turn on memoization optimization
ParserElement.enablePackrat(None)

# default whitespace rules complicate things needlessly, remove them:
ParserElement.setDefaultWhitespaceChars("")


class PsqlParser:
    """Parses psql output for syntactic analysis."""

    # Turn on verbose output to psqlparser.log file in working directory
    debug: bool = False

    # Parsing functions common to more than 1 parsing functions are listed here

    prompt_chars: str = identbodychars
    stmt_end: str = ";"
    # Naively, SQL statement body can contain all printable characters
    # (incl. whitespace), apart from ';'
    stmt_chars: str = printable.translate(str.maketrans("", "", stmt_end))

    # tok, or token, is parsing element with only single element output,
    # either by only having single element, or using Combine to squash
    # multiple elements into one. These are often combined to build functions
    # for matching.

    # 'rev' in variable names is shorthand for reversed.
    # Reversing happens for performance reasons,
    # as interesting things usually are at end of a long string.

    # %/%R%x%# per postgres bin/psql/settings.h
    # prompt1 per bin/psql/prompt.c:
    # %/ = current database
    # %R = =, ^
    # %x = nothing, *, !, ?
    # %# = #, >
    # This will match against "%R%x%# ", e.g "=> ".
    tok_rev_prompt_end: ParserElement = Combine(
            Opt(White())
            + (Literal('#') | Literal('>'))
            + Opt(Literal('*') | Literal('!') | Literal('?'), "")
            + (Literal('=') | Literal('^'))
    )

    # %/%R%x%# per postgres bin/psql/settings.h
    # prompt2 per bin/psql/prompt.c:
    # %/ = current database
    # %R = -, *, ', "; also $, (
    # %x = nothing, *, !, ?
    # %# = #, >
    # This will match against "%R%x%# ", e.g "-> ".
    # To save time, since linebreak prompts are only removed,
    # just match against a list of all possible combinations
    multiline_prompt_ends: list[str] = \
        ["-#", "*#", "\'#", "\"#", "$#", "(#",
         "->", "*>", "\'>", "\">", "$>", "(>",
         "-*#", "**#", "\'*#", "\"*#", "$*#", "(*#",
         "-*>", "**>", "\'*>", "\"*>", "$*>", "(*>",
         "-!#", "*!#", "\'!#", "\"!#", "$!#", "(!#",
         "-!>", "*!>", "\'!>", "\"!>", "$!>", "(!>",
         "-?#", "*?#", "\'?#", "\"?#", "$?#", "(?#",
         "-?>", "*?>", "\'?>", "\"?>", "$?>", "(?>"]
    # ParserElement for these would look this:
    #    tok_multiline_prompt_end: ParserElement = \
    #        Combine(
    #            (Literal('-') | Literal('*') | Literal('\'') |
    #             Literal('\"') | Literal('$') | Literal('(')) +
    #            Opt(Literal('*') | Literal('!') | Literal('?'), "") +
    #            (Literal('#') | Literal('>')))

    def __init__(self):
        """Plain constructor for PsqlParser."""
        pass

    def output_has_magical_return(self, psql: str) -> bool:
        """Check for weird Return presses.

        :param psql: Raw console output that includes terminal control codes.
        :returns: if output has a weird Return press.
        """
        # cheaper and easier to reverse & start from the end
        psql_rev: str = psql[::-1]  # slicing is fastest operation for reverse

        # Based on exploratory testing,
        # magic strings (related at least to ctrl-R use) are
        # "\r\n\x1b[?2004l\r", "\r\n\r\r\n" and "\x08\r\n".
        match_rev_magical_returns: ParserElement = (
            Literal("\r\n\x1b[?2004l\r"[::-1])
            | Literal("\r\n\r\r\n"[::-1])
            | Literal("\x08\r\n"[::-1])
        )

        has_magical_return: bool = False
        magical_return_res: Optional[ParseResults] = None

        try:
            magical_return_res = \
                match_rev_magical_returns.parse_string(psql_rev)
        except ParseException as e:
            if self.debug:
                f = open("psqlparser.log", "a")
                f.write(str(e.explain()) + "\n")
                f.close()

        if magical_return_res:
            has_magical_return = True
        return has_magical_return

    def output_has_new_prompt(self, psql: str) -> bool:
        """Detect when psql query evaluation has ended by parsing for a new prompt.

        :param psql: Raw console output that includes terminal control codes.
        :returns: if output is a fresh prompt.
        """
        # cheaper and easier to reverse & start from the end
        psql_rev: str = psql[::-1]
        has_new_prompt: bool = False
        prompt_res: Optional[ParseResults] = None

        match_rev_prompt_end: ParserElement = self.tok_rev_prompt_end

        try:
            prompt_res = match_rev_prompt_end.parse_string(psql_rev)
        except ParseException as e:
            if self.debug:
                f = open("psqlparser.log", "a")
                f.write(str(e.explain()) + "\n")
                f.close()

        if prompt_res:
            has_new_prompt = True

        return has_new_prompt

    def parse_new_prompt_and_rest(self, psql: str) -> list[str]:
        """Parse for a fresh prompt and everything preceding it into 2-length \
        list, facilitating easy message injection.

        :param psql: Raw console output that includes terminal control codes.
        :returns: a two-part list with everything before the prompt \
        line and then the prompt, to allow easy message injection.
        List is empty if no fresh prompt was found.
        """
        # cheaper and easier to reverse & start from the end
        psql_rev: str = psql[::-1]
        results: list[str] = []
        prompt_res: Optional[ParseResults] = None

        match_rev_prompt_and_then_rest: ParserElement = (
            self.tok_rev_prompt_end 
            + Word(self.prompt_chars)
            + (StringEnd()         # output may stop at end of db name,
               | (  # or continue \x1b[?.. 
                   Literal("?[\x1b")  # in this case control code parameter
                   + ...              # has already been parsed as prompt_chars
                   + StringEnd()
               )
            )
        )

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
            if len(res_list) == 2:  # parsing stops right after database name
                results = [
                    '',
                    res_list[1][::-1] + res_list[0][::-1]
                ]
            elif len(res_list) == 4:  # results include \x1b[?2004h..
                results = [
                    res_list[3][::-1],
                    res_list[2][::-1] + res_list[1][::-1] + res_list[0][::-1]
                ]

        return results

    def parse_last_stmt(self, psql: str) -> str:
        """Parse for last SQL query statement in a string.

        :param psql: screenscraped psql string with only whitespace \
        after most recent query.
        :returns: parsed SQL query as plain string.
        """
        # cheaper and easier to reverse & start from the end
        psql_rev = psql[::-1]

        # Match statement that might have \r\n or whitespace at the end.
        # Parse prompt text at the end, so multiline queries can be cleaned
        # properly.
        tok_stmt_end: ParserElement = Char(';')

        match_rev_any_sql_stmt: ParserElement = (
            ZeroOrMore(White())
            + tok_stmt_end
            + ...
            + self.tok_rev_prompt_end
        )

        match_rev_last_stmt: ParserElement = (
            ZeroOrMore(Char("\n") | Char("\r") | White())
            + match_rev_any_sql_stmt
            + Word(self.prompt_chars)
        )

        results: list[str] = []
        stmt_res: Optional[ParseResults] = None
        db_name: str = ""

        try:
            stmt_res = match_rev_last_stmt.parse_string(psql_rev)
        except ParseException as e:
            if self.debug:
                f = open("psqlparser.log", "a")
                f.write(str(e.explain()) + "\n")
                f.close()

        # If parsing was successful, pick interesting parts.
        if stmt_res is not None:
            stmt_res_list = stmt_res.as_list()
            length: int = len(stmt_res_list)

            results = [
                stmt_res_list[length - 3],
                stmt_res_list[length - 4]
            ]
            db_name = stmt_res_list[length - 1][::-1]

        # reverse back, concatenate, and remove \n's
        unreversed_flattened_res: str = reduce(
            lambda x, y: x + y[::-1], results, ""
        )

        # Replacing \n's has some edge cases where wrapper transparency
        # breaks, because both of these work right in straight psql. See:
        # "=> SELECT * FROM orders WHERE order_tot
        # al_eur = 100;"
        # should convert \n -> "" to avoid "order_tot al_eur"
        # "=> SELECT
        # * FROM orders WHERE order_total_eur = 100;"
        # should convert \n -> " " to avoid "SELECT* FROM ..".
        #
        # Replacing \n's with " " seems to have less edge cases.
        no_newlines_res = unreversed_flattened_res.replace('\n', ' ')

        # Is the statement a SELECT statement?
        match_select_stmt: ParserElement = (
            ZeroOrMore(White())
            + CaselessLiteral("SELECT")
        )
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
            demultilined_res: str = no_newlines_res
            for multiline_prompt_end in self.multiline_prompt_ends:
                prompt = db_name + multiline_prompt_end
                demultilined_res = demultilined_res.replace(prompt, "")
            return demultilined_res
        else:
            return ""

    def parse_psql_version(self, psql: str) -> str:
        """Parse for psql version and return version number.

        :param psql: psql --version output
        :returns: version string (e.g "14.5")
        """
        match_version_stmt: ParserElement = (
            Literal("psql (PostgreSQL) ")
            + Combine(
                Word(nums)
                + '.'
                + Word(nums)
            )
        )
        stmt_res: Optional[ParseResults] = None
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

    def parse_syntax_error(self, psql: str) -> str:
        """Parse for syntax error output.

        :param psql: screen-scraped psql output.
        :returns: syntax error message from 'ERROR:' to last '^'.
        """
        psql_rev = psql[::-1]

        tok_marker_caret: ParserElement = Literal("^")
        tok_rev_error: ParserElement = Literal(":RORRE")
        match_error_statement: ParserElement = (
            ...
            + tok_marker_caret
            + ...
            + tok_rev_error
        )

        results: list[str] = []
        stmt_res: Optional[ParseResults] = None

        try:
            stmt_res = match_error_statement.parse_string(psql_rev)
        except ParseException as e:
            if self.debug:
                f = open("psqlparser.log", "a")
                f.write(str(e.explain()) + "\n")
                f.close()

        if stmt_res is not None:
            stmt_res_list = stmt_res.as_list()
            results = [stmt_res_list[3],
                       stmt_res_list[2],
                       stmt_res_list[1]]
        unreversed_flattened_res: str = \
            reduce(lambda x, y: x + y[::-1], results, "")

        return unreversed_flattened_res
