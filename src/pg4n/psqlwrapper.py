# Written by Tatu Heikkilä, tatu.heikkila@tuni.fi
# Licensed under MIT.
"""Interface with psql and capture all input and output.

Uses pexpect in combination with pyte for interfacing and screen-scraping
respectively.
"""

from copy import deepcopy
from shutil import get_terminal_size
from typing import Callable, List

import pexpect
from pyte import Stream, Screen

from .psqlparser import PsqlParser


class PsqlWrapper:
    """Handles terminal interfacing with psql, using the parameter parser \
    to pick up relevant SQL statements and syntax errors for hook functions."""

    # debug creates pyte.screen (current screenscraping context) and
    # psqlwrapper.log (capturing terminal stream) in working directory
    debug: bool = False

    supported_psql_versions: list[str] = ["14.5"]

    def __init__(
        self,
        psql_args: bytes,
        hook_semantic_f: Callable[[str], str],
        hook_syntax_f: Callable[[str], str],
        parser: PsqlParser
    ):
        """Build wrapper for selected database.

        :param psql_args: are the command-line arguments pg4n has been called \
        with.
        :param hook_semantic_f: is a callback to which scraped SQL queries are\
        passed to, and from which corresponding semantic warning messages are \
        received in return.
        :param hook_syntax_f: is a callback to which scraped syntax error \
        messages are passed to, and from which corresponding warning messages \
        are received.
        :param parser: A parser that implements the required parsing functions.
        """
        self.psql_args: bytes = psql_args
        self.semantic_analyze: Callable[[str], str] = hook_semantic_f
        self.syntax_analyze: Callable[[str], str] = hook_syntax_f
        self.parser: PsqlParser = parser

        # shutil.get_terminal_size()
        (self.cols, self.rows) = get_terminal_size()

        # pyte.Screen, pyte.Stream
        self.pyte_screen: Screen = Screen(self.cols, self.rows)
        self.pyte_screen_output_sink: Stream = Stream(self.pyte_screen)

        # Semantic analysis is always done when user presses Return
        # and resulting message is saved here until when new prompt comes in
        self.pg4n_message: str = ""

    def start(
        self
    ) -> None:
        """Start psql process and feed hook functions with \
        intercepted queries and syntax errors.

        Control is only returned after psql process exits.
        """
        version_msg = self._check_psql_version()
        if version_msg != "":
            print(version_msg)

        c = pexpect.spawn(
            "psql " + bytes.decode(self.psql_args),
            encoding="utf-8",
            dimensions=(self.rows, self.cols)
        )

        c.interact(input_filter=lambda x: x, output_filter=self._intercept)

    def _check_psql_version(self) -> str:
        """Check PostgreSQL version via psql child process and match \
        against versions pg4n is tested with.

        :returns: an empty string if current version has been tested, \
        otherwise a warning message.
        """
        version_info = pexpect.spawn(
            "psql " + bytes.decode(self.psql_args) + " --version"
        )
        version_info.expect(pexpect.EOF)
        version_info_str: str = bytes.decode(version_info.before)
        version: str = self.parser.parse_psql_version(version_info_str)

        # command-line arguments prevent interactive sessions (e.g pg4n --help)
        if version == "":
            return ""

        version_ok: bool = version in self.supported_psql_versions
        if version_ok:
            return ""
        else:
            return (
                "Pg4n has only been tested on psql versions "
                + str(self.supported_psql_versions)
                + "."
            )

    def _intercept(
        self,
        output: bytes
    ) -> bytes:
        """Forward output to `_check_and_act_on_repl_output` and feed \
        output to pyte screen for screenscraping.

        :param output: output seen on terminal screen.
        :returns: output with injected semantic error messages.
        """
        new_output: bytes = self._check_and_act_on_repl_output(output)

        self.pyte_screen_output_sink.feed(bytes.decode(new_output))

        if self.debug:
            f = open("pyte.screen", "w")
            f.write(
                '\n'.join(line.rstrip() for line in self.pyte_screen.display)
            )
            f.close()
            g = open("psqlwrapper.log", "a")
            g.write(str(new_output) + '\n')
            g.close()

        return new_output

    def _check_and_act_on_repl_output(
        self,
        latest_output: bytes
    ) -> bytes:
        """Check if user has hit Return so we can start analyzing, \
        or if a fresh prompt has come in and we can show them a helpful \
        message.

        :param latest_output: is used for Return press and fresh prompt \
        analysis. It is also what the helpful message will be injected to.
        :returns: output with injected semantic error messages.
        """
        new_output: bytes = ""
        # for optimization reasons, check output only if len() > 1, so most
        # keyboard input does not trigger parsing
        # (Return is always at least 2 length)
        if len(latest_output) <= 1:
            return latest_output

        # User hit Return: parse for potential SQL query, analyze, and
        # save a potential warning to be included in before next fresh prompt.
        if self._user_hit_return(latest_output):
            # get terminal screen contents
            screen: str = \
                '\n'.join(line.rstrip() for line in self.pyte_screen.display)

            parsed_sql_query: str = self.parser.parse_last_stmt(screen)
            if parsed_sql_query != "":
                # feed query to semantic analysis hook function
                # and save resulting message
                self.pg4n_message = self.semantic_analyze(parsed_sql_query)

        # If there is a fresh prompt:
        if self.parser.output_has_new_prompt(
                bytes.decode(latest_output)
        ):
            # If we have a semantic error message waiting
            if self.pg4n_message != "":
                new_output = self._replace_prompt(latest_output)
                self.pg4n_message = ""
                return new_output

            # Since latest_output contains error details, we will have to
            # see how the screen would look like, but still allow injecting
            # an insightful syntax error message from the syntax analysis.
            potential_future_screen = \
                deepcopy(self.pyte_screen)
            potential_future_screen_output_sink = \
                Stream(potential_future_screen)
            potential_future_screen_output_sink.feed(
                bytes.decode(latest_output)
            )

            potential_future_contents: str = '\n'.join(
                line.rstrip() for line in potential_future_screen.display
            )
            syntax_error = \
                self.parser.parse_syntax_error(potential_future_contents)
            if syntax_error != "":
                self.pg4n_message = self.syntax_analyze(syntax_error)
                new_output = self._replace_prompt(latest_output)
                self.pg4n_message = ""
                return new_output

        return latest_output

    def _replace_prompt(self, prompt: bytes) -> bytes:
        """Inject saved semantic error message into given prompt.

        :param prompt: is output where message is injected to. A fresh prompt \
        is expected, or otherwise no injection is made.
        :returns: a prompt with injected message, or unchanged if \
        detecting new prompt fails.
        """
        split_prompt: List[str] = self.parser.parse_new_prompt_and_rest(
                bytes.decode(prompt, "utf-8")
        )
        if split_prompt == []:
            return prompt  # prompt is malformed and is returned as-is.
        print_msg = self.pg4n_message.replace("\n", "\r\n")
        return bytes(
            split_prompt[0] + "\r\n"
            + print_msg + "\r\n\r\n"
            + split_prompt[1],
            "utf-8"
        )

    def _user_hit_return(self, output: bytes) -> bool:
        """Check if user hit return.

        :param output: is raw console output to be checked for return press.
        :returns: if user has hit return.
        """
        # trivial case:
        # we assume only situation with \r\n at start of
        # line is when user has pressed enter
        if output[0:2] == b"\r\n":
            return True

        # complicated case: user has ctrl-R'd, copypasted command or something.
        # and the \r\n is somewhere in midst of output..
        if self.parser.output_has_magical_return(
                bytes.decode(output, "utf-8")
        ):
            return True
        return False
