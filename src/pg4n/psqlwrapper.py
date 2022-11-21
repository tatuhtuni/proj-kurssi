"""Interface with psql and capture all input and output.

Uses pexpect in combination with pyte for interfacing and screen-scraping
"""

from typing import Callable, List
from functools import reduce
import pexpect
from pyte import Stream, Screen
from shutil import get_terminal_size

from .psqlparser import PsqlParser


class PsqlWrapper:
    """Handles terminal interfacing with psql, using the parameter parser \
    to pick up relevant SQL statements for the hook function.

    Only one (static) class instance is intended for use in program.
    """

    debug: bool = False
    supported_psql_versions: list[str] = ["14.5"]

    def __init__(self, psql_args: bytes,
                 hook_f: Callable[[str], str], parser: PsqlParser):
        """Build wrapper for selected database.

        :param db_name_parameter: is name of database we are connecting to.
        :param hook_f: is a callback to which scraped SQL queries are passed \
        to and from which semantic error messages are received in return.
        :param parser: A parser that implements the required parsing functions.
        """
        self.psql_args: bytes = psql_args
        self.analyze: Callable[[str], str] = hook_f
        self.parser: PsqlParser = parser

        # shutil.get_terminal_size()
        (self.cols, self.rows) = get_terminal_size()

        # pyte.Screen, pyte.Stream
        self.pyte_screen: Screen = Screen(self.cols, self.rows)
        self.pyte_screen_output_sink: Stream = Stream(self.pyte_screen)

        # Analysis is always done when user presses Return
        # and resulting message is saved until when new prompt comes in
        self.pg4n_message: str = ""

    def start(self) -> None:
        """Start psql process and then start feeding hook function with \
        intercepted output."""

        version_msg = self.check_psql_version()
        if version_msg != "":
            print(version_msg)

        c = pexpect.spawn("psql " + bytes.decode(self.psql_args),
                          encoding="utf-8",
                          dimensions=(self.rows, self.cols))

        c.interact(input_filter=self.ifilter,
                   output_filter=self.ofilter)

    def check_psql_version(self) -> str:
        version_info = \
            pexpect.spawn("psql " + bytes.decode(self.psql_args) +
                          " --version")
        version_info.expect(pexpect.EOF)
        version_info_str: str = bytes.decode(version_info.before)
        version: str = self.parser.parse_psql_version(version_info_str)
        version_ok: bool = version in self.supported_psql_versions
        if version_ok:
            return ""
        else:
            return "Pg4n has only been tested on psql versions " + \
                str(self.supported_psql_versions) + "."
        

    def ifilter(self, input: bytes) -> bytes:
        """User input filter function for pexpect.interact: not used.

        :param input: user input characters.
        :returns: unchanged input.
        """
        return input

    def ofilter(self, output: bytes) -> bytes:
        """Forward output to `check_and_act_on_repl_output()` and feed \
        output to pyte screen for future screen-scraping.

        :param output: output seen on terminal screen.
        :returns: output with injected semantic error messages.
        """
        new_output: bytes = self.check_and_act_on_repl_output(output)

        self.pyte_screen_output_sink.feed(bytes.decode(new_output))

        if self.debug:
            f = open("pyte.screen", "w")
            f.write('\n'.join(
                line.rstrip() for line in self.pyte_screen.display))
            f.close()
            g = open("psqlwrapper.log", "a")
            g.write(str(new_output) + '\n')
            g.close()

        return new_output

    def check_and_act_on_repl_output(self, latest_output: bytes) -> bytes:
        """Check if user has hit Return so we can start analyzing, \
        or if a fresh prompt has come in and we can show them a helpful \
        message.

        :param latest_output: is used for Return press and fresh prompt \
        analysis. It is also what the helpful message will be injected to.
        :returns: output with injected semantic error messages.
        """
        # for optimization reasons, check output only if len() > 1, so keyboard
        # input does not trigger parsing (Return is always at least 2 length)
        if len(latest_output) <= 1:
            return latest_output

        # User hit Return: parse for potential SQL query, analyze, and
        # possibly provide a message to be included in next new prompt.
        if self._user_hit_return(latest_output):
            screen: str = '\n'.join(
                line.rstrip() for line in self.pyte_screen.display)
            parsed_sql_query: str = \
                self.parser.parse_last_found_stmt(screen)
            if parsed_sql_query != "":
                # feed query to hook function and save resulting message
                self.pg4n_message = self.analyze(parsed_sql_query)

        # If we have a semantic error message waiting and there is a fresh
        # prompt:
        # optimization: do not spend time parsing if there is no message:
        # AND-clause will stop executing after first False.
        if self.pg4n_message != "" \
           and self.parser.parse_new_prompt(
               bytes.decode(latest_output)) != []:  # there is new prompt
            new_output: bytes = self._replace_prompt(latest_output)
            self.pg4n_message = ""
            return new_output

        return latest_output

    def _replace_prompt(self, prompt: bytes) -> bytes:
        """Inject saved semantic error message into given prompt.

        :param prompt: is where the message is injected. A fresh prompt is \
        expected.
        :returns: a prompt with injected message.
        """
        split_prompt: List[str] = \
            self.parser.parse_new_prompt_and_rest(
                bytes.decode(prompt, "utf-8"))
        print_msg = self.pg4n_message.replace("\n", "\r\n")
        return bytes(split_prompt[0] + "\r\n" +
                     print_msg + "\r\n\r\n" +
                     split_prompt[1],
                     "utf-8")

    def _user_hit_return(self, output: bytes) -> bool:
        """Check if user hit return.

        :param output: is raw console output to be checked for return press.
        :returns: if user has indeed hit return.
        """
        # trivial case:
        # we assume only situation with \r\n at start of
        # line is when user has pressed enter
        if output[0:2] == b'\r\n':
            return True

        # complicated case: user has ctrl-R'd, copypasted command or something.
        # and the \r\n is somewhere in midst of output..
        if self.parser.parse_magical_return(
                bytes.decode(output, "utf-8")) != []:
            return True
        return False
