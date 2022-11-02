"""Interface with psql and capture all input and output.

Uses pexpect in combination with pyte for interfacing and screen-scraping
"""

from typing import Callable
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

    input_log: bytes = b''
    output_log: bytes = b''
    db_name: bytes = b''
    analyze: Callable[str, str]
    parser: PsqlParser

    def __init__(self, db_name_parameter: bytes,
                 hook_f: Callable[str, str], parser: PsqlParser):
        """Start wrapper on selected database."""
        self.db_name = db_name_parameter
        self.analyze = hook_f
        self.parser = parser

        (self.cols, self.rows) = get_terminal_size()

        self.pyte_screen: Screen = Screen(self.cols, self.rows)
        self.pyte_screen_output_sink: Stream = Stream(self.pyte_screen)

        # Mennäänkin niin, että analyysi suoritetaan aina enteriä painaessa,
        # ja jos löytyy järkevä lause niin tuotetaan viesti, joka tallennetaan
        # seuraavaa promptia varten
        # Analysis is always done when user presses Return
        # and resulting message is saved until when new prompt comes in
        self.pg4n_message: str = ""

        c = pexpect.spawn("psql " + bytes.decode(self.db_name),
                          encoding="utf-8",
                          dimensions=(self.rows, self.cols))

        c.interact(input_filter=self.ifilter,
                   output_filter=self.ofilter)

    def ifilter(self, input: bytes) -> bytes:
        """User input filter function for pexpect.interact: not used."""
        self.input_log = self.input_log + input
        return input

    def ofilter(self, output: bytes) -> bytes:
        """Forward output to check_and_act_on_repl_output() and feed \
        output to pyte screen for future screen-scraping."""
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

        `latest_output` is in what the helpful message will be included in. \
        It is also used for Return press and fresh prompt analysis.
        """
        # for optimization reasons, check output only if len() > 1, so keyboard
        # input does not trigger parsing (Return is at least 2 length)
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
                if self.debug:
                    self.pg4n_message = "SQL query: " + parsed_sql_query + \
                        "\r\n" + self.analyze(parsed_sql_query)
                else:
                    self.pg4n_message = self.analyze(parsed_sql_query)
            pass

        # optimization: do not spend time parsing if there is no message:
        # AND-clause will stop executing after first False.
        if self.pg4n_message != "" \
           and self.parser.parse_for_new_prompt(
               bytes.decode(latest_output)) != []:  # there is new prompt
            new_output: bytes = self.replace_prompt(latest_output)
            self.pg4n_message = ""
            return new_output

        return latest_output

    def replace_prompt(self, prompt: bytes):
        """Fit the new message in the right place, \
        just on the previous line before new prompt."""
        # TODO: fix and enhance this very naive implementation

        newline_pos: int = prompt.rfind(b'\n')

        if newline_pos < 0:
            # trivial case. # b'\x1b[?2004hpgdb=> '
            return self.pg4n_message.encode("utf-8") + b'\r\n' + prompt
        else:
            # prompt is more complicated and has newlines in it:
            # psql REPL has outputted a multiline chunk that may
            # a) start from previous prompt
            # b) amidst a return statement

            prompt_line_begins_at: int = prompt.rfind(b'\x1b')

            return prompt[:prompt_line_begins_at - 1] + \
                self.pg4n_message.encode("utf-8") + b'\r\n' + \
                prompt[prompt_line_begins_at:]

    def _user_hit_return(self, output: bytes) -> bool:
        # trivial case:
        # we assume only situation with \r\n at start of
        # line is when user has pressed enter
        if output[0:2] == b'\r\n':
            return True

        # complicated case: user has ctrl-R'd, copy-pasted command or something,
        # and the \r\n is somewhere in midst of output..
        if self.parser.parse_for_magical_return(
                bytes.decode(output, "utf-8")) != []:
            return True
