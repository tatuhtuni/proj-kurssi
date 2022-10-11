"""Interface with psql and capture all input and output."""

from typing import Callable, List, TextIO
from functools import reduce
import pexpect

from psqlparser import PsqlParser

# ANSI escape codes (\x1b[) used by psql:
# K clear part of the line
# m SGR (set graphics mode)
# ?1049 these
# ?2004 are
# ?1h   cursor?
# ?1l   escapes (h = high, l = low)
# >
# =


class PsqlWrapper:
    """Handles terminal interfacing with psql, using the parameter parser \
    to pick up relevant SQL statements for the hook function."""

    debug: bool = False
    fout: TextIO

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

        c = pexpect.spawn("psql " + bytes.decode(self.db_name),
                          encoding="utf-8",
                          dimensions=(48, 160))

        if self.debug:
            self.fout = open('wrapper.log', 'w')

        c.interact(input_filter=self.ifilter,
                   output_filter=self.ofilter)

    def ifilter(self, input: bytes) -> bytes:
        """User input filter function for pexpect.interact: not used."""
        self.input_log = self.input_log + input
        return input

    def ofilter(self, output: bytes) -> bytes:
        """Forward output to check_and_act_on_repl_output() and flush \
        output log if substitute output is returned and replace output \
        with it accordingly."""
        new_output: bytes = self.check_and_act_on_repl_output(output,
                                                              self.output_log)

        if new_output != b'':
            self.output_log = b''
            if self.debug:
                self.fout.write(str(new_output) + '\n')
            return new_output
        else:
            self.output_log = self.output_log + output
            if self.debug:
                self.fout.write(str(output) + '\n')
            return output

    def check_and_act_on_repl_output(self, latest_output: bytes,
                                     psql_log: bytes) -> bytes:
        """Detect if psql has run a statement and is asking for \
        a new statement ("=> " prompt).

        Entering a statement will always lead to a new prompt, so now \
        the output_log will be examined if an interesting statement was run, \
        and if it was run successfully. If so, add a fitting message in \
        before the new prompt.
        psql_log is only needed for analysis.
        latest_output is what's being changed and returned in new form.
        """
        there_is_new_prompt: bool = len(self.parser.parse_for_new_prompt(
            bytes.decode(latest_output))) > 0

        if there_is_new_prompt:
            # BUG: control codes are not handled. #27
            parsed_psql_stmt: List[str] = \
                self.parser.parse_first_found_stmt(
                    bytes.decode(psql_log + latest_output))

            if parsed_psql_stmt:
                sql_stmt: str = reduce(lambda x, y: x + y, parsed_psql_stmt)

                if self.debug:
                    self.fout.write(sql_stmt + '\n')

                helpful_message: str = \
                    self.analyze(sql_stmt)

                return self.replace_prompt(helpful_message,
                                           latest_output)

        return b''

    def replace_prompt(self, new_prompt_msg: str,
                       prompt: bytes):
        """Fit the new message in the right place, \
        just on the previous line before new prompt."""
        newline_pos: int = prompt.rfind(b'\n')

        if newline_pos < 0:
            # trivial case. # b'\x1b[?2004hpgdb=> '
            return new_prompt_msg.encode("utf-8") + b'\r\n' + prompt
        else:
            # prompt is more complicated and has newlines in it:
            # psql REPL has outputted a multiline chunk that may
            # a) start from previous prompt
            # b) amidst a return statement

            prompt_line_begins_at: int = prompt.rfind(b'\x1b')

            return prompt[:prompt_line_begins_at - 1] + \
                new_prompt_msg.encode("utf-8") + b'\r\n' + \
                prompt[prompt_line_begins_at:]
