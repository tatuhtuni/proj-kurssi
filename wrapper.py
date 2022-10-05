from typing import Callable, TextIO
import pexpect

import psqlparser

# ANSI escape codes (\x1b[) used by psql:
# K clear part of the line
# m SGR (set graphics mode)
# ?1049 these
# ?2004 are
# ?1h   cursor?
# ?1l   escapes (h = high, l = low)
# >
# =

class Wrapper:

    debug: bool = True
    fout: TextIO

    input_log: bytes = b''
    output_log: bytes = b''
    db_name: bytes = b''
    analyze: Callable[str, str]

    def __init__(self, db_name_parameter: bytes, hook_f: Callable[str, str], parser: psqlparser.PsqlParser):
        """
        Start wrapper on selected database.
        """

        self.db_name = db_name_parameter
        self.analyze = hook_f

        c = pexpect.spawn("psql " + bytes.decode(self.db_name),
                          encoding="utf-8",
                          dimensions=(48, 160))

        if self.debug:
            self.fout = open('wrapper.log', 'w')

        c.interact(input_filter=self.ifilter,
                   output_filter=self.ofilter)

    def ifilter(self, input: bytes) -> bytes:
        self.input_log = self.input_log + input
        return input

    def ofilter(self, output: bytes) -> bytes:
        """
        Forward output to check_and_act_on_repl_output() and flush output log if substitute output is returned and replace output with it accordingly.
        """

        new_output: bytes = self.check_and_act_on_repl_output(output,
                                                              self.output_log)

        if new_output is not None:
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
        """
        Detect if psql has run a statement and is asking for a new statement ("=> " prompt). Entering a statement will always lead to a new prompt, so now the output_log will be examined if an interesting statement was run, and if it was run successfully. If so, add a fitting message in before the new prompt.
        psql_log is only needed for analysis. latest_output is what's being changed and returned in new form.
        """
        prompt: bytes = self.db_name + b'=> '

        # psql output for new prompt will (pending bug reports) look like this b'db_name=> '
        there_is_new_prompt: bool = latest_output.endswith(prompt)

        if there_is_new_prompt:
            previous_prompt_ends_at: int = psql_log.rfind(prompt)
            if previous_prompt_ends_at < 0:
                # User has entered a new statement right on our previously modified prompt,
                # so => was lost when flushing output log
                previous_prompt_ends_at = -1

            decoded_stmt_and_result: str = bytes.decode(
                psql_log[previous_prompt_ends_at + 1:]) + bytes.decode(latest_output[:latest_output.find(prompt)])

            if self.debug:
                self.fout.write(decoded_stmt_and_result)

            helpful_message: str = analyze(decoded_stmt_and_result)

            return self.replace_prompt(helpful_message,
                                       latest_output)

        return None

    def replace_prompt(self, new_prompt_msg: str,
                       prompt: bytes):
        """
        Fit the new message in the right place, just on the previous line before new prompt
        """

        newline_pos: int = prompt.rfind(b'\n')
        if newline_pos < 0:
            # trivial case
            return new_prompt_msg.encode("utf-8") + b'\r\n' + prompt  # b'\x1b[?2004hpgdb=> '
        else:
            # prompt is more complicated and has newlines in it: psql REPL has outputted a multiline chunk that may
            # a) start from previous prompt
            # b) amidst a return statement

            prompt_line_begins_at: int = prompt.rfind(b'\x1b')

            return prompt[:prompt_line_begins_at - 1] + \
                new_prompt_msg.encode("utf-8") + b'\r\n' + prompt[prompt_line_begins_at:]
