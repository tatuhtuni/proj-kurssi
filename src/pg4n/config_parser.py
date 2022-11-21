import re
import sys
from dataclasses import dataclass
from typing import Optional, TextIO

from .config_values import ConfigValues


class ConfigParser:
    _option_matcher: re.Pattern = re.compile(
        r"\s*(?P<optname>\w+)\s+(?P<optval>1|0|true|false|yes|no)\s*$",
        flags=re.IGNORECASE,
    )
    _empty_line_matcher: re.Pattern = re.compile(r"^\s*$")
    _comment_matcher: re.Pattern = re.compile(r"^\s*#+.*$")

    def __init__(self, file: TextIO):
        self.file: TextIO = file

    def parse(self) -> Optional[ConfigValues]:
        """
        Reads config values from file givein in __init__.
        """

        optnames = [x.lower() for x in ConfigValues.__annotations__.keys()]
        config_values: ConfigValues = {}

        # Needed for bytes containing files
        self.file.seek(0)

        # The following stuff is for improved error messages
        @dataclass(frozen=True)
        class SeenOptionContext:
            key: str
            line: str
            line_number: int

        seen_option_contexts: list[SeenOptionContext] = []
        multiply_defined_options = set()

        for line_number, line in enumerate(self.file.readlines()):
            # To make this work with bytes objects like TemporaryFile contents
            if isinstance(line, bytes):
                line = bytes.decode(line, "utf-8")
            line = str(line)

            if match := ConfigParser._empty_line_matcher.match(line):
                continue

            if match := ConfigParser._comment_matcher.match(line):
                continue

            if match := ConfigParser._option_matcher.match(line):
                optname = match.group("optname")
                if optname.lower() in optnames:
                    key = self._convert_from_anycase_to_propercase(optname)
                    config_values[key] = self._optval_to_bool(
                        str(match.group("optval"))
                    )

                    if key in [x.key for x in seen_option_contexts]:
                        seen_option_contexts.append(
                            SeenOptionContext(
                                key=key, line=line.rstrip("\n"), line_number=line_number
                            )
                        )
                        multiply_defined_options.add(key)
                    else:
                        seen_option_contexts.append(
                            SeenOptionContext(
                                key=key, line=line.rstrip("\n"), line_number=line_number
                            )
                        )
                else:
                    output_line = line.rstrip("\n")
                    print(
                        f"warning: bad warning name or value in line {line_number}: '{output_line}' in configuration file: '{self.file.name}'",
                        file=sys.stderr,
                    )
            else:
                output_line = line.rstrip("\n")
                print(
                    f"warning: unable to parse line {line_number}: '{output_line}' in configuration file: '{self.file.name}'",
                    file=sys.stderr,
                )

        # Not so brilliant computational complexity
        for key in multiply_defined_options:
            seen_option_lines = ""
            for ctx in filter(lambda x: x.key == key, seen_option_contexts):
                seen_option_lines += f"line {ctx.line_number}: '{ctx.line}'\n"
            print(
                f"warning: option '{key}' is set multiple times\n{seen_option_lines}in configuration file: '{self.file.name}'",
                file=sys.stderr,
            )

        return config_values if len(config_values) > 0 else None

    def _optval_to_bool(self, optval: str) -> bool:
        """
        Excepts only valid option values.
        Converts config file option value string into bool.
        """

        true_values = ("true", "1", "yes")
        false_values = ("false", "0", "no")

        if optval.lower() in true_values:
            return True
        if optval.lower() in false_values:
            return False

        return False

    def _convert_from_anycase_to_propercase(self, anycase_key: str) -> str:
        """
        Assumes that anycase_key matches some option name case-insensitively.

        Users can write option values case-insensitively. We still need to
        convert that user written value to the proper one that matches the
        fields in ConfigValues class.
        """

        fields = ConfigValues.__annotations__.keys()
        for field in fields:
            if anycase_key.lower() == field.lower():
                return field
