import re
from typing import Optional, TextIO

from .config_values import ConfigValues


class ConfigParser:
    def __init__(self, file: TextIO):
        self.file = file

    def parse(self) -> Optional[ConfigValues]:
        """
        Reads config values from file givein in __init__.
        """

        optnames = [x.lower() for x in ConfigValues.__annotations__.keys()]
        config_values: ConfigValues = {}

        option_matcher = re.compile(
            r"[ \t]*(?P<optname>\w+)[ \t]+(?P<optval>1|0|true|false|yes|no)[ \t]*$",
            flags=re.IGNORECASE,
        )

        for line in self.file.readlines():
            if match := option_matcher.match(line):
                if len(match.groups()) != 2:
                    continue
                optname = match.group("optname")
                if optname.lower() in optnames:
                    key = self._convert_from_anycase_to_propercase(optname)
                    config_values[key] = self._optval_to_bool(
                        str(match.group("optval"))
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
