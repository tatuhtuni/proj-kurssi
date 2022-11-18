import os.path
import sys
from os import getenv
from typing import TextIO, TypedDict, Optional
from pprint import pprint

from .config_values import ConfigValues
from .config_parser import ConfigParser

# Adds way more readable python error messages
from IPython.core import ultratb
sys.excepthook = ultratb.FormattedTB(mode='Verbose', color_scheme='Linux', call_pdb=False)


class ConfigReader:
    def __init__(self):
        pass

    def read(self) -> Optional[ConfigValues]:
        """
        The configuration files are read in order from:
        /etc/pg4n/config, $XDG_CONFIG_HOME/pg4n/config, or if
        $XDG_CONFIG_HOME is not set, from $HOME/.config/pg4n/config,
        each new value introduced in latter files overriding the previous value.
        """

        config_fnames = []
        config_values: ConfigValues = {}

        xdg_config_home = getenv("XDG_CONFIG_HOME")
        home = getenv("HOME")

        if os.path.isfile("/etc/pg4n/config"):
            config_fnames.append("/etc/pg4n/config")
        elif xdg_config_home:
            if os.path.isfile(xdg_config_home + "/pg4n/config"):
                config_fnames.append(xdg_config_home + "/pg4n/config")
        else:
            if os.path.isfile(home + "/pg4n/config"):
                config_fnames.append(home + "/pg4n/config")

        if len(config_fnames) == 0:
            return None

        for config_fname in config_fnames:
            try:
                with open(config_fname, "r") as config_file:
                    config_parser = ConfigParser(config_file)
                    new_config_values: ConfigValues = config_parser.parse()
                    if new_config_values is not None:
                        for k, v in new_config_values.items():
                            config_values[k] = v
            except Exception as e:
                if hasattr(e, "errno"):
                    print(f"error: unable_to_read config file: '{config_fname}' [str({e.errno})]",
                          file=sys.stderr)
                else:
                    print(f"error: unable_to_read config file: '{config_fname}'",
                          file=sys.stderr)
                exit(1)

def main():
    config_reader = ConfigReader()
    config_values: ConfigValues = config_reader.read()

if __name__ == "__main__":
    main()
