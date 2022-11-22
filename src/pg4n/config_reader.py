import os.path
import sys
from os import getcwd, getenv
from typing import Optional

from .config_parser import ConfigParser
from .config_values import ConfigValues

CONFIG_FILE_NAME = "pg4n.conf"


class ConfigReader:
    def __init__(self):
        pass

    def read(self) -> Optional[ConfigValues]:
        """
        The configuration files are read in order from: /etc/pg4n.conf then from
        $XDG_CONFIG_HOME/pg4n.conf, or if $XDG_CONFIG_HOME is not set, from
        $HOME/.config/pg4n.conf, and lastly from $PWD/pg4n.conf, with each new
        value introduced in latter files overriding the previous value.
        """

        config_fnames = []
        config_values: ConfigValues = {}

        xdg_config_home = getenv("XDG_CONFIG_HOME")
        home = getenv("HOME")

        etc_config_path = "/etc/" + CONFIG_FILE_NAME
        if os.path.isfile(etc_config_path):
            config_fnames.append(etc_config_path)

        if xdg_config_home:
            custom_config_home_path = xdg_config_home + "/" + CONFIG_FILE_NAME
            if os.path.isfile(custom_config_home_path):
                config_fnames.append(custom_config_home_path)
        else:
            home_config_path = home + "/.config/" + CONFIG_FILE_NAME
            if os.path.isfile(home_config_path):
                config_fnames.append(home_config_path)

        # As a Last resort, we check the current directory for a config file.
        try:
            cwd = os.getcwd()
            cwd_config_path = cwd + "/" + CONFIG_FILE_NAME
            if os.path.isfile(cwd_config_path):
                config_fnames.append(cwd_config_path)
        except Exception as e:
            print(
                f"error: unable to get current working directory '{e}'", file=sys.stderr
            )

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
                    print(
                        f"error: unable to read config file: '{config_fname}' [str({e.errno})]",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"error: unable to read config file: '{config_fname}'",
                        file=sys.stderr,
                    )

        return config_values
