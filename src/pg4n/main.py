from functools import reduce
import sys
from typing import Optional

import pexpect

from .psqlconninfo import PsqlConnInfo
from .semanticrouter import SemanticRouter
from .psqlparser import PsqlParser
from .psqlwrapper import PsqlWrapper
from .config_reader import ConfigReader
from .config_values import ConfigValues


def main() -> None:
    """Initiate session by getting psql connection parameters via psql \
    child process, initializing semantic analysis and wrapper modules, \
    and then starting the session."""

    # Configuration is ignored if there is error reading config files
    config_values: Optional[ConfigValues] = None
    try:
        config_reader = ConfigReader()
        config_values = config_reader.read()
    except Exception as e:
        config_values = None

    if len(sys.argv) > 1:
        conn_info = PsqlConnInfo(
            reduce(lambda x, y: x + y, sys.argv[1:], "")  # concat arguments
            ).get()
        if conn_info is not None:
            # asterisk unpacks the 5-tuple
            sem_router = SemanticRouter(*conn_info, config_values)
            psql = PsqlWrapper(
                sys.argv[1].encode("utf-8"),
                # semantic analysis:
                sem_router.run_analysis,
                # no syntax error analysis:
                lambda syntax_error_analysis: "",
                PsqlParser()
            )
            psql.start()
        else:
            # Psql is not connecting to any database,
            # e.g. "pg4n --help" is being run.
            # for simplicity, just use pexpect here
            psql_output = pexpect.spawn(
                "psql "
                + reduce(
                    lambda x, y: x + y, sys.argv[1:], ""
                )
            )
            psql_output.expect(pexpect.EOF)
            print(bytes.decode(psql_output.before))
    else:
        print("pg4n [psql arguments] <database name>")


if __name__ == "__main__":
    main()
