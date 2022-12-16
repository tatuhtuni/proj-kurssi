def main() -> None:
    import os
    import sys
    from functools import reduce
    from typing import Optional

    from .psqlconninfo import PsqlConnInfo
    from .semanticrouter import SemanticRouter
    from .psqlparser import PsqlParser
    from .psqlwrapper import PsqlWrapper
    from .config_reader import ConfigReader
    from .config_values import ConfigValues

    config_values: Optional[ConfigValues] = None
    try:
        config_reader = ConfigReader()
        config_values = config_reader.read()
    except Exception as e:
        pass

    if len(sys.argv) > 1:
        conn_info = PsqlConnInfo(
            reduce(lambda x, y: x + y, sys.argv[1:], "")  # concat arguments
            ).get()
        sem_router = SemanticRouter(*conn_info, config_values)  # asterisk unpacks the 5-tuple
        psql = PsqlWrapper(sys.argv[1].encode("utf-8"),
                           sem_router.run_analysis,
                           PsqlParser())
        psql.start()
    else:
        print(f"{os.path.basename(sys.executable)} \
                main.py [psql arguments] <database name>")


if __name__ == "__main__":
    main()
