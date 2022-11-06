def main() -> None:
    import os
    import sys
    from functools import reduce

    from .psqlconninfo import PsqlConnInfo
    from .semanticrouter import SemanticRouter
    from .psqlparser import PsqlParser
    from .psqlwrapper import PsqlWrapper

    if len(sys.argv) > 1:
        conn_info = PsqlConnInfo(
            reduce(lambda x, y: x + y, sys.argv[1:], "")  # concat arguments
            ).get()
        sem_router = SemanticRouter(*conn_info)  # asterisk unpacks the 5-tuple
        psql = PsqlWrapper(sys.argv[1].encode("utf-8"),
                           sem_router.run_analysis,
                           PsqlParser())
        psql.start()
    else:
        print(f"{os.path.basename(sys.executable)} \
                main.py [psql arguments] <database name>")


if __name__ == "__main__":
    main()
