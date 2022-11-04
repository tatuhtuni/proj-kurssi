def main():
    import os
    import sys

    from . import psqlparser
    from . import psqlwrapper

    if len(sys.argv) > 1:
        psql = psqlwrapper.PsqlWrapper(sys.argv[1].encode("utf-8"),
                                       lambda x: "Helpful message",
                                       psqlparser.PsqlParser())
        psql.start()
    else:
        print(f"{os.path.basename(sys.executable)} main.py <database name>")


if __name__ == "__main__":
    main()
