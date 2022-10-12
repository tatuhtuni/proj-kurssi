import os
import sys

from pg4n import psqlparser
from pg4n import psqlwrapper

if __name__ == "__main__":
    if len(sys.argv) > 1:
        wrap = psqlwrapper.PsqlWrapper(sys.argv[1].encode("utf-8"),
                                       lambda x: "Helpful message",
                                       psqlparser.PsqlParser())
    else:
        print(f"{os.path.basename(sys.executable)} main.py <database name>")
