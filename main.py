import sys

import wrapper

if len(sys.argv) > 1:
    wrap = wrapper.Wrapper(sys.argv[1].encode("utf-8"))
else:
    print("python3 main.py <database name>")
