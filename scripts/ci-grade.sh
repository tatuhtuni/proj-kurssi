#!/usr/bin/env bash

#
# print format infos
#

blackurl="[black](https://black.readthedocs.io/en/stable/usage_and_configuration/the_basics.html#diffs)"
pylinturl="[pylint](https://pylint.pycqa.org/en/latest/user_guide/usage/output.html#source-code-analysis-section)"
mypyurl="[mypy](https://mypy.readthedocs.io/en/stable/error_codes.html#error-codes)"
echo "ℹ Format info: $blackurl • $pylinturl • $mypyurl"

#
# print black result
#

blackres=$(black src --diff -q)
echo "<details><summary>black diff: \
-$(grep -cP '^-(?!-)' <<<"$blackres") \
+$(grep -cP '^\+(?!\+)' <<<"$blackres")</summary>

\`\`\`diff
$blackres
\`\`\`
</details>"

#
# print pylint result
#

lintres=$(pylint src --exit-zero -sn)
lintscore=$(for x in I R C W E F; do
    n=$(echo "$lintres" | grep -cP '^\S+\s+'$x)
    [ $n != 0 ] && echo $n$x
done)
[ "$lintscore" = "" ] && lintscore="OK"
echo "<details><summary>pylint score: "$lintscore"</summary>

\`\`\`diff
$(sed -E -e '/\.py:[:0-9]+\s+[CEF]/s/^/- /' \
    -e '/\.py:[:0-9]+\s+W/s/^/! /' \
    -e '/^([-!*])/! s/^/# /' <<<"$lintres")
\`\`\`
</details>"

#
# print mypy result
#

mypyres=$(mypy --non-interactive --install-types --ignore-missing-imports --strict --show-error-codes src 2>&1)
echo "<details><summary>mypy errors: $(sed -nr 's/^Found ([0-9]+) errors.*/\1 error(s)/p' <<<"$mypyres")\
</summary>

\`\`\`diff
$(sed -r -e '/^src\//! d' -e 's/^[^ ]+ error/- &/' -e 's/^[^-]/# &/' <<<"$mypyres")
\`\`\`
</details>"
