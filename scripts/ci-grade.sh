#!/usr/bin/env bash

#
# print format infos
#

blackurl="[black](https://black.readthedocs.io/en/stable/usage_and_configuration/the_basics.html#diffs)"
pylinturl="[pylint](https://pylint.pycqa.org/en/latest/user_guide/usage/output.html#source-code-analysis-section)"
mypyurl="[mypy](https://mypy.readthedocs.io/en/stable/error_codes.html#error-codes)"
isorturl="[isort](https://pycqa.github.io/isort/)"
echo "ℹ Tool info: $blackurl • $pylinturl • $mypyurl • $isorturl"

#
# black
#

runblack() {
    blackres=$(black src --diff -q)
    echo "<details><summary>black: \
-$(grep -cP '^-(?!-)' <<<"$blackres") \
+$(grep -cP '^\+(?!\+)' <<<"$blackres")</summary>

\`\`\`diff
$blackres
\`\`\`
</details>"
}

#
# pylint
#

runpylint() {
    lintres=$(pylint src --exit-zero -sn)
    lintscore=$(for x in I R C W E F; do
        n=$(echo "$lintres" | grep -cP '^\S+\s+'$x)
        [ $n != 0 ] && echo $n$x
    done)
    [ "$lintscore" = "" ] && lintscore="OK"
    echo "<details><summary>pylint: "$lintscore"</summary>

\`\`\`diff
$(sed -E -e '/\.py:[:0-9]+\s+[CEF]/s/^/- /' \
        -e '/\.py:[:0-9]+\s+W/s/^/! /' \
        -e '/^([-!*])/! s/^/# /' <<<"$lintres")
\`\`\`
</details>"
}

#
# mypy
#

runmypy() {
    mypyres=$(mypy --non-interactive --install-types --ignore-missing-imports --strict --show-error-codes --show-error-context src 2>&1)
    echo "<details><summary>mypy: $(sed -nr 's/^Found (.+)/\1/p' <<<"$mypyres")\
</summary>

\`\`\`diff
$(sed -r -e '/^src\//! d' -e 's/^[^ ]+ error/- &/' -e 's/^[^-]/# &/' <<<"$mypyres")
\`\`\`
</details>"
}

#
# isort
#

runisort() {
    isortres=$(isort src --diff --profile black)
    echo "<details><summary>isort: \
-$(grep -cP '^-(?!-)' <<<"$isortres") \
+$(grep -cP '^\+(?!\+)' <<<"$isortres")</summary>

\`\`\`diff
$isortres
\`\`\`
</details>"
}

# run them in parallel
cat <(runblack) <(runpylint) <(runmypy) <(runisort)
