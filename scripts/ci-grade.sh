#!/usr/bin/env bash

blackurl="[black](https://black.readthedocs.io/en/stable/usage_and_configuration/the_basics.html#diffs)"
pylinturl="[pylint](https://pylint.pycqa.org/en/latest/user_guide/usage/output.html#source-code-analysis-section)"
echo "ℹ Format info: $blackurl • $pylinturl"

blackres=$(black src --diff -q)
echo "<details><summary>black diff: -$(echo "$blackres" | grep -cP '^-(?!-)') +$(echo "$blackres" | grep -cP '^\+(?!\+)')</summary>

\`\`\`diff
$blackres
\`\`\`
</details>"

lintres=$(pylint src --exit-zero -sn)
lintscore=$(echo -e 'I\nR\nC\nW\nE\nF' |
    xargs -I? bash -c 'echo "$(echo "$1" | grep -cP "^\S+\s+?")?"' -- "$lintres" |
    grep -v '^0' | tr '\n' ' ' | xargs echo -n)
if [[ "$lintscore" = "" ]]; then
    lintscore="OK"
fi
echo "<details><summary>pylint score: $lintscore</summary>

\`\`\`diff
$(echo "$lintres" | sed -E '/\.py:[:0-9]+\s+[CEF]/s/^/- /' | sed -E '/\.py:[:0-9]+\s+W/s/^/! /' | sed -E '/^([-!*])/!s/^/# /' | head -n -3)
\`\`\`
</details>"
