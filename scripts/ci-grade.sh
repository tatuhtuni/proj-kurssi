#!/usr/bin/env bash

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
$(echo "$lintres" | sed -E '/\.py:[:0-9]+\s+[CE]/s/^/- /' | sed -E '/\.py:[:0-9]+\s+W/s/^/! /' | sed -E '/^([-!*])/!s/^/# /' | head -n -3)
\`\`\`
</details>"
