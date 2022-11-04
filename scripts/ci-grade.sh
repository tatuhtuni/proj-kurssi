#!/usr/bin/env bash

blackres=$(black src --diff -q)
echo "<details><summary>black diff: -$(echo "$blackres" | grep -cP '^-(?!-)') +$(echo "$blackres" | grep -cP '^\+(?!\+)')</summary>

\`\`\`diff
$blackres
\`\`\`
</details>"

lintres=$(pylint src --exit-zero)
echo "<details><summary>pylint score: $(echo "$lintres" | tail -n 1 | grep -oP '(?<=at )\d+(?:\.\d+)?/\d+')</summary>

\`\`\`diff
$(echo "$lintres" | sed -E '/\.py:[:0-9]+\s+[CE]/s/^/- /' | sed -E '/\.py:[:0-9]+\s+W/s/^/! /' | sed -E '/^([-!*])/!s/^/# /' | head -n -3)
\`\`\`
</details>"
