#!/usr/bin/env bash

#
# print format infos
#

blackurl="[black](https://black.readthedocs.io/en/stable/usage_and_configuration/the_basics.html#diffs)"
pylinturl="[pylint](https://pylint.pycqa.org/en/latest/user_guide/usage/output.html#source-code-analysis-section)"
mypyurl="[mypy](https://mypy.readthedocs.io/en/stable/error_codes.html#error-codes)"
isorturl="[isort](https://pycqa.github.io/isort/)"
res="ℹ Tool info: $blackurl • $pylinturl • $mypyurl • $isorturl"

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

#
# output
#

# run them in parallel
res="$res $(cat <(runblack) <(runpylint) <(runmypy) <(runisort))"

# if more than 65536 characters, use a gist
if [ "$GITHUB_TOKEN" != "" && ${#res} -gt 65536 ]; then
    gistid="CI grade for $GITHUB_REPOSITORY/$(git rev-parse --abbrev-ref HEAD)"
    gistbody='{
  "description": "'$gistid'",
  "public": false,
  "files": {
    "grade.md": {
      "content": '"$(jq -Rs . <<<"$res")"'
    }
  }
}'

    gisturl=$(curl -s -l -X POST -H "Content-Type: application/json" -H "Authorization: Bearer $GITHUB_TOKEN" \
        https://api.github.com/gists -d @- <<<"$gistbody" | jq -r '.html_url')
    if [ "$gisturl" = "null" ]; then
        echo '⚠ Output is truncated - run `scripts/ci-grade.sh` locally to see details'
    else
        echo "⚠ Output is truncated - see [gist]($gisturl)"

        # delete other gists that have the same description
        curl -s -l -X GET -H "Content-Type: application/json" -H "Authorization: Bearer $GITHUB_TOKEN" \
            https://api.github.com/gists | jq -r '.[] | select(.description=="'"$gistid"'" and .html_url!="'"$gisturl"'") | .url' |
            xargs -I % curl -s -l -X DELETE -H "Content-Type: application/json" -H "Authorization: Bearer $GITHUB_TOKEN" %

    fi
    echo "$(sed -e '/^```diff$/ i **Removed - see warning above**' -e '/^```diff$/,/^```$/d' <<<"$res")"
else
    echo "$res"
fi
