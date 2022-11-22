#!/bin/bash

# This script generates the configuration values (which correspond to warning
# names).
# This script must be run from the project root

src_dir=src/pg4n

readarray -d '' files < <(find "${src_dir}/" -maxdepth 1 -type f -iname '*checker.py' -print0)

comment=

for f in "${files[@]##${src_dir}/}"
do
    echo "$f" | sed -E 's/(^|_)(\w)/\U\2/g' | \
                sed -E 's/^(.*)Checker\.py$/\1/' | \
                sed -E 's/^.*$/\0: bool/'
done
