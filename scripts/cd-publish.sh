#!/usr/bin/env bash

# https://github.com/python-poetry/poetry/issues/3670#issuecomment-776462445

if [[ "$BRANCH_NAME" == *main ]]; then
  poetry version --short
else
  poetry version $(poetry version --short)-dev.$GITHUB_RUN_NUMBER
  poetry version --short
fi
echo -e "\nPublishing to version ref '$(poetry version --short)'...\n\n"
poetry publish --no-interaction --build -u $PYPI_USER -p $PYPI_PASS
