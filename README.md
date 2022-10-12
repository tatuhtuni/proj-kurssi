# postgresql-for-novices

[Documentation](https://project-c-sql.github.io/postgresql-for-novices/)

## Notes for developers

### Imports

You must run the program as a module, e.g., `python -m src.pg4n.main`, so that the imports work.

### Poetry

This project uses [Poetry](https://python-poetry.org/) for packaging. Although one should refer to [Poetry docs](https://python-poetry.org/docs/) for a thorough introduction, here's a short summary of the intended workflow with Poetry:

- To make VS Code use Poetry's virtual environment, type `poetry env info`, copy virtual environment executable path, press F1 and type `Python: Select Interpreter` > `Enter interpreter path...` > paste path and press `<ENTER>`.
- To install all dependencies and the application, type `poetry install`.
- To add/remove a dependency, type `poetry add <dep>`/`poetry remove <dep>`.
- To execute a command from within virtual environment shell, type `poetry run <cmd>`.
- To enter a shell session within the Poetry virtual environment, type `poetry shell`.

### Running tests

Having PostgreSQL running on port 5432, do `poetry run pytest` (or, if on port x, just do `PGPORT=x poetry run pytest`).

To get a similar instance as with GitHub Actions workflow:<br>
`docker run --rm -P -p 127.0.0.1:5432:5432 --name pg -e POSTGRES_PASSWORD=postgres -d postgres:14.5-alpine`

### Building documents

1. If `docs/api` is not up-to-date or doesn't exist, run:<br>`poetry run sphinx-apidoc -f -o docs/api src/pg4n '*/test_*'`
2. To generate the documentation:<br>`poetry run sphinx-build -b html docs docs/build`

Note that the GitHub Pages site is only updated on pushes to `main` branch.
