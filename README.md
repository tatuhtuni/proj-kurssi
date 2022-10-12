# postgresql-for-novices

## Notes for developers

### Poetry

This project uses [Poetry](https://python-poetry.org/) for packaging. Although one should refer to [Poetry docs](https://python-poetry.org/docs/) for a thorough introduction, here's a short summary of the intended workflow with Poetry:

- To make VS Code use Poetry's virtual environment, type `poetry env info`, copy virtual environment executable path, press F1 and type `Python: Select Interpreter` > `Enter interpreter path...` > paste path and press `<ENTER>`.
- To install all dependencies and the application, type `poetry install`.
- To add/remove a dependency, type `poetry add`/`poetry remove`.
- To execute a command from within virtual environment shell, type `poetry run <cmd>`.
- To enter a shell session within the Poetry virtual environment, type `poetry shell`.

### Running tests

Having PostgreSQL running on port 5432, do `poetry run pytest` (or, if on port x, just do `PGPORT=x poetry run pytest`).

To get a similar instance as with GitHub Actions workflow:<br>
`docker run --rm -P -p 127.0.0.1:5432:5432 --name pg -e POSTGRES_PASSWORD=postgres -d postgres:14.5-alpine`
