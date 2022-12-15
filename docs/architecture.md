# Architecture

![Program architecture sketch](./architecture.jpg)

## Backend

### ErrorFormatter

### PsqlConnInfo

`PsqlConnInfo` fetches PostgresSQL connection info by running a `psql` command with given arguments (usually same arguments as with what the main `psql` process was called with).

### QEPParser

### SemanticRouter

### SQLParser

### Analysis modules

#### CmpDomainChecker

#### EqWildcardChecker

#### ImpliedExpressionChecker

#### InconsistentExpressionChecker

#### StrangeHavingChecker

#### SubqueryOrderByChecker

#### SubquerySelectChecker

#### SumDistinctChecker

### Program configuration

#### ConfigParser

#### ConfigReader

#### ConfigValues

## Frontend

Frontend handles user's psql session completely transparently via `PsqlWrapper`, although also injecting insightful messages regarding user's semantic errors into the terminal output stream. It parses user's SQL queries via `PsqlParser` for consumption in the backend.

### PsqlParser

`PsqlParser` uses `pyparsing` parser combinator library to provide parsing functions for
- checking for non-obvious Return presses (`output_has_magical_return`)
- checking if given string has a new prompt (e.g `=> `) (`output_has_new_prompt`)
- parsing a new prompt and everything that precedes it in a string, to allow easy message injection (`parse_new_prompt_and_rest`)
- parsing last SQL SELECT query in a string (`parse_last_stmt`)
- parsing `psql --version` output for version number (`parse_psql_version`)
- parsing syntax errors (`ERROR:` .. `^`) (`parse_syntax_error`)

Parsing rules common to more than 1 of these functions are listed in `PsqlParser` body, but otherwise rules are inside respective functions.

### PsqlWrapper

`PsqlWrapper` is responsible for spawning and intercepting the user-interfacing `psql` process. `pexpect` library allows both spawning and intercepting the terminal control stream. `pyte` library keeps track of current terminal display.

Overall working logic is handled by `_check_and_act_on_repl_output`, where it can be seen that queries are checked for every time user presses Return. If `PsqlParser` finds an SQL SELECT query, it's passed to `SemanticRouter` for further analysis, and any insightful message returned is saved for later. Once all query results have been printed, and a new prompt (e.g `..=> `) is going to be printed next per `latest_output` parameter, the wrapper injects the returned message. If results included `ERROR:` .. `^`, it is sent to syntax error analysis, and any returned message will be injected immediately.

`PsqlWrapper` also checks `psql` version info and checks it against `PsqlWrapper.supported_psql_versions`.
