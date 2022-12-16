# Maintaining pg4n

## Backend

### Implementing semantic analysis modules

Most of the semantic analysis modules have been implemented by parsing the SQL with `sqlglot`. Only few of the Checker classes use the query evaluation plan (QEP).

### Thoughts on syntax error analysis

`PsqlWrapper` has a `hook_syntax_f` function parameter (of type `str -> str`), which is called with `PsqlParser.parse_syntax_error`-produced string, ideally "ERROR: ... ^", which, to our understanding, will always include the whole result, and also the SQL query itself (as caret will point to it), so it does not have to be parsed separately. A syntax error analysis component should then return a message string, and it will be displayed in exactly the same way as semantic error strings.

## Frontend

### Fixing parsing/interception bugs

If improper parsing is suspected, turn `PsqlParser.debug` to `True`, that way all `ParserException.explain`s are saved into `psqlparser.log`. These exceptions are verbose, and will require fair amount of sifting. If improper interception by `PsqlWrapper` is suspected, turn `PsqlWrapper.debug` to `True`, to have current `pyte` display contents copied to `pyte.screen` on every update, and `pexpect` terminal control stream appended into `psqlwrapper.log`. Any wrapper issues are expected to be quite obtuse to fix, as they likely are `pexpect` or `pyte` library issues.

`pyparsing` documentation is available on [Welcome to PyParsing’s documentation!](https://pyparsing-docs.readthedocs.io/en/latest/)

### Extending parsers

To our knowledge, there are no interdependencies with the parser functions, so they should be able to be extended as needed, and new ones added.

## Known limitations

- `pexpect` does not seem to handle all terminal traffic. `pyte` and user terminal occasionally disagree on contents when user uses ctrl-R to fetch past queries, which prevents screenscraping SQL query properly. `pyte` also disagrees on display contents when exiting a separate query results screen, but this has no impact on `pg4n` performance.

- Semantic error modules are expected to produce false negatives.
