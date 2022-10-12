# Interfaces

## PsqlWrapper

Constructor is the only currently required interface.

### `Wrapper(db_name: str, hook_f: Callable[str, str], parser: PsqlParser)`

`db_name` is name of the database psql needs to access

`hook_f` is the function that the wrapper sends prettified user input & program output to, and from where it gets its helpful messages to inject before next prompt.

`parser` is an implementation of PsqlParser interface. 

## PsqlParser

### `parse_for_a_new_prompt(psql: str) -> List[str]`
Parse for an empty prompt, usually to detect when a query evaluation has ended.

Returns an empty list if none found.

`psql` is the psql REPL log to be inspected.

Returns an empty list if no prompt is found. Returns prompt text ("dbname=>" or "dbname=#") if found.

### `parse_first_found_stmt(psql:str) -> List[str]`
Parse for the content between two prompts. Returns an empty list if there is no statement or there was an error.

Expected to be deprecated when detecting multiple statements is implemented.

`psql` is the psql REPL log to be inspected.

Returns an empty list if no SQL statement is found. If found, returns a list of strings containing complete statement ("SELECT ...;") if flattened.
