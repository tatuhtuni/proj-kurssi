# Architecture

The psql wrapper module has following requirements:
- Handle a user's psql session transparently with select injections to psql output.
- Get query statements and their results, and sanitize them for syntactic analysis
