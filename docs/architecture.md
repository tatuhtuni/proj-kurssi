# Architecture

![Program architecture sketch](./architecture.jpg)

## Backend

### ErrorFormatter

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

### PsqlConnInfo

### PsqlParser

### PsqlWrapper

The psql wrapper module has following requirements:
- Handle a user's psql session transparently with select injections to psql output.
- Get query statements and their results, and sanitize them for syntactic analysis
