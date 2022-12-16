# General information

## Installing pg4n

`pip install pg4n`

## Updating pg4n

`pip install --upgrade pg4n`

## Using pg4n

Pg4n only injects messages for the user, and is otherwise completely transparent. For this reason, usage is identical to `psql` usage. [PostgreSQL: Documentation: 14: psql](https://www.postgresql.org/docs/14/app-psql.html)

## Semantic errors detected

- Comparison between different domains (Error 31 per Brass and Goldberg, 2005) (`CmpDomainChecker`)
- Condition in the subquery can be moved up (Error 30 per Brass and Goldberg, 2005) (`SubquerySelectChecker`)
- DISTINCT in SUM and AVG (Error 33 per Brass and Goldberg, 2005) (`SumDistinctChecker`)
- Implied expression (Table already enforces the given expression) (`ImpliedExpressionChecker`)
- Inconsistent expression (Error 1 per Brass and Goldberg, 2005) (`InconsistentExpressionChecker`)
- ORDER BY in a subquery (`SubqueryOrderByChecker`)
- SELECT in subquery uses no tuple variable of subquery (Error 29 per Brass and Goldberg, 2005) (`SubquerySelectChecker`)
- Strange HAVING (Error 32 per Brass and Goldberg, 2005) (`StrangeHavingChecker`)
- Wildcards without LIKE (Error 34 per Brass and Goldberg, 2005) (`EqWildcardChecker`)
