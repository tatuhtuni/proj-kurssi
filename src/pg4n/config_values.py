from typing import TypedDict


# Contains all the key-value pairs meaningful in a config file.
class ConfigValues(TypedDict):
    SubquerySelect: bool
    SumDistinct: bool
    StrangeHaving: bool
    CmpDomain: bool
    SubqueryOrderBy: bool
    ImpliedExpression: bool
    EqWildcard: bool
