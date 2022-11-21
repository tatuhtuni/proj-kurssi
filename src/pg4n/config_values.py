from typing import TypedDict


# Contains all the key-value pairs meaningful in a config file.
class ConfigValues(TypedDict):
    ImpliedExpression: bool
    CmpDomains: bool
    InnerOrderBy: bool
    SubquerySelect: bool
