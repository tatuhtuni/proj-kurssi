from tempfile import TemporaryFile
from typing import Optional

from ..config_parser import ConfigParser
from ..config_values import ConfigValues


def test_parse():
    CONFIG = """ImpliedExpression 0

######################
 # # #
######   # # #
              ##       3              ## jkelm el el e
CmpDomain false
SubqueryOrderBy true
ImpliedExpression yes

SUBQUERYORDERBY true
subqueryorderby true


SuBqUeRyOrDeRbY true

# Gives too many false positives
SubquerySelect 0
SubquerySelect 0
#
"""

    with TemporaryFile(buffering=0) as tmp_file:
        tmp_file.write(bytes(CONFIG, "utf-8"))
        tmp_file.flush()
        tmp_file.seek(0)

        parser = ConfigParser(tmp_file)
        config_values: Optional[ConfigValues] = parser.parse()
        assert config_values is not None

        try:
            from pprint import pprint
            pprint(config_values)
            assert config_values["ImpliedExpression"] == True
            assert config_values["CmpDomain"] == False
            assert config_values["SubqueryOrderBy"] == True
            assert config_values["SubquerySelect"] == False
        except Exception as e:
            assert False, f"{e}"


# TODO: The actual test
# def test_multiple_option_definition_warings():

# This is the config file
#     CONFIG1 = """ImpliedExpression false
# ImpliedExpression 1
# CmpDomain false
# SubqueryOrderBy true
# SubqueryOrderBy true
# SubqueryOrderBy true
# SubqueryOrderBy true
# SubqueryOrderBy true

# SubqueryOrderBy true
# SubquerySelect yes
# """

# This is the matching output of running the program that reads only 1 config
# file CONFIG
# configuration file output is not tested because its system dependent
# warning_matcher = re.compile(\
# """
# line 4: 'SubqueryOrderBy true'
# line 5: 'SubqueryOrderBy true'
# line 6: 'SubqueryOrderBy true'
# line 7: 'SubqueryOrderBy true'
# line 8: 'SubqueryOrderBy true'
# line 10: 'SubqueryOrderBy true'
# in configuration file: '.*'
# warning: option 'ImpliedExpression' is set multiple times
# line 1: 'ImpliedExpression false'
# line 2: 'ImpliedExpression 1'
# in configuration file: '.*'
# )
