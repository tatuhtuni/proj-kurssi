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
CmpDomains false
InnerOrderBy true
ImpliedExpression yes

INNERORDERBY true
innerorderby true


iNnErOrDeRbY true

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
            assert config_values["ImpliedExpression"] == True
            assert config_values["CmpDomains"] == False
            assert config_values["InnerOrderBy"] == True
            assert config_values["SubquerySelect"] == False
        except Exception as e:
            assert False, f"{e}"


# TODO: The actual test
# def test_multiple_option_definition_warings():

# This is the config file
#     CONFIG1 = """ImpliedExpression false
# ImpliedExpression 1
# CmpDomains false
# InnerOrderBy true
# InnerOrderBy true
# InnerOrderBy true
# InnerOrderBy true
# InnerOrderBy true

# InnerOrderBy true
# SubquerySelect yes
# """

# This is the matching output of running the program that reads only 1 config
# file CONFIG
# configuration file output is not tested because its system dependent
# warning_matcher = re.compile(\
# """
# line 4: 'InnerOrderBy true'
# line 5: 'InnerOrderBy true'
# line 6: 'InnerOrderBy true'
# line 7: 'InnerOrderBy true'
# line 8: 'InnerOrderBy true'
# line 10: 'InnerOrderBy true'
# in configuration file: '.*'
# warning: option 'ImpliedExpression' is set multiple times
# line 1: 'ImpliedExpression false'
# line 2: 'ImpliedExpression 1'
# in configuration file: '.*'
# )
