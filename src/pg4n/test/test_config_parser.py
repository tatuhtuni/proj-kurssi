from tempfile import TemporaryFile
from typing import Optional

from ..config_parser import ConfigParser
from ..config_values import ConfigValues


def test_parse():
    CONFIG1 = """ImpliedExpression 0
CmpDomains false
InnerOrderBy true
ImpliedExpression yes

INNERORDERBY true
innerorderby true


iNnErOrDeRbY true

SubquerySelect 0
SubquerySelect 0
"""

    with TemporaryFile(buffering=0) as tmp_file:
        tmp_file.write(bytes(CONFIG1, "utf-8"))
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
