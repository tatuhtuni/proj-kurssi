import re
import sys
from dataclasses import dataclass
from typing import Optional

import psycopg
import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.postgres import Postgres


@dataclass(frozen=True)
class PostgreSQLDataType:
    name: str
    digits: Optional[int]
    precision: Optional[int]


@dataclass(frozen=True)
class Column:
    name: str
    type: PostgreSQLDataType
    table: str


class SqlParser:
    # Patches the postgres dialect to recognize bpchar
    Postgres.Tokenizer.KEYWORDS["BPCHAR"] = sqlglot.TokenType.CHAR

    def __init__(self, db_connection: psycopg.Connection):
        self.dialect: str = "postgres"
        self.db_connection: psycopg.Connection = db_connection

    def parse(self, sql: str) -> list[sqlglot.exp.Expression]:
        """
        Parses all the statements in 'sql'.

        'sql' should be a string of one or more postgresql statements
        (delimited by ';'). The last ';' in 'sql' is optional.
        Raises whichever Exception sqlglot wants to throw on invalid sql.
        """

        return sqlglot.parse(sql, read=self.dialect)

    def parse_one(self, sql: str) -> sqlglot.exp.Expression:
        """
        Parses the first statement in 'sql'.
        Does not validate that 'sql' contains only 1 statement!
        'sql' should be a postgresql statement.
        The trailing ';' in 'sql' is optional.
        Raises whichever Exception sqlglot wants to throw on invalid sql.
        """

        return sqlglot.parse_one(sql, read=self.dialect)

    def get_query_columns(self, parsed_sql: exp.Expression) -> list[Column]:
        """
        Gets all columns from all tables mentioned in parsed_sql.
        """

        table_names = self.find_all_table_names(parsed_sql)

        columns = []
        for table_name in table_names:
            table_columns = self._get_columns(table_name)
            columns.extend(table_columns)

        return columns

    @staticmethod
    def get_root_node(node: exp.Expression) -> exp.Expression:
        """
        Finds the root node (probably exp.Select) from node.
        """
        root_node = node

        while True:
            new_root = root_node.find_ancestor(exp.Expression)
            if new_root is None:
                break
            root_node = new_root
        return root_node

    @staticmethod
    def find_all_table_names(parsed_sql: exp.Expression) -> list[str]:
        """
        Finds all unique table names in 'parsed_sql'.
        Any table name with an alias is listed without the alias
        (e.g.: FROM <table> AS <alias>)
        Also removes duplicate table names from the result.
        """
        table_names = []
        tables = parsed_sql.find_all(exp.Table)
        # table.this.this: first .this gets the exp.Identifier of the table and
        # second .this gets the name of the identifier.
        table_names = [table.this.this for table in tables]

        unique_table_names = list(set(table_names))

        return unique_table_names

    @staticmethod
    def get_column_name_from_column_expression(column_expression: exp.Column) -> str:
        """
        Returns the column name of column expression ast node.
        """
        return column_expression.find(exp.Identifier).this

    @staticmethod
    def find_where_predicates(root: exp.Expression) -> list[exp.Predicate]:
        """
        Finds all Predicate nodes inside Where statements from some root node
        (usually the whole parsed output from sqlparser.parse_one()).
        This function takes care to not introduce any duplicate Predicate's in
        case of nested Where statements.
        """
        predicates = []

        where_statements = root.find_all(exp.Where)

        toplevel_where_statements = list(
            filter(lambda x: not x.find_ancestor(exp.Where), where_statements)
        )

        for where_statement in toplevel_where_statements:
            for predicate in where_statement.find_all(exp.Predicate):
                predicates.append(predicate)

        return predicates

    def _get_columns(self, table_name: str) -> list[Column]:
        """
        Gets the columns from db 'db_name' table 'table_name'.
        """
        names = self._get_column_names(table_name)
        types = self._get_column_types(table_name, names)

        assert len(names) == len(types)

        columns = []
        i = 0
        for name in names:
            columns.append(Column(name, types[i], table_name))
            i += 1

        return columns

    def _get_column_names(self, table_name: str) -> list[str]:
        """
        Requires that 'table_name' exists.
        Extracts columns of the table by running postgres specific metadata
        query.
        """

        statement = (
            "SELECT column_name "
            "FROM information_schema.columns "
            f"WHERE table_name = '{table_name}';"
        )

        column_names = []
        with self.db_connection.cursor() as cursor:
            cursor.execute(statement)
            column_names = cursor.fetchall()
            self.db_connection.rollback()

        # transforms 1-element tuples to just list of elements
        column_names = [x[0] for x in column_names]

        return column_names

    def _get_column_types(
        self, table_name: str, columns: list[str]
    ) -> list[PostgreSQLDataType]:
        # sql adapted from: https://stackoverflow.com/a/20194807/13540518
        column_type_statement_prefix = f"""
SELECT
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS "data_type"
FROM
    pg_catalog.pg_attribute AS a
WHERE
    a.attnum > 0
    AND NOT a.attisdropped
    AND a.attrelid = (
        SELECT c.oid
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = '{table_name}'
                AND pg_catalog.pg_table_is_visible(c.oid)
                AND a.attname IN ("""

        if len(columns) == 0:
            return ""

        in_condition = ""
        for col in columns[:-1]:
            in_condition += f"'{col}', "
        in_condition += f"'{columns[-1]}')\n    );"
        column_type_statement = column_type_statement_prefix + in_condition

        type_names = []
        with self.db_connection.cursor() as cursor:
            cursor.execute(column_type_statement)
            type_names = cursor.fetchall()
            self.db_connection.rollback()

        # transforms 1-element tuples to just list of elements
        type_names = [x[0] for x in type_names]

        parseable_type_names = self._convert_from_internal_types(type_names)

        return parseable_type_names

    def _convert_from_internal_types(
        self, type_names: list[str]
    ) -> list[PostgreSQLDataType]:
        """
        Postgresql has internal type names such as: character or character(x).
        sqlglot can only parse the types in the form they are declared (atleast
        in column definitions inside CREATE TABLE) like: CHAR or CHAR(x).
        This function converts from the internal to the declared form.
        """

        converted_types = []

        #  +----------------------------------------------------------+
        #  | TRIVIAL_MATCHERS                                         |
        #  +----------------------------------------------------------+
        int_matcher = re.compile(r"^(?:int|integer|int4)$", re.IGNORECASE)
        smallint_matcher = re.compile(r"^(?:smallint|int2)$", re.IGNORECASE)
        bigint_matcher = re.compile(r"^(?:bigint|int8)$", re.IGNORECASE)
        bool_matcher = re.compile(r"^(?:boolean|bool)$")
        real_matcher = re.compile(r"^(?:real|float4)$")
        bytea_matcher = re.compile(r"^(?:bytea)$")
        money_matcher = re.compile(r"^(?:money)$")
        date_matcher = re.compile(r"^(?:date)$")
        cidr_matcher = re.compile(r"^(?:cidr)$")
        inet_matcher = re.compile(r"^(?:inet)$")
        json_matcher = re.compile(r"^(?:json)$")
        jsonb_matcher = re.compile(r"^(?:jsonb)$")
        macaddr_matcher = re.compile(r"^(?:macaddr)$")
        macaddr8_matcher = re.compile(r"^(?:macaddr8)$")
        double_precission_matcher = re.compile(r"^(?:(?:double precision)|float8)$")
        text_matcher = re.compile(r"^(?:text)$")
        tsquery_matcher = re.compile(r"^(?:tsquery)$")
        tsvector_matcher = re.compile(r"^(?:tsvector)$")
        uuid_matcher = re.compile(r"^(?:uuid)$")
        xml_matcher = re.compile(r"^(?:xml)$")
        circle_matcher = re.compile(r"^(?:circle)$")
        box_matcher = re.compile(r"^(?:box)$")
        path_matcher = re.compile(r"^(?:path)$")
        line_matcher = re.compile(r"^(?:line)$")
        lseg_matcher = re.compile(r"^(?:lseg)$")
        point_matcher = re.compile(r"^(?:point)$")
        polygon_matcher = re.compile(r"^(?:polygon)$")
        interval_matcher = re.compile(r"^(?:interval)$")

        #  +----------------------------------------------------------+
        #  | OPT_PRECISSION_MATCHERS                                  |
        #  +----------------------------------------------------------+
        character_matcher = re.compile(r"^(?:character|char)(?:\(\s*(\d+)\s*\))?$")
        bit_matcher = re.compile(r"^(?:bit)(?:\(\s*(\d+)\s*\))?$")
        varchar_matcher = re.compile(
            r"^(?:(?:character varying)|varchar)(?:\(\s*(\d+)\s*\))?$"
        )
        varbit_matcher = re.compile(r"^(?:(?:bit varying)|varbit)(?:\(\s*(\d+)\s*\))?$")

        timestamp_matcher = re.compile(
            r"^timestamp(?:\(\s*(\d+)\s*\))?(?: without time zone)?$"
        )
        timestamptz_matcher = re.compile(
            r"^(?:timestamp|timestamptz)(?:\(\s*(\d+)\s*\))?(?: with time zone)?$"
        )
        time_matcher = re.compile(r"^time(?:\(\s*(\d+)\s*\))?(?: without time zone)?$")
        timetz_matcher = re.compile(
            r"^(?:time|timetz)(?:\(\s*(\d+)\s*\))?(?: with time zone)?$"
        )

        #  +----------------------------------------------------------+
        #  | COMPLEX_MATCHERS                                         |
        #  +----------------------------------------------------------+
        numeric_matcher = re.compile(
            r"^(?:numeric|decimal)(?:\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\))?$"
        )

        for type_name in type_names:
            #  +----------------------------------------------------------+
            #  | TRIVIAL_CASES                                            |
            #  +----------------------------------------------------------+
            if match := int_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("INT", None, None))
            elif match := smallint_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("SMALLINT", None, None))
            elif match := bigint_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("BIGINT", None, None))
            elif match := bool_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("BOOL", None, None))
            elif match := real_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("REAL", None, None))
            elif match := bytea_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("BYTEA", None, None))
            elif match := money_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("MONEY", None, None))
            elif match := date_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("DATE", None, None))
            elif match := cidr_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("CIDR", None, None))
            elif match := inet_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("INET", None, None))
            elif match := json_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("JSON", None, None))
            elif match := jsonb_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("JSONB", None, None))
            elif match := macaddr_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("MACADDR", None, None))
            elif match := macaddr8_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("MACADDR8", None, None))
            elif match := double_precission_matcher.match(type_name):
                converted_types.append(
                    PostgreSQLDataType("DOUBLE_PRECISSION", None, None)
                )
            elif match := text_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("TEXT", None, None))
            elif match := tsquery_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("TSQUERY", None, None))
            elif match := tsvector_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("TSVECTOR", None, None))
            elif match := uuid_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("UUID", None, None))
            elif match := xml_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("XML", None, None))
            elif match := circle_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("CIRCLE", None, None))
            elif match := box_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("BOX", None, None))
            elif match := path_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("PATH", None, None))
            elif match := line_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("LINE", None, None))
            elif match := lseg_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("LSEG", None, None))
            elif match := point_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("POINT", None, None))
            elif match := polygon_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("POLYGON", None, None))
            elif match := interval_matcher.match(type_name):
                converted_types.append(PostgreSQLDataType("INTERVAL", None, None))

            #  +----------------------------------------------------------+
            #  | OPT_PRECISSION_CASES                                     |
            #  +----------------------------------------------------------+
            elif match := character_matcher.match(type_name):
                converted_types.append(
                    self._eval_opt_precission_field_match(match, "CHAR")
                )
            elif match := bit_matcher.match(type_name):
                converted_types.append(
                    self._eval_opt_precission_field_match(match, "BIT")
                )
            elif match := varchar_matcher.match(type_name):
                converted_types.append(
                    self._eval_opt_precission_field_match(match, "VARCHAR")
                )
            elif match := varbit_matcher.match(type_name):
                converted_types.append(
                    self._eval_opt_precission_field_match(match, "VARBIT")
                )
            elif match := timestamp_matcher.match(type_name):
                converted_types.append(
                    self._eval_opt_precission_field_match(match, "TIMESTAMP")
                )
            elif match := timestamptz_matcher.match(type_name):
                converted_types.append(
                    self._eval_opt_precission_field_match(match, "TIMESTAMPTZ")
                )
            elif match := time_matcher.match(type_name):
                converted_types.append(
                    self._eval_opt_precission_field_match(match, "TIME")
                )
            elif match := timetz_matcher.match(type_name):
                converted_types.append(
                    self._eval_opt_precission_field_match(match, "TIMETZ")
                )

            #  +----------------------------------------------------------+
            #  | COMPLEX_CASES                                            |
            #  +----------------------------------------------------------+
            elif match := numeric_matcher.match(type_name):
                if match.group(1) is None and match.group(2) is None:
                    conv_type = PostgreSQLDataType("DECIMAL", None, None)
                    converted_types.append(conv_type)
                elif match.group(1) is not None and match.group(2) is None:
                    conv_type = PostgreSQLDataType(
                        f"DECIMAL({match.group(1)})", int(match.group(1)), None
                    )
                elif match.group(1) is not None and match.group(2) is not None:
                    name = f"DECIMAL({match.group(1)},{match.group(2)})"
                    conv_type = PostgreSQLDataType(
                        name, int(match.group(1)), int(match.group(2))
                    )
                    converted_types.append(conv_type)
                else:
                    print(
                        f"unrecognized number type format '{type_name}' for numeric() column type",
                        file=sys.stderr,
                    )
                    raise Exception
            else:
                print(
                    f"unable to convert from internal type '{type_name}' to declared type",
                    file=sys.stderr,
                )
                raise Exception

        return converted_types

    def _eval_opt_precission_field_match(
        self, match: re.Match, type_name_prefix: str
    ) -> PostgreSQLDataType:
        """
        Evaluates a internal type of the form: '<typename>[ (p) ]'
        where p is optional precission field and returns a converted type used
        "normalized" in the context of this program's conventions.
        """
        if match.group(1) is None:
            return PostgreSQLDataType("TIMESTAMPTZ", None, None)
        return PostgreSQLDataType(
            f"{type_name_prefix}({match.group(1)})", int(match.group(1)), None
        )
