import sys
import re
from dataclasses import dataclass
from typing import Optional

import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.postgres import Postgres
import psycopg


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
        Throws sqlglot.ParseError on invalid sql.
        """

        return sqlglot.parse(sql, read=self.dialect)

    def parse_one(self, sql: str) -> sqlglot.exp.Expression:
        """
        Parses the first statement in 'sql'.
        Does not validate that 'sql' contains only 1 statement!
        'sql' should be a postgresql statement.
        The trailing ';' in 'sql' is optional.
        Throws sqlglot.ParseError on invalid sql.
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
        #        print(f"column_names: {column_names}")

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
        #        print(f"type_names: {type_names}")

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
        character_matcher = re.compile(r"^(?:character|char)\(\s*(\d+)\s*\)$")
        varchar_matcher = re.compile(
            r"^(?:(?:character varying)|varchar)\(\s*(\d+)\s*\)$"
        )
        numeric_matcher = re.compile(
            r"^(?:numeric|decimal)\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\)$"
        )

        for type_name in type_names:
            if type_name == "integer":
                converted_types.append(PostgreSQLDataType("INT", None, None))
            elif type_name == "character":
                converted_types.append(PostgreSQLDataType("CHAR", None, None))
            elif type_name == "char":
                converted_types.append(PostgreSQLDataType("CHAR", None, None))
            elif match := character_matcher.match(type_name):
                conv_type = PostgreSQLDataType(
                    f"CHAR({match.group(1)})", int(match.group(1)), None
                )
                converted_types.append(conv_type)

            elif match := varchar_matcher.match(type_name):
                conv_type = PostgreSQLDataType(
                    f"VARCHAR({match.group(1)})", int(match.group(1)), None
                )
                converted_types.append(conv_type)

            elif match := numeric_matcher.match(type_name):
                num_groups = len(match.groups())
                if num_groups == 1:
                    conv_type = PostgreSQLDataType(
                        f"NUMERIC({match.group(1)})", int(match.group(1)), None
                    )
                    converted_types.append(conv_type)

                elif num_groups == 2:
                    name = f"NUMERIC({match.group(1)},{match.group(2)})"
                    conv_type = PostgreSQLDataType(
                        name, int(match.group(1)), int(match.group(2))
                    )
                    converted_types.append(conv_type)
                else:
                    # TODO: proper error handling
                    print(
                        f"unrecognized number '{num_groups}' of arguments for numeric() column type",
                        file=sys.stderr,
                    )
                    exit(1)
            else:
                # TODO: proper error handling
                print(
                    f"unable to convert from internal type '{type_name}' to declared type",
                    file=sys.stderr,
                )
                exit(1)

        return converted_types
