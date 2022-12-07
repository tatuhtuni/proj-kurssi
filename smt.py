import sys
from pprint import pprint
from typing import Optional, Union, Type

import sqlparser
import sqlglot
import sqlglot.expressions as exp

from pysmt.shortcuts import (
        Symbol,
        Equals,
        Not,
        Or,
        And,
        GT,
        LT,
        NotEquals,
        Int,
        String,
        is_sat,
        simplify,
        )
from pysmt.typing import INT, STRING
import pysmt.type_checker

def putsep():
    print(80 * '=')

def handle_error(msg):
    print(msg, file=sys.stderr)
    exit(1)


def get_where_statements(sql: exp.Expression) -> list[exp.Where]:
    return sql.find_all(exp.Where)


OperandType = Union[exp.Column, exp.Literal]

def get_operands(predicate: exp.Predicate) -> tuple[OperandType, OperandType]:
    """
    Return type is tuple with len() == 2 of 2 OperandsType's but is too hard to
    annotate cleanly.
    """

    column_or_literals = list(predicate.find_all(exp.Column, exp.Literal))
    op1 = None
    op2 = None

    if isinstance(column_or_literals[0], exp.Column):
        op1 = column_or_literals[0].find(exp.Identifier)
    elif isinstance(column_or_literals[0], exp.Literal):
        op1 = column_or_literals[0]

    if isinstance(column_or_literals[1], exp.Column):
        op2 = column_or_literals[1].find(exp.Identifier)
    elif isinstance(column_or_literals[1], exp.Literal):
        op2 = column_or_literals[1]

    return (op1, op2)


def get_pysmt_literal_type(type_str: str) -> Union[Type[Int], Type[String]]:
    if type_str.isnumeric():
        return Int

    return String


def get_pysmt_symbol_type(id_name: str) -> Type:
    # TODO: Somehow get the column type, possibly with a database query.
    return INT


def make_pysmt_predicate_operands(predicate: exp.Predicate) -> list:
    pysmt_operands = []
    operands = get_operands(predicate)

    if isinstance(operands[0], exp.Identifier):
        id_name = operands[0].this
        symbol_type = get_pysmt_symbol_type(id_name)
        symbol = Symbol(id_name, symbol_type)
        pysmt_operands.append(symbol)

    elif isinstance(operands[0], exp.Literal):
        literal = operands[0].this
        pysmt_type = get_pysmt_literal_type(str(literal))
        if pysmt_type == Int:
            pysmt_operands.append(pysmt_type(int(literal)))
        elif pysmt_type == String:
            pysmt_operands.append(pysmt_type(str(literal)))
        else:
            handle_error(f"unknown pysmt literal type '{pysmt_type}'")
    else:
        handle_error(f"unable to make formula of predicate '{predicate}' with left operand '{operands[0]}' of type '{type(operands[0])}'")

    if isinstance(operands[1], exp.Identifier):
        id_name = operands[1].this
        symbol_type = get_pysmt_symbol_type(id_name)
        symbol = Symbol(id_name, symbol_type)
        pysmt_operands.append(symbol)

    elif isinstance(operands[1], exp.Literal):
        literal = operands[1].this
        pysmt_type = get_pysmt_literal_type(str(literal))
        if pysmt_type == Int:
            pysmt_operands.append(pysmt_type(int(literal)))
        elif pysmt_type == String:
            pysmt_operands.append(pysmt_type(str(literal)))
        else:
            handle_error(f"unknown pysmt literal type '{pysmt_type}'")
    else:
        handle_error(f"unable to make formula of predicate '{predicate}' with right operand '{operands[1]}' of type '{type(operands[1])}'")

    return pysmt_operands


def make_predicate_formula(predicate: exp.Predicate):
    formula = None

    if isinstance(predicate, exp.EQ):
        formula = Equals(*make_pysmt_predicate_operands(predicate))
    elif isinstance(predicate, exp.GT):
        formula = GT(*make_pysmt_predicate_operands(predicate))
    elif isinstance(predicate, exp.GTE):
        operands = get_operands(predicate)
        formula = Or(Equals(operands[0], operands[1]),
                     GT(operands[0], operands[1]))
    elif isinstance(predicate, exp.LT):
        formula = LT(*make_pysmt_predicate_operands(predicate))
    elif isinstance(predicate, exp.LTE):
        operands = get_operands(predicate)
        formula = Or(Equals(operands[0], operands[1]),
                     LT(operands[0], operands[1]))
    elif isinstance(predicate, exp.NEQ):
        formula = NotEquals(*make_pysmt_predicate_operands(predicate))
    # List of unhandled cases gotten from sqlglot repo root with:
    # grep -E '((Predicate)|(,\s*Predicate))' sqlglot/expressions.py
    #
    # class Is(Binary, Predicate):
    # class SubqueryPredicate(Predicate):
    # class All(SubqueryPredicate):
    # class Any(SubqueryPredicate):
    # class Exists(SubqueryPredicate):
    # class ILike(Binary, Predicate):
    # class Like(Binary, Predicate):
    # class SimilarTo(Binary, Predicate):
    # class Between(Predicate):
    # class In(Predicate):

    return formula


def get_formula(expression: exp.Expression):
    formula = None
    node = expression.find(exp.Connector, exp.Predicate)

    if node == None:
        return formula

    if isinstance(node, exp.And):
        formula = And(get_formula(node.left), get_formula(node.right))
    elif isinstance(node, exp.Or):
        formula = Or(get_formula(node.left), get_formula(node.right))
    elif isinstance(node, exp.Predicate):
        formula = make_predicate_formula(node)

    return formula


def is_satisfiable(sql: exp.Expression) -> bool:
    satisfiable = True
    formulas = []

    where_statements = get_where_statements(sql)

    for where_statement in where_statements:
        pprint(where_statement)
        putsep()

        formula = get_formula(where_statement)

        formulas.append(formula)

    print("is_tautology: %s" % (not is_sat(Not(formulas[0]))))

    return any(filter(lambda x: is_sat(x), formulas))


def main():
    sql_statement = \
"""
SELECT *
FROM customers
WHERE customer_id = 1 OR 100 = 100;"""

# """
# select *
# from orders
# where order_total_eur = order_total_eur OR
#       (order_total_eur = 0 AND order_total_eur = 100);"""

    parsed_sql = sqlparser.parse(sql_statement)
    print(f"is_satisfiable: {is_satisfiable(parsed_sql)}")


if __name__ == "__main__":
    main()
