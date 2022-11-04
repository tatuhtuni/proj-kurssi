from itertools import chain
from typing import Callable, Iterable, List, TypedDict
import psycopg
from psycopg import Connection

from . import util  # used to test relative imports


# TODO: break into variants discriminated by Node Type
# right now, the interface isn't safe to use because it's not clear what fields
# are available for each node type
node = TypedDict("Plan", {
    "Node Type": str,
    "Parent Relationship": str,
    "Startup Cost": float,
    "Total Cost": float,
    "Plan Rows": int,
    "Plan Width": int,
    "Actual Startup Time": float,
    "Actual Total Time": float,
    "Actual Rows": int,
    "Actual Loops": int,
    "Plans": List["node"],
    "Index Cond": str,
    "Filter": str,
    "Relation Name": str,
    "Alias": str,
    "Scan Direction": str,
    "Index Name": str,
    "Triggers": list[str],
    "Total Runtime": float,
    "One-Time Filter": str,
})

qep = TypedDict("QEP", {
    "Plan": node,
    "Triggers": list[str],
    "Planning Time": float,
    "Execution Time": float,
    "Total Runtime": float,
})


class QEPNode:
    """A node in a query execution plan."""

    def __init__(self, node_: node):
        """Create a new QEPNode.

        :param node_: the node to wrap"""
        self._node = node_

    def __iter__(self) -> Iterable["QEPNode"]:
        """Iterate over child nodes."""
        return map(QEPNode, self._node.get("Plans", []))

    def __len__(self) -> int:
        """Get the number of child nodes."""
        return len(self._node["Plans"])

    def __getitem__(self, key: int) -> "QEPNode":
        """Get the child node at the given index.

        :param key: the index of the child node to get

        :returns: the child node at the given index
        """
        return QEPNode(self._node["Plans"][key])

    def __str__(self):
        return self._node.__str__()

    def __repr__(self):
        return self._node.__repr__()

    @property
    def plan(self) -> node:
        """A dict of the node's properties."""
        return self._node

    @property
    def plans(self) -> list[node]:
        """A list of the node's children."""
        return self._node.get("Plans", [])

    def find(self, pr: Callable[[node], bool],
             recursive=False) -> list[node]:
        """Finds nodes matching the predicate.

        :param pr: a function that takes a node and returns True if it matches
        :param recursive: if True, search recursively, otherwise only search
            this+children

        :returns: a list of matching nodes
        """
        if recursive:
            return self.find(pr) + \
                list(chain.from_iterable(x.find(pr, True) for x in iter(self)))
        return list(filter(pr, chain((self._node,), self.plans)))

    def rfind(self, pred: Callable[[node], bool]) -> list[node]:
        """Finds nodes matching the predicate, recursively.

        :param pred: a function that takes a node and returns True if it matches

        :returns: a list of matching nodes
        """
        return self.find(pred, recursive=True)

    def findval(self, key: str, val: object, recursive=False) -> list[node]:
        """Finds nodes with the given key and value.

        :param key: the key to search for
        :param val: the value to search for
        :param recursive: if True, search recursively, otherwise only search
            this+children

        :returns: a list of matching nodes
        """
        return self.find(lambda x: x.get(key) == val, recursive)

    def rfindval(self, key: str, val: object) -> list[node]:
        """Finds nodes with the given key and value, recursively.

        :param key: the key to search for
        :param val: the value to search for

        :returns: a list of matching nodes
        """
        return self.findval(key, val, recursive=True)


class QEPAnalysis:
    """Represents the result of EXPLAIN ANALYZE."""

    def __init__(self, qep_: qep):
        self._qep = qep_

    def __str__(self):
        return self._qep.__str__()

    def __repr__(self):
        return self._qep.__repr__()

    @property
    def root(self) -> QEPNode:
        """The root node of the query execution plan."""
        return QEPNode(self._qep["Plan"])

    @property
    def plan(self) -> node:
        """A dict of the root node's properties."""
        return self._qep["Plan"]

    @property
    def qep(self) -> qep:
        """A dict of the query execution plan's properties."""
        return self._qep


class QEPParser:
    """Performs analyses on given queries, returning resultant QEPAnalysis."""

    def __init__(self, *args, conn=None, constraint_exclusion=True, **kwargs):
        self._ref = bool(conn)
        self._conn: Connection = conn or psycopg.connect(*args, **kwargs)
        # use constraint_exclusion to avoid unnecessary index scans
        if constraint_exclusion:
            with self._conn.cursor() as cur:
                cur.execute("set constraint_exclusion = on;")
                self._conn.commit()
        else:
            with self._conn.cursor() as cur:
                cur.execute("set constraint_exclusion = off;")
                self._conn.commit()

    def __del__(self):
        if not self._ref:
            self._conn.close()

    def __call__(self, stmt: str, *args, **kwargs) -> QEPAnalysis:
        """
        Executes a query and returns the query execution plan as a dictionary.

        Parameters:
            stmt: The query to execute.
            *args: Positional arguments to pass to cursor.execute().
            **kwargs: Keyword arguments to pass to cursor.execute().

        Returns:
            A dictionary representing the query execution plan.
        """
        stmt = "explain (format json, analyze, verbose)" + \
            stmt.strip().rstrip(';') + ";"
        with self._conn.cursor() as cur:
            cur.execute(stmt, *args, **kwargs)
            res = cur.fetchall()
            self._conn.rollback()
            if (n := len(res)) != 1:
                raise ValueError(f"Expected 1 row, got {n}")
            if (n := len(res[0])) != 1:
                raise ValueError(f"Expected 1 column, got {n}")
            if (n := len(res[0][0])) != 1:
                raise ValueError(f"Expected 1 item in column, got {n}")
            if (t := type(res[0][0][0])) != dict:
                raise ValueError(f"Expected dict in column, got {t}")
            return QEPAnalysis(res[0][0][0])

    def parse(self, stmt: str, *args, **kwargs) -> QEPAnalysis:
        '''Alias for __call__'''
        return self(stmt, *args, **kwargs)
