import contextvars
import uuid
from typing import List, Union
from graphviz import Digraph

# TODO: Implement caching

__dag = contextvars.ContextVar("dag")


def get_dag():
    try:
        return __dag.get()
    except LookupError:
        return None


def set_dag(dag):
    __dag.set(dag)


class FeatureDAG:
    def __init__(self, dag_params: dict = None):
        self._nodes = set()
        self._dot = None
        self._diag_params = dag_params

    @property
    def dag_params(self):
        return self._dag_params

    # TODO Extend to allow assigning nodes outside of a context manager
    def __enter__(self):
        set_dag(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        set_dag(None)
        self.print_graph()

    def add_node(self, node: "FeatureNode"):
        self._nodes.add(node)

    def run_feature_graph(self):

        # Get nodes without parents
        nodes_no_children = [node for node in self._nodes if len(node.children) == 0]

        for node in nodes_no_children:
            self._walk_graph_query(parent_nodes=node.parents)
            if not node.query_ran:
                node.run()

    def _walk_graph_query(self, parent_nodes: List["FeatureNode"]):

        for node in parent_nodes:
            self._walk_graph_query(parent_nodes=node.parents)
            if not node.query_ran:
                node.run()

    # Printing the graph ------------------------------------------------------

    def print_graph(self):

        self._dot = Digraph()

        # Get nodes without parents
        nodes_no_parents = []
        for node in self._nodes:
            self._dot.node(node.node_id, node.name)
            if len(node.parents) == 0:
                nodes_no_parents.append(node)
        for node in nodes_no_parents:
            self._walk_graph_print(parent_node=node, child_nodes=node.children)

    def _walk_graph_print(
        self, parent_node: "FeatureNode", child_nodes: List["FeatureNode"]
    ):

        for node in child_nodes:
            self._dot.edge(parent_node.node_id, node.node_id)
            self._walk_graph_print(parent_node=node, child_nodes=node.children)

    def _repr_png_(self):
        return self._dot.pipe(format="png")


class FeatureNode:
    def __init__(self, name: str, input_etags: List[str] = None):

        self._node_id = str(uuid.uuid4())
        self._name = name
        self._input_etags = input_etags
        self._parents = set()
        self._children = set()
        self._query_ran = False

        self._dag = get_dag()
        if self._dag is None:
            raise EnvironmentError("Global DAG not set up")
        self._dag.add_node(self)

    @property
    def name(self):
        return self._name

    @property
    def parents(self):
        return self._parents

    @property
    def children(self):
        return self._children

    @property
    def node_id(self):
        return self._node_id

    @property
    def query_ran(self) -> bool:
        return self._query_ran

    def run(self):
        self._query_ran = True

    def __rshift__(self, other: Union["FeatureNode", List["FeatureNode"]]):
        if not isinstance(other, list):
            other = [other]

        for node in other:
            # TODO: Also check that a node is not one if it's parents or parents parents...
            if node.node_id == self._node_id:
                raise ValueError("Node can not be connected to itself")
            self._children.add(node)
            node._parents.add(self)

        return other

    def __lshift__(self, other: Union["FeatureNode", List["FeatureNode"]]):
        raise NotImplementedError("lshift is not yet implemented")

    def __rrshift__(self, other: List["FeatureNode"]):
        if not isinstance(other, list):
            other = [other]

        for node in other:
            # TODO: Also check that a node is not one if it's parents or parents parents...
            if node.node_id == self._node_id:
                raise ValueError("Node can not be connected to itself")
            self._parents.add(node)
            node._children.add(self)

        # return self

    def __rlshift__(self, other: List["FeatureNode"]):
        raise NotImplementedError("rlshift is not yet implemented")
