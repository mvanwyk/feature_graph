import contextvars
from typing import List, Union
from graphviz import Digraph
from sqlitedict import SqliteDict
from hashlib import md5

__dag = contextvars.ContextVar("dag")


def get_dag():
    try:
        return __dag.get()
    except LookupError:
        return None


def set_dag(dag):
    __dag.set(dag)


class FeatureDAG:
    def __init__(self, dag_params: dict = None, state_db: str = ":memory:"):
        self._nodes = set()
        self._dot = None
        self._dag_params = dag_params
        self._state_dict = SqliteDict(
            state_db, autocommit=True, encode=str, decode=str, tablename="state"
        )

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

        if node in self._nodes:
            raise ValueError("The same node can't be added to the DAG twice")
        if node.name in [n.name for n in self._nodes]:
            raise ValueError("Two nodes can't have the same name")

        self._nodes.add(node)

    def compact_state(self):
        """Removes any nodes in the state that aren't in the DAGs current list of nodes

        Args:
            None

        Returns:
            None

        """
        dag_node_ids = [n.node_id for n in self._nodes]
        for node_id in self._state_dict.keys():
            if node_id not in dag_node_ids:
                del self._state_dict[node_id]

    def run_feature_graph(self):

        nodes_no_children = [node for node in self._nodes if len(node.children) == 0]

        for node in nodes_no_children:
            self._walk_graph_query(parent_nodes=node.parents)
            if node.is_node_stale:
                node.run()
                node._update_cache()

    def _walk_graph_query(self, parent_nodes: List["FeatureNode"]):

        for node in parent_nodes:
            self._walk_graph_query(parent_nodes=node.parents)
            if node.is_node_stale:
                node.run()
                node._update_cache()

    # Printing the graph ------------------------------------------------------

    def print_graph(self):

        self._dot = Digraph()

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

    def _is_node_parent(self, node: "FeatureNode", check_node_id: str) -> bool:

        for parent_node in node.parents:
            if parent_node.node_id == check_node_id or self._is_node_parent(
                node=parent_node, check_node_id=check_node_id
            ):
                return True
        return False


class FeatureNode:
    def __init__(self, name: str):

        self._name = name.strip()
        self._node_id = md5(self._name.lower().encode("utf-8")).hexdigest()
        self._parents = set()
        self._children = set()

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
        """Returns the node_id

        The node_id is the md5 hash of the nodes name after being stripped and
        converted to lower case. It used by the internals of feature_graph and
        uniquely identifies a node.

        Args:
            None

        Returns:
            str: A unique ID used to identify a node

        """
        return self._node_id

    @property
    def is_node_stale(self) -> bool:
        """Used to check if the node needs to be run

        Args:
            None

        Returns:
            bool: True if the state cache tag DOES NOT equal the node's current cache
            tag, False otherwise.

        """
        return not self._calc_current_cache_tag() == self._get_state_cache_tag

    def _calc_current_cache_tag(self) -> str:
        """Calculates the current state of the node and returns as a string

        This function should be generally be overridden by a subclass. It's purpose is
        to return a string (eg a hex md5 hash) that represents the state of all the
        inputs to the node. For instance, if the node was processing a file it might
        check the last modified date of the file, hash it and return that as a hex
        string. By comparing the this cache tag with the one in the state it can be
        determined if the node needs to be rerun.

        Args:
            None

        Returns:
            str: A string representing the state of all inputs to a node

        """
        return "True"

    @property
    def _get_state_cache_tag(self) -> str:
        """Returns the cache tag in the DAG's state

        Args:
            None

        Returns:
            str: The cache tag for the node in the DAG's cache

        """
        return self._dag._state_dict.get(self.node_id, None)

    def _set_state_cache_tag(self, cache_tag: str):
        """Internal function that sets the cache tag for the node

        Args:
            cache_tag (str): The second parameter.

        Returns:
            None

        """
        self._dag._state_dict[self.node_id] = cache_tag

    def clear_state(self):
        """Clears the cache tag from the DAG's state

        Args:
            None

        Returns:
            None

        """
        if self.node_id in self._dag._state_dict.keys():
            del self._dag._state_dict[self.node_id]

    def run(self):
        """Internal function that sets the cache tag for the node

        This function should be overridden by a subclass.

        Args:
            None

        Returns:
            None

        """
        pass

    def _update_cache(self):
        self._set_state_cache_tag(cache_tag=self._calc_current_cache_tag())

    def __rshift__(self, other: Union["FeatureNode", List["FeatureNode"]]):
        if not isinstance(other, list):
            other = [other]

        for node in other:

            if node in self._children:
                raise ValueError("Node {} already a child".format(node.name))

            if node.node_id == self._node_id:
                raise ValueError("Node can not be connected to itself")

            if self._dag._is_node_parent(node=self, check_node_id=node.node_id):
                raise ValueError("Node can not be connected to a parent node")

            self._children.add(node)
            node._parents.add(self)

        return other

    def __lshift__(self, other: Union["FeatureNode", List["FeatureNode"]]):
        raise NotImplementedError("lshift is not yet implemented")

    def __rrshift__(self, other: List["FeatureNode"]):
        if not isinstance(other, list):
            other = [other]

        for node in other:

            if node.node_id == self._node_id:
                raise ValueError("Node can not be connected to itself")

            if self._dag._is_node_parent(node=self, check_node_id=node.node_id):
                raise ValueError("Node can not be connected to a parent node")

            self._parents.add(node)
            node._children.add(self)

    def __rlshift__(self, other: List["FeatureNode"]):
        raise NotImplementedError("rlshift is not yet implemented")
