import contextvars
from typing import List, Union, Set
from graphviz import Digraph
from sqlitedict import SqliteDict
from hashlib import md5

__dag = contextvars.ContextVar("dag")


def get_dag() -> "FeatureDAG":
    """Gets the FeatureDAG set by the current context manager

    Returns:
        FeatureDAG: The FeatureDAG related to the context manager
    """
    try:
        return __dag.get()
    except LookupError:
        return None


def set_dag(dag: "FeatureDAG") -> None:
    """Set the FeatureDAG to be used by nodes created under the current context manager

    Args:
        dag (FeatureDAG): The FeatureDAG to set
    """
    __dag.set(dag)


class FeatureDAG:
    def __init__(self, dag_params: dict = None, state_db: str = ":memory:"):
        """FeatureDAG constructor

        Args:
            dag_params (dict, optional): Parameters that are made available to any node
            in the DAG. Defaults to None.
            state_db (str, optional): The name of the sqlite database to store the DAG
            state in. If not supplied then a temporary in memory database is used.
            Defaults to ":memory:".
        """

        self._nodes = set()
        self._dot = None
        self._dag_params = dag_params
        self._state_dict = SqliteDict(
            state_db, autocommit=True, encode=str, decode=str, tablename="state"
        )

    @property
    def dag_params(self) -> dict:
        """Returns the DAG parameters

        DAG parameters are parameters that are made available to any node in the DAG.
        A common use case is to simplify node creation by adding common node
        parameters, such as a project, so they don't need to be specified with every
        node

        Returns:
            dict: The DAG parameters
        """
        return self._dag_params

    def __enter__(self) -> "FeatureDAG":
        """Sets the context DAG for nodes to itself

        Returns:
            FeatureDAG: Returns itself
        """
        set_dag(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        set_dag(None)

    def add_node(self, node: "FeatureNode") -> None:
        """Adds a node to the DAG

        Args:
            node (FeatureNode): The FeatureNode to add to the DAG

        Raises:
            ValueError: "The same node can't be added to the DAG twice"
            ValueError: "Two nodes can't have the same name"
        """

        if node in self._nodes:
            raise ValueError("The same node can't be added to the DAG twice")
        if node.name in [n.name for n in self._nodes]:
            raise ValueError("Two nodes can't have the same name")

        self._nodes.add(node)

    def compact_state(self) -> None:
        "Removes any nodes in the state that aren't in the DAGs current list of nodes"
        dag_node_ids = [n.node_id for n in self._nodes]
        for node_id in self._state_dict.keys():
            if node_id not in dag_node_ids:
                del self._state_dict[node_id]

    def run_feature_graph(self) -> None:
        """Runs the nodes in the DAG

        The function will traverse a DAG starting with leaf nodes and working its way
        to the root nodes. Once it finds a root node it will work backwards looking for
        stale nodes in need of running.

        """

        nodes_no_children = [node for node in self._nodes if len(node.children) == 0]

        for node in nodes_no_children:
            self._walk_graph_query(parent_nodes=node.parents)
            if node.is_node_stale:
                node.run()
                node._update_cache()

    def _walk_graph_query(self, parent_nodes: List["FeatureNode"]) -> None:
        """Internal function that recursively walks the DAG running any stale nodes

        Args:
            parent_nodes (List[FeatureNode]): A list of a node's parent nodes to
            recursively walk
        """

        for node in parent_nodes:
            self._walk_graph_query(parent_nodes=node.parents)
            if node.is_node_stale:
                node.run()
                node._update_cache()

    # Printing the graph ------------------------------------------------------

    def print_graph(self) -> None:
        """Draws the DAG as a graphviz dot diagram

        The function starts by adding all the nodes to the diagram. It then traverses
        the DAG starting with any root nodes and working its way down the graph.

        """

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
    ) -> None:
        """Internal function that recursively walks the DAG adding connections between
        nodes

        Args:
            child_nodes (List[FeatureNode]): A list of a node's children nodes to
            recursively walk
        """

        for node in child_nodes:
            self._dot.edge(parent_node.node_id, node.node_id)
            self._walk_graph_print(parent_node=node, child_nodes=node.children)

    def _repr_png_(self):
        if not self._dot:
            self.print_graph()
        return self._dot.pipe(format="png")

    def _is_node_parent(self, node: "FeatureNode", check_node_id: str) -> bool:
        """Recursive internal function to check if a node_id is a node's parent or
        other ancestor

        Args:
            node (FeatureNode): The possible child node
            check_node_id (str): The node ID to check if it is a parent or ancestor

        Returns:
            bool: True if the check node ID is a parent or ancestor. False otherwise.
        """

        for parent_node in node.parents:
            if parent_node.node_id == check_node_id or self._is_node_parent(
                node=parent_node, check_node_id=check_node_id
            ):
                return True
        return False

    def _is_node_child(self, node: "FeatureNode", check_node_id: str) -> bool:
        """Recursive internal function to check if a node_id is a node's child or
        other descendant

        Args:
            node (FeatureNode): The possible parent node
            check_node_id (str): The node ID to check if it is a child or descendant

        Returns:
            bool: True if the check node ID is a child or descendant. False otherwise.
        """

        for child_node in node.children:
            if child_node.node_id == check_node_id or self._is_node_child(
                node=child_node, check_node_id=check_node_id
            ):
                return True
        return False


class FeatureNode:
    def __init__(self, name: str):
        """FeatureNode constructor

        Args:
            name (str): The name of the node. Note, it must be a unique in a DAG

        Raises:
            EnvironmentError: If the node can't find a FeatureDAG to be associated with
            you will receive the error `Global DAG not set up`
        """

        self._name = name.strip()
        self._node_id = md5(self._name.lower().encode("utf-8")).hexdigest()
        self._parents = set()
        self._children = set()

        self._dag = get_dag()
        if self._dag is None:
            raise EnvironmentError("Global DAG not set up")
        self._dag.add_node(self)

    @property
    def name(self) -> str:
        """The name of the node

        Returns:
            str: The name of the node
        """
        return self._name

    @property
    def parents(self) -> Set["FeatureNode"]:
        """The set of nodes which are direct parents of the node

        Returns:
            Set[FeatureNode]: The set of nodes which are direct parents of the node
        """
        return self._parents

    @property
    def children(self) -> Set["FeatureNode"]:
        """The set of nodes which are direct children of the node

        Returns:
            Set[FeatureNode]: The set of nodes which are direct children of the node
        """
        return self._children

    @property
    def node_id(self) -> str:
        """Returns the node_id

        The node_id is the md5 hash of the nodes name after being stripped and
        converted to lower case. It used by the internals of feature_graph and
        uniquely identifies a node.

        Returns:
            str: A unique ID used to identify a node

        """
        return self._node_id

    @property
    def is_node_stale(self) -> bool:
        """Used to check if the node needs to be run

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

        Returns:
            str: A string representing the state of all inputs to a node

        """
        return "True"

    @property
    def _get_state_cache_tag(self) -> str:
        """Returns the cache tag in the DAG's state

        Returns:
            str: The cache tag for the node in the DAG's cache

        """
        return self._dag._state_dict.get(self.node_id, None)

    def _set_state_cache_tag(self, cache_tag: str) -> None:
        """Internal function that sets the cache tag for the node

        Args:
            cache_tag (str): The second parameter.

        """
        self._dag._state_dict[self.node_id] = cache_tag

    def clear_state(self) -> None:
        "Clears the cache tag from the DAG's state"

        if self.node_id in self._dag._state_dict.keys():
            del self._dag._state_dict[self.node_id]

    def run(self) -> None:
        """Internal function that sets the cache tag for the node

        This function should be overridden by a subclass.

        """
        pass

    def _update_cache(self) -> None:
        "Updates the cache tag in the state database"

        self._set_state_cache_tag(cache_tag=self._calc_current_cache_tag())

    def __rshift__(
        self, other: Union["FeatureNode", List["FeatureNode"]]
    ) -> Union["FeatureNode", List["FeatureNode"]]:
        """Adds a FeatureNode or list of FeatureNodes as children

        Args:
            other (Union[FeatureNode, List[FeatureNode]]): A FeatureNode or list of
            FeatureNodes to add as children
        Raises:
            ValueError: Node ___ already a child
            ValueError: Node can not be connected to itself
            ValueError: Node can not be connected to a parent node. This prevents
            cyclical DAGs

        Returns:
            Union[FeatureNode, List[FeatureNode]]: Returns the original FeatureNode or
            list of FeatureNodes
        """

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

    def __lshift__(self, other: Union["FeatureNode", List["FeatureNode"]]) -> None:
        "Not implemented"

        raise NotImplementedError("lshift is not yet implemented")

    def __rrshift__(self, other: List["FeatureNode"]) -> None:
        """Connects a list of nodes as parents of the node

        Args:
            other (List[FeatureNode]): A list of FeatureNodes to connect with the node

        Raises:
            ValueError: Node can not be connected to itself
            ValueError: Node can not be connected to a parent node. This prevents
            cyclical DAGs
        """
        if not isinstance(other, list):
            other = [other]

        for node in other:

            if node.node_id == self._node_id:
                raise ValueError("Node can not be connected to itself")

            if self._dag._is_node_child(node=self, check_node_id=node.node_id):
                raise ValueError(
                    "Node {}, can not be connected to child node {}".format(
                        self.name, node.name
                    )
                )

            self._parents.add(node)
            node._children.add(self)

    def __rlshift__(self, other: List["FeatureNode"]):
        "Not implemented"
        raise NotImplementedError("rlshift is not yet implemented")
