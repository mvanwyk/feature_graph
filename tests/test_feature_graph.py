from feature_graph import __version__
from feature_graph.base import FeatureDAG, FeatureNode
import pytest
import os


def test_version():
    assert __version__ == "0.1.0"


def test_simple_dag():

    with FeatureDAG():
        a = FeatureNode(name="query a")
        b = FeatureNode(name="query b")

        a >> b

    assert b in a.children
    assert a in b.parents


def test_double_arrow_dag():

    with FeatureDAG():
        a = FeatureNode(name="query a")
        b = FeatureNode(name="query b")
        c = FeatureNode(name="query c")

        a >> b >> c

    assert b in a.children
    assert c in b.children
    assert a in b.parents
    assert b in c.parents


def test_bracket_parent_dag():

    with FeatureDAG():
        a = FeatureNode(name="query a")
        b = FeatureNode(name="query b")
        c = FeatureNode(name="query c")

        [a, b] >> c

    assert c in a.children
    assert c in b.children
    assert a in c.parents
    assert b in c.parents


def test_bracket_children_dag():

    with FeatureDAG():
        a = FeatureNode(name="query a")
        b = FeatureNode(name="query b")
        c = FeatureNode(name="query c")

        a >> [b, c]

    assert b in a.children
    assert c in a.children
    assert a in b.parents
    assert a in c.parents


def test_bracket_middle_dag():

    with FeatureDAG():
        a = FeatureNode(name="query a")
        b = FeatureNode(name="query b")
        c = FeatureNode(name="query c")
        d = FeatureNode(name="query d")

        a >> [b, c] >> d

    assert b in a.children
    assert c in a.children
    assert a in b.parents
    assert a in c.parents
    assert d in b.children
    assert d in c.children
    assert b in d.parents
    assert c in d.parents


def test_assign_node_to_self_fail():

    with FeatureDAG():

        a = FeatureNode(name="query a")

        with pytest.raises(ValueError):
            a >> a


def test_assign_parent_to_node_fail():

    with FeatureDAG():
        a = FeatureNode(name="query a")
        b = FeatureNode(name="query b")

        a >> b
        with pytest.raises(ValueError):
            b >> a

        c = FeatureNode(name="query c")
        b >> c
        with pytest.raises(ValueError):
            c >> a


def test_no_dag_setup():

    with pytest.raises(EnvironmentError):
        _ = FeatureNode(name="query a")


def test_node_cant_be_assigned_twice():

    with FeatureDAG():
        a = FeatureNode(name="query a")
        b = FeatureNode(name="query b")

        a >> b
        with pytest.raises(ValueError):
            a >> b


def test_two_nodes_cant_have_same_name():

    with FeatureDAG():
        _ = FeatureNode(name="query")
        with pytest.raises(ValueError):
            _ = FeatureNode(name="query")


def test_same_node_cant_be_added_to_dag_twice():

    with FeatureDAG() as dag:
        a = FeatureNode(name="query")

    with pytest.raises(ValueError):
        dag.add_node(a)


def test_compact_state():

    with FeatureDAG() as dag:
        a = FeatureNode(name="query a")

    dag.run_feature_graph()

    dag._nodes.remove(a)

    assert a.node_id in dag._state_dict.keys()

    dag.compact_state()

    assert a.node_id not in dag._state_dict.keys()


def test_state_stored():

    with FeatureDAG() as dag:
        a = FeatureNode(name="query a")

    assert a.is_node_stale is True

    dag.run_feature_graph()

    assert a.is_node_stale is False


def test_clear_state():

    with FeatureDAG() as dag:
        a = FeatureNode(name="query a")

    assert a._get_state_cache_tag is None

    dag.run_feature_graph()

    assert a._get_state_cache_tag is not None

    a.clear_state()

    assert a._get_state_cache_tag is None


def test_state_stored_with_file():

    state_db = "./dag_state.sqlite"
    if os.path.exists(state_db):
        os.remove(state_db)

    with FeatureDAG(state_db=state_db) as dag:
        a = FeatureNode(name="query a")

    assert a.is_node_stale is True

    dag.run_feature_graph()

    assert a.is_node_stale is False

    del a
    del dag

    with FeatureDAG(state_db=state_db):
        a = FeatureNode(name="query a")

    assert a.is_node_stale is False

    os.remove(state_db)
