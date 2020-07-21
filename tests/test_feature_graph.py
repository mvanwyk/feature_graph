from feature_graph import __version__
from feature_graph.base import FeatureDAG, FeatureNode


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
