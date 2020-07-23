from feature_graph.base import FeatureDAG
from feature_graph.bigquery_node import BigQueryNode
import pytest
import os


def test_node_with_query_str():

    query_str = "SELECT {value}"
    query_params = {"value": 1}

    with FeatureDAG():
        a = BigQueryNode(name="query a", query=query_str, project="my-project")

    assert a._query == query_str

    with FeatureDAG():
        a = BigQueryNode(
            name="query a",
            query=query_str,
            query_params=query_params,
            project="my-project",
        )

    assert a._query == query_str.format(**query_params)


def test_node_with_query_file():

    query_str = "SELECT {value}"
    query_params = {"value": 1}

    test_file = "bigquery_node_test.sql"

    with open(test_file, "w") as f:
        f.write(query_str)

    with FeatureDAG():
        a = BigQueryNode(name="query a", query_file=test_file, project="my-project")

    assert a._query == query_str

    with FeatureDAG():
        a = BigQueryNode(
            name="query a",
            query_file=test_file,
            query_params=query_params,
            project="my-project",
        )

    assert a._query == query_str.format(**query_params)

    os.remove(test_file)


def test_node_with_both_query_and_file():

    with FeatureDAG():
        with pytest.raises(ValueError):
            _ = BigQueryNode(
                name="query a",
                query="test",
                query_file="test.sql",
                project="my-project",
            )


def test_node_with_neither_query_or_file():

    with FeatureDAG():
        with pytest.raises(ValueError):
            _ = BigQueryNode(name="query a", project="my-project")


def test_node_with_non_existent_file():

    with FeatureDAG():
        with pytest.raises(FileNotFoundError):
            _ = BigQueryNode(
                name="query a", query_file="does_not_exist.sql", project="my-project"
            )


def test_node_project_not_specified():

    with FeatureDAG():
        with pytest.raises(LookupError):
            _ = BigQueryNode(name="query a", query="SELECT 1")


def test_node_project_specified_in_dag():

    project = "my-project"
    with FeatureDAG(dag_params={"project": project}):
        a = BigQueryNode(name="query a", query="SELECT 1")

    assert a.project == project
