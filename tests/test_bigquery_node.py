from feature_graph.base import FeatureDAG
from feature_graph.bigquery_node import BigQueryNode
import pytest
import os
from datetime import datetime
from google.cloud import bigquery
from unittest.mock import MagicMock


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


def test_calc_cache_without_input_tables():

    with FeatureDAG(dag_params={"project": "my-project"}):
        a = BigQueryNode(name="query a", query="SELECT 1")
        b = BigQueryNode(name="query b", query="SELECT 1")
        c = BigQueryNode(name="query c", query="SELECT 2")

    assert a._calc_current_cache_tag() == b._calc_current_cache_tag()
    assert a._calc_current_cache_tag() != c._calc_current_cache_tag()


def test_calc_cache_with_input_tables():

    client = bigquery.Client()

    mod_timestamp = datetime.now()

    with FeatureDAG(dag_params={"project": "my-project"}):

        client.get_table = MagicMock(
            project="my-project",
            dataset_id="my_fake_dataset",
            table_id="my_fake_table",
            modified=mod_timestamp,
        )
        a = BigQueryNode(
            name="query a",
            query="SELECT 1",
            input_tables="my_fake_dataset.my_fake_table",
            client=client,
        )
        node_a_catch_tag = a._calc_current_cache_tag()
        b = BigQueryNode(
            name="query b",
            query="SELECT 1",
            input_tables="my_fake_dataset.my_fake_table",
            client=client,
        )
        node_b_catch_tag = b._calc_current_cache_tag()

        client.get_table = MagicMock(
            project="my-project",
            dataset_id="my_fake_dataset",
            table_id="my_other_fake_table",
            modified=mod_timestamp,
        )
        c = BigQueryNode(
            name="query c",
            query="SELECT 2",
            input_tables="my_fake_dataset.my_other_fake_table",
            client=client,
        )

    assert node_a_catch_tag == node_b_catch_tag
    assert node_a_catch_tag != c._calc_current_cache_tag()

    del a
    del b

    # Also test date changed
    with FeatureDAG(dag_params={"project": "my-project"}):

        client.get_table = MagicMock(
            project="my-project",
            dataset_id="my_fake_dataset",
            table_id="my_fake_table",
            modified=mod_timestamp,
        )
        a = BigQueryNode(
            name="query a",
            query="SELECT 1",
            input_tables="my_fake_dataset.my_fake_table",
            client=client,
        )
        node_a_catch_tag = a._calc_current_cache_tag()

        client.get_table = MagicMock(
            project="my-project",
            dataset_id="my_fake_dataset",
            table_id="my_fake_table",
            modified=datetime.now(),
        )
        b = BigQueryNode(
            name="query b",
            query="SELECT 1",
            input_tables="my_fake_dataset.my_fake_table",
            client=client,
        )

    assert node_a_catch_tag != b._calc_current_cache_tag()
