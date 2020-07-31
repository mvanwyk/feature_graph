from feature_graph.base import FeatureNode
from google.cloud import bigquery
from loguru import logger
import os
from typing import Set
import hashlib


class BigQueryNode(FeatureNode):
    def __init__(
        self,
        name: str,
        query: str = None,
        query_file: str = None,
        project: str = None,
        query_params: dict = None,
        input_tables: Set[str] = None,
        client: bigquery.Client = None,
    ):
        super().__init__(name=name)

        if query and query_file:
            raise ValueError("You can not specify both query and query_file")
        if not query and not query_file:
            raise ValueError("You must specify either query or query file")

        if query:
            query_str = query

        if query_file:
            if not os.path.exists(query_file):
                raise FileNotFoundError(
                    "The query_file {} is not found".format(query_file)
                )
            with open(query_file, "r") as f:
                query_str = f.read()

        self._query = query_str
        if query_params:
            self._query = query_str.format(**query_params)

        self._project = project
        if not self._project:
            if not self._dag.dag_params or "project" not in self._dag.dag_params.keys():
                raise LookupError(
                    "project was not specified and was not found in the DAG parameters"
                )
            self._project = self._dag.dag_params["project"]

        self._client = client
        if not self._client:
            self._client = bigquery.Client()

        if input_tables and not isinstance(input_tables, list):
            input_tables = [input_tables]
        self._input_tables = input_tables

    @property
    def project(self) -> str:
        """Returns the project associated with a node

        Returns:
            str: Returns the project associated with a node
        """
        return self._project

    def run(self) -> None:
        "Runs the query on BigQuery"

        logger.debug("Query: {}".format(self._query))

        _ = self._client.query(self._query, project=self._project).result()

    def _calc_current_cache_tag(self) -> str:
        """Used to check if the node needs to be run

        The function returns a string which defines the state of the inputs to the
        node. It is used by the internals of feature_graph to determine if the node
        needs rerunning because an input has changed.

        It works by creating a string with the states of the inputs and then md5
        hashing it. The string is composed of,

        1) The query with the query_params substituted in
        2) If input tables used by the query are provided it will then,
           a) Get the full table names of the input_tables, including the project
           b) Get the last modified timestamp of each table
           c) Sort the list of full table names to add determinism
           d) Concatenate the list of full table names and timestamps into a single
              string

        Returns:
            str: A string which changes when the query, query_params or input tables to
            the node change

        """
        str_to_hash = self._query

        if self._input_tables:

            table_data_list = []
            for tbl in self._input_tables:

                tbl_ref = bigquery.table.TableReference.from_string(
                    tbl, default_project=self._project
                )
                table = self._client.get_table(tbl_ref)

                table_data_list.append(
                    {
                        "full_table_name": "{}.{}.{}".format(
                            table.project, table.dataset_id, table.table_id
                        ),
                        "last_modified": str(table.modified),
                    }
                )

            # Sort the list of tables to add determinism
            table_data_list = sorted(
                table_data_list, key=lambda i: i["full_table_name"]
            )

            str_to_hash += "|".join(
                [
                    "{}_{}".format(table.table_id, table.modified)
                    for t in table_data_list
                ]
            )

        cache_tag = hashlib.md5(str_to_hash.encode("utf-8")).hexdigest()

        return cache_tag
