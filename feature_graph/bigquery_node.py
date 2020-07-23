from feature_graph.base import FeatureNode
from google.cloud import bigquery
from loguru import logger
import os


class BigQueryNode(FeatureNode):
    def __init__(
        self,
        name: str,
        query: str = None,
        query_file: str = None,
        project: str = None,
        query_params: dict = None,
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

    @property
    def project(self):
        return self._project

    def run(self):
        logger.info("Running query {}".format(self._name))
        logger.info("Query: {}".format(self._query))

        client = bigquery.Client()
        _ = client.query(self._query, project=self._project).result()

        super(BigQueryNode, self).run()
