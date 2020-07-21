from feature_graph.base import FeatureNode
from google.cloud import bigquery
from loguru import logger


class BigQueryNode(FeatureNode):
    def __init__(
        self, name: str, query: str, project: str, query_param_dict: dict = None
    ):

        self._project = project

        # TODO: Also allow passing a filename as query
        if query_param_dict:
            self._query = query.format(**query_param_dict)
        else:
            self._query = query

        super().__init__(name=name)

    @property
    def project(self):
        return self._project

    def run(self):
        logger.info("Running query {}".format(self._name))
        logger.info("Query: {}".format(self._query))

        client = bigquery.Client()
        _ = client.query(self._query, project=self._project).result()

        super(BigQueryNode, self).run()
