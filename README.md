<h1 align="center" style="background-color:#f64e8b;">
  <a href="https://storage.googleapis.com/feature_graph_static_content/logo.png"><img src="./logo.png" title="Feature Graph" alt="Feature Graph"></a>
</h1>

# Feature Graph

## A simple DAG orchestrator built specifically for machine learning feature generation

[![Build Status](https://travis-ci.org/mvanwyk/feature_graph.svg?branch=master)](https://travis-ci.org/mvanwyk/feature_graph)[![Coverage Status](https://coveralls.io/repos/github/mvanwyk/feature_graph/badge.svg?branch=master)](https://coveralls.io/github/mvanwyk/feature_graph?branch=master)[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)[![Linter: flake8](https://img.shields.io/badge/linter-flake8-yellow)](https://gitlab.com/pycqa/flake8)[![License](http://img.shields.io/:license-mit-blue.svg?style=flat-square)](http://badges.mit-license.org)

Feature Graph is an easy to use feature generation framework

- **Only orchestration** - Simplifies code by separating orchestration from the heavy lifting of data processing
- **No cluster required** - No complex and expensive to run infrastructure
- **Compose features as a graph (DAG)** - Creating features atomically is inefficient. Creating them in a single step is complex and fragile. Feature Graph simplifies things by allowing you to break feature creation into steps (or nodes) and composable graph. The execution engine will then execute the graph taking in account any dependencies.
- **Intelligent graph execution** - Re-running a DAG will only re-run those which have changed

<!-- **Recordit**

![Recordit GIF](http://g.recordit.co/iLN6A0vSD8.gif) -->

<!-- ## Back story

- Orchestrator and processing infrastructure tightly coupled so complex to setup
- A lot of data pipelines tailored around flexibility for things like image processing but more business data is still in good old fashion databases
- Tried moving feature creation to pandas and pyspark but after countless hours of tuning and tinkering BigQuery was able to process the data much more quickly, at a lower cost and with a lot less code - the only problem is that SQL isn't the nicest language to build complex data pipelines with
- There are tools to do this but I wanted something simple and efficient
- Don't need heavy weight orchestrator when using managed services -->

---

## Table of Contents

- [Installation](#installation)
- [Features](#features)
- [Documentation](#documentation)
- Contributing (coming soon)
- FAQ (coming soon)
- [License](#license)
- [Acknowledgements](#acknowledgements)

---

## Installation

- Required python >=3.7
- To render the DAG diagrams you will also need graphviz installed ([download page](https://www.graphviz.org/download/))

```shell
$ pip install feature-graph
```

---

## Features

### Basic example

```python
with FeatureDAG(dag_params={"project": "my_project"}) as dag:

  base_query = BigQueryNode(name="Base Query", query_file="base_query.sql")
  feat_query_1 = BigQueryNode(name="Feat Query 1", query_file="feat_query_1.sql")
  final_query = BigQueryNode(name="Final Query", query_file="final_query.sql")

  base_query >> feat_query_1
  [feat_query_1, base_query] >> final_query

dag.run_feature_graph()
```

### Display a DAG diagram

```python

with FeatureDAG():

  base_query = BigQueryNode(
    name="Base Query", query_file="base_query.sql", project="my-project"
  )
  ...

  dag.print_graph()
```

![Sample of graph image generated](https://storage.googleapis.com/feature_graph_static_content/sample_graph.png)

### Save/load DAG state

```python

with FeatureDAG(state_db="my_state.db") as dag:

  base_query = BigQueryNode(
    name="Base Query", query_file="base_query.sql", project="my-project"
  )
  ...

dag.run_feature_graph()
```

### Intelligently re-run a graph

```python

with FeatureDAG(state_db="my_state.db") as dag:

  base_query = BigQueryNode(
    name="Base Query",
    query_file="base_query.sql",
    project="my-project",
    # The tables base_query.sql relies upon
    input_tables = ["my_dataset.my_table", "my_dataset.my_other_table"]
  )
  ...

# The next The node base_query will re-run if the contents of `base_query.sql` change
# or if one or more of the input tables have been modified since the last time the DAG
# was run.
dag.run_feature_graph()

```

## Documentation

> Better documentation coming. Check the docstrings for now

---

## Roadmap

v0.3.0

- [x] Improved safety checks to avoid loops in DAGs
- [x] Stateful DAG running - Store the state of the last run in the DAG and only re-run those parts that have changes
- [x] BigQueryNode load query from file
- [x] Improved docstrings
- [x] Complete README.md

v0.4.0

- [x] Docstring coverage monitoring w/badge
- [ ] Image of nodes that were run in when calling `run_feature_graph()`
- [ ] Image of node cache state (stale/fresh)
- [ ] Create nodes outside of a FeatureDAG context manager

v0.5.0

- [ ] CLI
- [ ] More node types
- [ ] Template node example
- [ ] Read the docs

v0.6.0

- [ ] Streaming feature generation support

---

## License

[![License](http://img.shields.io/:license-mit-blue.svg?style=flat-square)](http://badges.mit-license.org)

- **[MIT license](http://opensource.org/licenses/mit-license.php)**
- Copyright 2020 © [Murray Vanwyk](https://github.com/mvanwyk/)

---

## Acknowledgements

- Readme template - [https://gist.github.com/fvcproductions/1bfc2d4aecb01a834b46](https://gist.github.com/fvcproductions/1bfc2d4aecb01a834b46)
- Implementation style - [https://diagrams.mingrammer.com/](https://diagrams.mingrammer.com/)
