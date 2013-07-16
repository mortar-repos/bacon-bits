# Bacon Bits

Bacon Bits is a library of Pig macros and UDFs which implement useful algorithms for data science, currently focusing on matrix operations and graph algorithms. It is developed and maintained by [Mortar Data](http://www.mortardata.com/) and designed to be used with the [Mortar Development Framework](http://help.mortardata.com/tutorials/overview/getting_started_with_mortar).

## Installation

This repo is public to demo the components, but we have not yet implemented the system that will distribute them. We'll be building a tool on top of maven that will allow you to package all your Pig resources (macros, python udfs, java udfs, controlscript components) and add package dependencies to your projects.

## Manifest

Documented components:

- Macros for [sparse matrix](macros/matrix.pig) and [graph](macros/graph.pig) operations
- Macros implementing a complete [collaborative filter](macros/recsys.pig)
- Macros for [normalizing data](macros/normalization.pig) into the range [0, 1]
- [Graph Sampler](controlscripts/graph_sampler.py) controlscript which extracts a contiguous neighborhood of a graph for local development. You can specify a set of seed vertices to take the neighborhoods of.
- [Pagerank](controlscript/pagerank.py) implementation which uses a [reusable "iterate-until-convergence" component](controlscripts/lib/iteration_utils)

Experimental, undocumented components:

- A framework called PigCollection for serializing machine learning feature vectors (dense vectors, sparse vectors, or sets) as Pig bytearrays to greatly reduce memory overhead and consequently time spent writing to disk. UDFs handle all the serialization/deserialization for you. It also facilitates writing generic UDFs such as Distance which can handle all types of feature vectors (dense, sparse, set) and apply an appropriate metric/algorithm for each type.
- An implementation of K Nearest Neighbors using PigCollection feature vectors. This could be used for content-based filtering.
- An implementation of Term-Frequency-Inverse-Document-Frequency (TF-IDF) using PigCollection feature vectors.
- A prototype load-balancing UDF that intelligently allocates bags after a GROUP-BY to reducers in order to combat reducer skew.
- An implementation of Resevoir Sampling. Pig's SAMPLE operator allows you to take x% of a relation, but this allows you to take exactly N items.
- A controlscript library for checkpointing sequences of pigscripts and automatically restarting from the script that failed after the problem has been fixed.
