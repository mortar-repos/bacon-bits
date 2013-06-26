# Graph Algorithms

Bacon Bits contains ready-made implementations of a few common graph algorithms. Currently implemented are Pagerank, Personalized Pagerank, and Shortest Paths.

All graphs in Bacon Bits are represented as adjacency matrices with the schema: {row: int, col: int, val: int}. The algorithms are build using matrix operation components (see `docs/components/matrix_operations.md`).

## Pagerank

Pagerank finds the "most important" vertices on graph, based on the structure of its edges. See [wikipedia](http://en.wikipedia.org/wiki/PageRank) for more detail.

## Personalized Pagerank

Personalized Pagerank is a modification of the Pagerank algorithm that finds the "most relevant" vertices in the neighborhood of each vertex on a graph. At Mortar, we use this algorithm to build recommender systems, finding the most similar items to each given item. `pigscripts/github_personalized_pagerank.pig` demonstrates using Bacon Bits components to implement this algorithm on the Github repo similarity graph, finding for each repo the repos most similar to it.

## Shortest Paths

A related problem to finding similar vertices is finding shortest paths on a graph. Taking the N closest vertices by shortest path distance also gives a neighborhood of similar vertices for each vertex. Unlike pagerank-based methods, this strategy requires no algorithm parameters other than the neighborhood size, making it a simpler and often faster way to find similar vertices. `pigscripts/github_shortest_paths.pig` demonstrates using Bacon Bits components to implement this algorithm on the Github repo similarity graph.
