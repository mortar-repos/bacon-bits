/*
 * The Shortest Paths algorithm finds the shortest-path distance from each vertex
 * to vertices in its neighborhood. The closest vertices to it will be the vertices
 * "most similar" to it.
 *
 * This pigscript demonstrates running Shortest Paths on a Github repository similarity graph.
 * You can run the example with:
 *     mortar baconbits:local:run github_shortest_paths -p OUTPUT_PATH=/a/path/for/output
 */

%default GRAPH_INPUT_PATH    '../example_data/github_repo_similarity_matrix.txt'
%default REPO_IDS_INPUT_PATH '../example_data/github_repo_ids.txt'

%default NEIGHBORHOOD_SIZE  20      -- as the algorithm follows all possible paths between vertices,
                                    -- there can be a combinatorial explosion. to improve performance,
                                    -- we only consider the N most promising paths at each step.

IMPORT 'matrix.pig';
IMPORT 'graph.pig';

----------------------------------------------------------------------------------------------------

-- map of integer ids to github repo names

repo_ids        =   LOAD '$REPO_IDS_INPUT_PATH' USING PigStorage()
                    AS (id: int, name: chararray);

-- repo similarity matrix
-- row and col are repo ids, val is a value in [0, 1] representing how similar they are.

graph           =   LOAD '$GRAPH_INPUT_PATH' USING PigStorage()
                    AS (row: int, col: int, val: double);

-- we invert the similarity scores to get distances,
-- so similar repos are "close" and dissimilar repos are "far"

graph           =   FOREACH graph GENERATE row, col, 1.0 / val AS val;

----------------------------------------------------------------------------------------------------

-- We implement Shortest Paths using Min-Plus Matrix Multiplication.
-- See http://en.wikipedia.org/wiki/Min-plus_matrix_multiplication

graph, vertices =   Graph__AddSelfLoops(graph);
squared         =   Matrix__MinPlusSquared(graph);
cubed           =   Matrix__MinPlusProduct(graph, squared);
valid_paths     =   FILTER cubed BY row != col;

----------------------------------------------------------------------------------------------------

-- Join the Shortest Path neighborhoods to a map of repo ids to repo names,
-- so we can get interpretable results

with_names      =   Matrix__IdsToNames(walk_results, repo_ids);
neighborhoods   =   FOREACH (GROUP with_names BY row) {
                        sorted = ORDER with_names BY val DESC;
                        GENERATE FLATTEN(sorted);
                    }

rmf $OUTPUT_PATH;
STORE neighborhoods INTO '$OUTPUT_PATH' USING PigStorage();

-- to get a sense of the results,
-- search the example output for "linkedin/datafu" or "pydata/pandas"
