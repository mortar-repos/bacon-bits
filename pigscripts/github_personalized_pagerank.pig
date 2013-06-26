/*
 * The Personalized Pagerank algorithm simulates a random walk for each vertex on the graph.
 * The vertices that you have a high probability of ending up at for a given source vertex
 * will be the vertices "most similar" or "most relevant" to it.
 *
 * This pigscript demonstrates running Personalized Pagerank on a Github repository similarity graph.
 * You can run the example with:
 *     mortar baconbits:local:run github_personalized_pagerank -p OUTPUT_PATH=/a/path/for/output
 */

%default GRAPH_INPUT_PATH    '../example_data/github_repo_similarity_matrix.txt'
%default REPO_IDS_INPUT_PATH '../example_data/github_repo_ids.txt'

%default TELEPORT_PROB      0.333   -- at each step of the random walk, there is a chance
                                    -- of "teleporting" back to the source vertex. if this
                                    -- is set close to one, the "neighborhood" the algorithm returns
                                    -- will be  conservative; if it is set to zero, the neighborhood
                                    -- will consider far-off but popular vertices
                                    -- (in our case, github repositories).

%default NEIGHBORHOOD_SIZE  20      -- as the algorithm records all possible destinations of each
                                    -- random walk, there can be a combinatorial explosion.
                                    -- to improve performance, we only consider the N most promising
                                    -- paths at each step of the walk.

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

----------------------------------------------------------------------------------------------------

-- Normalize the similarity matrix to get initial probabilities for the random walk
-- to follow each possible edge, proportional to the edge weights.
-- Bacon Bits has lots of useful matrix macros in macros/matrix.pig.

trans_mat       =   Matrix__NormalizeRows(graph);

-- Perform the random walk. Just two iterations is good enough in most cases.

walk_step_1     =   Graph__RandomWalk_Init(trans_mat);
walk_step_2     =   Graph__PersonalizedPagerank_Iterate(
                        walk_step_1, trans_mat, 
                        $TELEPORT_PROB, $NEIGHBORHOOD_SIZE
                    );
walk_step_3     =   Graph__PersonalizedPagerank_Iterate(
                        walk_step_2, trans_mat,
                        $TELEPORT_PROB, $NEIGHBORHOOD_SIZE
                    );
walk_results    =   Graph__RandomWalk_Complete(walk_step_3);

----------------------------------------------------------------------------------------------------

-- Join the Personalized Pagerank neighborhoods to a map of repo ids to repo names,
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
