%default ITEM_IDS_INPUT_PATH '/Users/jpacker/data/github/item_ids.txt'
%default GRAPH_INPUT_PATH    '/Users/jpacker/data/github/item_distance_matrix_sample_4_steps.txt';
%default OUTPUT_PATH         '../test_output/personalized_pageranks'

-- to get more noise: take nodes of lower pagerank
--                    reduce teleport prob
--                    extend the walk for more steps (WARNING COMBINATORIAL EXPLOSION WARNING)
--                    ^ deal with this by taking a selection of possible paths at each step

%default TELEPORT_PROB      0.333
%default NEIGHBORHOOD_SIZE  20

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

item_ids        =   LOAD '$ITEM_IDS_INPUT_PATH' USING PigStorage()
                    AS (id: int, name: chararray);

graph           =   LOAD '$GRAPH_INPUT_PATH' USING PigStorage()
                    AS (row: int, col: int, val: double);

-- INVERTING DISTANCE MATRIX TO GET SIMILARITIES.
-- REMOVE THIS LINE IF YOU ALREADY HAVE SIMILARITIES.
graph           =   FOREACH graph GENERATE row, col, 1.0 / val AS val;

----------------------------------------------------------------------------------------------------

trans_mat       =   Matrix__NormalizeRows(graph);
walk_step_1     =   Graph__RandomWalk_Init(trans_mat);
walk_step_2     =   Graph__PersonalizedPagerank_Iterate(walk_step_1, trans_mat, $TELEPORT_PROB);
walk_step_3     =   Graph__PersonalizedPagerank_Iterate(walk_step_2, trans_mat, $TELEPORT_PROB);
walk_results    =   Graph__RandomWalk_Complete(walk_step_3);

----------------------------------------------------------------------------------------------------

top_dests       =   FOREACH (GROUP walk_results BY row) {
                        sorted = ORDER walk_results BY val DESC;
                        top    = LIMIT sorted $NEIGHBORHOOD_SIZE;
                        GENERATE FLATTEN(top) AS (row, col, val);
                    }

----------------------------------------------------------------------------------------------------

with_names      =   Matrix__IdsToNames(top_dests, item_ids);
top_dests_2     =   FOREACH (GROUP with_names BY row) {
                        sorted = ORDER with_names BY val DESC;
                        GENERATE FLATTEN(sorted);
                    }

rmf $OUTPUT_PATH;
STORE top_dests_2 INTO '$OUTPUT_PATH' USING PigStorage();
