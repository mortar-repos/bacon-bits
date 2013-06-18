%default ITEM_IDS_INPUT_PATH '/Users/jpacker/data/github/item_ids.txt'
%default GRAPH_INPUT_PATH    '/Users/jpacker/data/github/item_distance_matrix_sample_4_steps.txt';
%default OUTPUT_PATH         '../test_output/shortest_paths'

%default NEIGHBORHOOD_SIZE  20

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

item_ids        =   LOAD '$ITEM_IDS_INPUT_PATH' USING PigStorage()
                    AS (id: int, name: chararray);

graph_tmp       =   LOAD '$GRAPH_INPUT_PATH' USING PigStorage()
                    AS (row: int, col: int, val: double);

----------------------------------------------------------------------------------------------------

graph, vertices =   Graph__AddSelfLoops(graph_tmp);
squared         =   Matrix__MinPlusSquared(graph);
cubed           =   Matrix__MinPlusProduct(graph, squared);
valid_paths     =   FILTER cubed BY row != col;

----------------------------------------------------------------------------------------------------

top_dests       =   FOREACH (GROUP valid_paths BY row) {
                        sorted = ORDER valid_paths BY val ASC;
                        top    = LIMIT sorted $NEIGHBORHOOD_SIZE;
                        GENERATE FLATTEN(top) AS (row, col, val);
                    }

----------------------------------------------------------------------------------------------------

with_names      =   Matrix__IdsToNames(top_dests, item_ids);
top_dests_2     =   FOREACH (GROUP with_names BY row) {
                        sorted = ORDER with_names BY val ASC;
                        GENERATE FLATTEN(sorted);
                    }

rmf $OUTPUT_PATH;
STORE top_dests_2 INTO '$OUTPUT_PATH' USING PigStorage();
