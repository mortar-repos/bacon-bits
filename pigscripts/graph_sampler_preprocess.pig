IMPORT '../macros/graph.pig';

graph   =   LOAD '$GRAPH_INPUT_PATH' USING PigStorage()
            AS (row: int, col: int, val: float);

graph_out, vertices =   Graph__AddSelfLoops(graph);
num_vertices        =   FOREACH (GROUP vertices ALL) GENERATE COUNT($1);

rmf $GRAPH_OUTPUT_PATH;
rmf $NUM_VERTICES_OUTPUT_PATH;

STORE graph_out    INTO '$GRAPH_OUTPUT_PATH'        USING PigStorage();
STORE num_vertices INTO '$NUM_VERTICES_OUTPUT_PATH' USING PigStorage();
