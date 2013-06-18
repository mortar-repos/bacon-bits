verts               =   LOAD '$VERTICES_INPUT_PATH' USING PigStorage()
                        AS (id: int);
graph               =   LOAD '$GRAPH_INPUT_PATH' USING PigStorage()
                        AS (row: int, col: int, val: float);

sample_edges        =   FOREACH (JOIN verts BY id, graph BY row) GENERATE row, col, val;

rmf $SAMPLE_OUTPUT_PATH;
STORE sample_edges INTO '$SAMPLE_OUTPUT_PATH' USING PigStorage();
