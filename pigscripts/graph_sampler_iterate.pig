verts           =   LOAD '$VERTICES_INPUT_PATH' USING PigStorage()
                    AS (id: int);
graph           =   LOAD '$GRAPH_INPUT_PATH' USING PigStorage()
                    AS (row: int, col: int, val: float);

frontier        =   FOREACH (JOIN verts BY id, graph BY row) GENERATE col AS id;
new_verts_dups  =   UNION verts, frontier;
new_verts       =   DISTINCT new_verts_dups;

rmf $VERTICES_OUTPUT_PATH;
STORE new_verts INTO '$VERTICES_OUTPUT_PATH' USING PigStorage();
