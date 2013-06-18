REGISTER '../udfs/jython/characterize_graph.py' USING jython AS udfs;
IMPORT '../macros/stats.pig';

graph           =   LOAD '$INPUT_PATH' USING PigStorage()
                    AS (row: bytearray, col: bytearray, val: double);

----------------------------------------------------------------------------------------------------

from_ids        =   FOREACH graph GENERATE row AS id;
to_ids          =   FOREACH graph GENERATE col AS id;
from_and_to_ids =   UNION from_ids, to_ids;
vertices        =   DISTINCT from_and_to_ids;

num_vertices    =   FOREACH (GROUP vertices ALL) GENERATE
                        COUNT(vertices) AS count;

num_edges       =   FOREACH (GROUP graph ALL) GENERATE
                        COUNT(graph) AS count;

----------------------------------------------------------------------------------------------------

in_lists        =   COGROUP vertices BY id, graph BY col;
in_degrees      =   FOREACH in_lists GENERATE COUNT(graph) AS degree;
in_deg_stats    =   Stats__CharacterizeDistribution(in_degrees, 'degree');

out_lists       =   COGROUP vertices BY id, graph BY row;
out_degrees     =   FOREACH out_lists GENERATE COUNT(graph) AS degree;
out_deg_stats   =   Stats__CharacterizeDistribution(out_degrees, 'degree');

----------------------------------------------------------------------------------------------------

verts_no_in     =   FILTER in_lists BY COUNT(graph) == 0;
num_no_in       =   FOREACH (GROUP verts_no_in ALL) GENERATE
                        COUNT(verts_no_in) AS num_no_in;

verts_no_out    =   FILTER out_lists BY COUNT(graph) == 0;
num_no_out      =   FOREACH (GROUP verts_no_out ALL) GENERATE
                        COUNT(verts_no_out) AS num_no_out;

----------------------------------------------------------------------------------------------------

weight_stats    =   Stats__CharacterizeDistribution(graph, 'val');

----------------------------------------------------------------------------------------------------

num_verts_out       =   FOREACH num_vertices GENERATE
                            1 AS idx,
                            '# vertices' AS stat,
                            udfs.to_string(num_vertices.count) AS val;

num_edges_out       =   FOREACH num_edges GENERATE
                            2 AS idx,
                            '# edges' AS stat,
                            udfs.to_string(num_edges.count) AS val;

in_deg_stats_out    =   FOREACH in_deg_stats GENERATE
                            3 AS idx,
                            'distribution of in-degrees' AS stat,
                            udfs.to_string(TOTUPLE(mean, std_dev, quantiles)) AS val;

out_deg_stats_out   =   FOREACH out_deg_stats GENERATE
                            4 AS idx,
                            'distribution of out-degrees' AS stat,
                            udfs.to_string(TOTUPLE(mean, std_dev, quantiles)) AS val;

num_no_in_out       =   FOREACH num_no_in GENERATE
                            5 AS idx,
                            '# vertices with no inbound edges',
                            udfs.to_string(TOTUPLE(num_no_in, 100.0 * num_no_in / num_vertices.count)) AS val;

num_no_out_out      =   FOREACH num_no_out GENERATE
                            6 AS idx,
                            '# vertices with no outbound edges',
                            udfs.to_string(TOTUPLE(num_no_out, 100.0 * num_no_out / num_vertices.count)) AS val;

weight_stats_out    =   FOREACH weight_stats GENERATE
                            7 AS idx,
                            'distribution of weights',
                            udfs.to_string(TOTUPLE(mean, std_dev, quantiles)) AS val;

----------------------------------------------------------------------------------------------------

all_out         =   UNION   num_verts_out,
                            num_edges_out,
                            in_deg_stats_out,
                            out_deg_stats_out,
                            num_no_in_out,
                            num_no_out_out,
                            weight_stats_out;

final_out       =   ORDER all_out BY idx ASC;

DUMP final_out;
