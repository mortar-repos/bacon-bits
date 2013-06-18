IMPORT '../macros/matrix.pig';

DEFINE Graph__AddSelfLoops(graph)
RETURNS out_graph, vertices {
    from_vertices       =   FOREACH $graph GENERATE row AS id;
    to_vertices         =   FOREACH $graph GENERATE col AS id;
    vertices_with_dups  =   UNION from_vertices, to_vertices;
    $vertices           =   DISTINCT vertices_with_dups;
    self_loops          =   FOREACH $vertices GENERATE id AS row, id AS col, 0.0 AS val;
    $out_graph          =   UNION self_loops, $graph;
};

DEFINE Graph__RandomWalk_Init(graph)
RETURNS walk_graph {
    $walk_graph         =   FOREACH $graph GENERATE *;
};

DEFINE Graph__RandomWalk_Step(prev_walk_step, trans_mat)
RETURNS new_walk_step {
    terms               =   FOREACH (JOIN $prev_walk_step BY col, $trans_mat BY row) GENERATE
                                $prev_walk_step::row AS row,
                                $trans_mat::col      AS col,
                                $prev_walk_step::val * $trans_mat::val AS val;
    $new_walk_step      =   FOREACH (GROUP terms BY (row, col)) GENERATE
                                FLATTEN(group) AS (row, col),
                                SUM(terms.val) AS val;
};

DEFINE Graph__RandomWalk_TeleportToSource(prev_walk_step, teleport_prob)
RETURNS new_walk_step {
    damped              =   FOREACH $prev_walk_step GENERATE
                                row, col,
                                (1.0 - $teleport_prob) * val AS val;
    sources             =   FOREACH $prev_walk_step GENERATE row;
    teleports           =   FOREACH (DISTINCT sources) GENERATE
                                row, row AS col, $teleport_prob AS val;
    $new_walk_step      =   UNION damped, teleports;
};

DEFINE Graph__RandomWalk_Complete(final_walk_step)
RETURNS walk_result {
    no_self_loops       =   FOREACH (FILTER $final_walk_step BY row != col) GENERATE row, col, val;
    $walk_result        =   Matrix__NormalizeRows(no_self_loops);
};

DEFINE Graph__PersonalizedPagerank_Iterate(prev_walk_step, trans_mat, teleport_prob)
RETURNS new_walk_step {
    new_walk_step_tmp   =     Graph__RandomWalk_Step($prev_walk_step, $trans_mat);
    $new_walk_step      =     Graph__RandomWalk_TeleportToSource(new_walk_step_tmp, $teleport_prob);
};
