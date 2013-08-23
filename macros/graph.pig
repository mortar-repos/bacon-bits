/*
 * Copyright 2013 Mortar Data Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

----------------------------------------------------------------------------------------------------

IMPORT 'matrix.pig';

----------------------------------------------------------------------------------------------------

/*
 * All graphs are represented as sparse adjacency matrices:
 * { row: int, col: int, val: float }
 */

/*
 * Adds an edge of zero weight from each vertex in a graph to itself.
 *
 * graph: { row: int, col: int, val: float }
 * -->
 * out_graph: { row: int, col: int, val: float }
 * vertices:  { id: int }
 */
DEFINE Graph__AddSelfLoops(graph)
RETURNS out_graph, vertices {
    from_vertices       =   FOREACH $graph GENERATE row AS id;
    to_vertices         =   FOREACH $graph GENERATE col AS id;
    vertices_with_dups  =   UNION from_vertices, to_vertices;
    $vertices           =   DISTINCT vertices_with_dups;
    self_loops          =   FOREACH $vertices GENERATE
                                id AS row, id AS col, 0.0f AS val;
    $out_graph          =   UNION self_loops, $graph;
};

/*
 * Given a graph, returns a new graph with edges from each vertex in the original graph
 * to the neighborhood_size closest vertices in its 2-neighborhood by shortest path distance.
 * Implemented using Matrix Min-Plus Multiplication.
 *
 * graph: {row: int, col: int, val: float}
 * neighborhood_size: int
 * -->
 * nhoods: {row: int, col: int, val: float}
 */
DEFINE Graph__ShortestPaths_TwoSteps(graph, neighborhood_size)
RETURNS nhoods {
    graph_copy          =   FOREACH $graph GENERATE *;
    dest_verts_dup      =   FOREACH graph_copy GENERATE col AS id;
    dest_verts          =   DISTINCT dest_vert_dups;
    self_loops          =   FOREACH dest_verts GENERATE
                                id AS row, id AS col, 0.0f AS val;
    paths_to_follow     =   UNION graph_copy, self_loops;

    two_steps_out       =   Matrix__MinPlusProduct(graph, paths_to_follow);
    no_self_loops       =   FILTER two_steps_out BY row != col;
    $nhoods             =   Matrix__TrimRows(no_self_loops, 'ASC', $neighborhood_size);
};

/*
 * Given a graph, returns a new graph with edges from each vertex in the original graph
 * to the neighborhood_size closest vertices in its 3-neighborhood by shortest path distance.
 * Implemented using Matrix Min-Plus Multiplication.
 *
 * graph: {row: int, col: int, val: float}
 * neighborhood_size: int
 * -->
 * nhoods: {row: int, col: int, val: float}
 */
DEFINE Graph__ShortestPaths_ThreeSteps(graph, neighborhood_size)
RETURNS nhoods {
    graph_copy          =   FOREACH $graph GENERATE *;
    dest_verts_dup      =   FOREACH graph_copy GENERATE col AS id;
    dest_verts          =   DISTINCT dest_vert_dups;
    self_loops          =   FOREACH dest_verts GENERATE
                                id AS row, id AS col, 0.0f AS val;
    paths_to_follow     =   UNION graph_copy, self_loops;

    two_steps_out       =   Matrix__MinPlusProduct(graph, paths_to_follow);
    two_steps_trimmed   =   Matrix__TrimRows(no_self_loops, 'ASC', $neighborhood_size);

    three_steps_out     =   Matrix__MinPLusProduct(two_steps_out, paths_to_follow);
    no_self_loops       =   FILTER three_steps_out BY row != col;
    $nhoods             =   Matrix__TrimRows(no_self_loops, 'ASC', $neighborhood_size);
};
