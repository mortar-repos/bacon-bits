/*
 * Given a bipartite graph returns several candidate nodes to be used
 * as seeds in sampling. Uses the degree distribution to do this and
 * relies on the user to choose appropriate degree range.
 *         
 * TODO: Be smarter about how to sample. May need a general
 *       'graph profiler' to do this.        
 *
 * bipartite_graph: { u, v }  A bipartite graph specified as edges
 *                            from nodes u in U to nodes v in V.
 * pivot                      Which set U or V (0 or 1) to pull from.
 * min_degree: int 
 * max_degree: int
 * max_results: int        
 * ==>
 * candidates: { u } or { v } depending on pivot.
 */
define GraphSampling__BiGraphSeedCandidates(bipartite_graph, pivot, min_degree, max_degree, max_results) returns candidates {
    degree_dist = GraphSampling__BiGraphDegreeDistribution($bipartite_graph, $pivot);
    nodes       = foreach (filter degree_dist by degree >= $min_degree and degree < $max_degree) generate node;
    $candidates = limit nodes $max_results;
};

define GraphSampling__BiGraphDegreeDistribution(bipartite_graph, pivot) returns degree_dist {
  $degree_dist = foreach (group $bipartite_graph by $pivot) generate
                   group                   as node,
                   COUNT($bipartite_graph) as degree;                
};

/*
 * Returns all the nodes one-step out on a bipartite graph, starting FROM v.
 *
 * bipartite_graph: { u, v }  A bipartite graph specified as edges
 *                            from nodes u in U to nodes v in V.
 * nodes: { v }               Walk graph starting from these nodes        
 */
define GraphSampling__BiGraphOneStep(bipartite_graph, nodes) returns steps, neighbors {
    edges = foreach (join $nodes by v, $bipartite_graph by v) generate
              $bipartite_graph::u as u,
              $bipartite_graph::v as v;
    
    out_links = distinct (foreach edges generate u);

    neighborhood = foreach (join out_links by u, $bipartite_graph by u) generate
                         $bipartite_graph::u as u,
                         $bipartite_graph::v as v;

    $steps     = distinct (foreach neighborhood generate u);
    $neighbors = distinct (foreach neighborhood generate v);
};
                
/*
 * Starting FROM v, walks the graph out two steps and collects the set of u that
 * are up to two steps away. Schema here is critical; bipartite graph MUST have
 * fields { u, v }. Sampling starts FROM v and walks the graph in a breadth-first
 * fashion. As such it is assumed that $seeds is a subset of V.
*/
define GraphSampling__BiGraphTwoStepSample(bipartite_graph, seeds) returns sample_nodes {
    one_u, one_v         = GraphSampling__BiGraphOneStep($bipartite_graph, $seeds);
    $sample_nodes, two_v = GraphSampling__BiGraphOneStep($bipartite_graph, one_v);
};

define GraphSampling__BiGraphThreeStepSample(bipartite_graph, seeds) returns sample_nodes {
    one_u, one_v           = GraphSampling__BiGraphOneStep($bipartite_graph, $seeds);
    two_u, two_v           = GraphSampling__BiGraphOneStep($bipartite_graph, one_v);
    $sample_nodes, three_v = GraphSampling__BiGraphOneStep($bipartite_graph, two_v); 
};
