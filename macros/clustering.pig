-- SANDBOX FOR CLUSTINING ALGORITHMS
-- NOT READY FOR PUBLIC CONSUMPTION

IMPORT 'matrix.pig';
IMPORT 'graph.pig';

REGISTER 'bacon-bits-0.1.0.jar';

DEFINE Clustering__RepeatedSample com.mortardata.pig.sampling.RepeatedResevoirSample(
    '$CLUSTERING__NUM_MEANS', '$CLUSTERING__NUM_SEED_SAMPLES'
);

DEFINE Clustering__Distance com.mortardata.pig.geometry.distance.Distance(
    '$CLUSTERING__DISTANCE_METRIC'
);

DEFINE Clustering__MinPairwiseDistance com.mortardata.pig.geometry.distance.MinPairwiseDistance(
    '$CLUSTERING__DISTANCE_METRIC'
);

DEFINE Clustering__MeanPairwiseDistance com.mortardata.pig.geometry.distance.MeanPairwiseDistance(
    '$CLUSTERING__DISTANCE_METRIC'
);

DEFINE Clustering__Centroid com.mortardata.pig.geometry.Centroid();

/*
 * clusters: { cluster: int, members: { item: int, features: tuple } }
 * ==>
 * cluster_stats: { cluster: int, num_members: long, mean_pairwise_dist: double }
 */
DEFINE Clustering__ClusterStats(clusters)
RETURNS cluster_stats {
    $cluster_stats  =   FOREACH $clusters GENERATE
                            cluster,
                            COUNT(members) AS num_members,
                            Clustering__MeanPairwiseDistance(members.features) AS mean_pairwise_dist;
};

/*
 * items: { item: int, features: tuple }
 * requires udf definitions:
 *     DEFINE RepeatedSample com.mortardata.pig.sampling.RepeatedResevoirSample(
 *         '$CLUSTERING__NUM_MEANS', '$CLUSTERING__NUM_SEED_SAMPLES'
 *     );
 *     DEFINE MinPairwiseDistance com.mortardata.pig.distance.MinPairwiseEuclideanDistance();
 * ==>
 * pivots: { item: int, features: tuple }
 */
DEFINE Clustering__KMeans_ChoosePivots(items)
RETURNS pivots {
    pivot_candidates    =   FOREACH (GROUP items ALL) GENERATE
                            FLATTEN(Clustering__RepeatedSample(items));

    pivot_candidates    =   FOREACH pivot_candidates GENERATE
                                sample_id, items,
                                Clustering__MinPairwiseDistance(items.features)
                                AS min_dist;

    candidates_ordered  =   ORDER pivot_candidates BY min_dist DESC;
    best_candidates     =   LIMIT candidates_ordered 1;
    $pivots             =   FOREACH best_candidates GENERATE
                                FLATTEN(items) AS (pivot, features);
};

/*
 * items:  { item: int, features: tuple }
 * pivots: { pivot: int, features: tuple }
 * ==>
 * new_pivots: { features: tuple }
 */
DEFINE Clustering__KMeans_Iterate(items, pivots)
RETURNS new_pivots {
    distances           =   FOREACH (FILTER (CROSS items, $pivots) BY item != pivot) GENERATE
                                $items::item     AS item,
                                $items::features AS item_features,
                                $pivots::pivot   AS pivot,
                                1.0 / Clustering__Distance($items::features, $pivots::features)
                                AS inv_distance;

    closest_pivots      =   FOREACH (GROUP distances BY item) GENERATE
                                FLATTEN(TOP(1, 3, distances))
                                AS (item, item_features, pivot, inv_distance);

    voronoi_cells       =   FOREACH closest_pivots GENERATE
                                pivot, item_features;

    $new_pivots         =   FOREACH (GROUP voronoi_cells BY pivot) GENERATE
                                group AS pivot,
                                Clustering__Centroid(voronoi_cells.item_features)
                                AS features;
};

/*
 * items:  { item: int, features: tuple }
 * pivots: { pivot: int, features: tuple }
 * ==>
 * clusters: { cluster: int, members: { item: int, features: tuple } }
 */
DEFINE Clustering__KMeans_Clusters(items, pivots)
RETURNS clusters {
    distances           =   FOREACH (FILTER (CROSS $items, $pivots) BY item != pivot) GENERATE
                                $items::item      AS item,
                                $items::features  AS item_features,
                                $pivots::pivot    AS pivot,
                                1.0 / Clustering__Distance($items::features, $pivots::features)
                                AS inv_distance;

    closest_pivots      =   FOREACH (GROUP distances BY item) GENERATE
                                FLATTEN(TOP(1, 3, distances))
                                AS (item, features, pivot, inv_distance);

    $clusters           =   FOREACH (GROUP closest_pivots BY pivot) GENERATE
                                group AS cluster,
                                closest_pivots.(item, features) AS members;

};

/*
 * See http://www.cs.ucsb.edu/~xyan/classes/CS595D-2009winter/MCL_Presentation2.pdf
 * {row, col, val}, double, double ==> {row, col, val}
 */
DEFINE Clustering__Markov_Iterate(in_mat, inflation_parameter, epsilon)
returns out_mat {
    expansion   =   MatrixSquared($in_mat);
    inflation   =   MatrixElementwisePower(expansion, $inflation_parameter);
    pruning     =   FILTER inflation 
                    BY (val > org.apache.pig.piggybank.evaluation.math.POW($epsilon, $inflation_parameter));
    $out_mat    =   NormalizeMatrixColumns(pruning);
};

/*
 * {row, col, val} ==> {cluster: {(item_id)}}
 */
DEFINE Clustering__Markov_GetClustersFromResult(mcl_result)
returns clusters {
    by_row                  =   GROUP $mcl_result BY row;
    clusters_with_dups      =   FOREACH by_row GENERATE $1.$1 AS cluster;
    clusters_dups_ordered   =   FOREACH clusters_with_dups {
                                    ordered = ORDER cluster BY $0 ASC;
                                    GENERATE ordered AS cluster;
                                }
    $clusters               =   DISTINCT clusters_dups_ordered;
};
