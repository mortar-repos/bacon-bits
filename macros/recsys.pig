IMPORT 'matrix.pig';
IMPORT 'graph.pig';

----------------------------------------------------------------------------------------------------

DEFINE Recsys__UIScores_To_IILinks(ui_scores, min_link_weight)
RETURNS ii_links {
    ui_copy         =   FOREACH $ui_scores GENERATE *;
    ui_joined       =   JOIN    $ui_scores BY user, ui_copy BY user;
    ui_filtered     =   FILTER   ui_joined BY ui_scores::item != ui_copy::item;

    ii_link_terms   =   FOREACH ui_filtered GENERATE
                            $ui_scores::item AS row,
                            ui_copy::item    AS col,
                            ($ui_scores::score < ui_copy::score ?
                                $ui_scores::score : ui_copy::score
                            ) AS val;
    agg_ii_links    =   FOREACH (GROUP ii_link_terms BY (row, col)) GENERATE
                            FLATTEN(group) AS (row, col),
                            (double) SUM(ii_link_terms.val) AS val;

    $ii_links       =   FILTER agg_ii_links BY val >= $min_link_weight;
};

DEFINE Recsys__IILinksRaw_To_IILinksBayes(ii_links_raw, bayesian_prior)
RETURNS ii_links_bayes {
    item_totals     =   FOREACH (GROUP $ii_links_raw BY row) GENERATE
                            group AS item,
                            SUM($ii_links_raw.val) AS total;

    $ii_links_bayes =   FOREACH (JOIN item_totals BY item, $ii_links_raw BY col) GENERATE
                            $ii_links_raw::row AS row,
                            $ii_links_raw::col AS col,
                            $ii_links_raw::val / (item_totals::total + $bayesian_prior) AS val;
};

----------------------------------------------------------------------------------------------------

DEFINE Recsys__IILinksShortestPathsTwoSteps(ii_links, neighborhood_size)
RETURNS ii_nhoods {
    distance_mat    =   FOREACH $ii_links  GENERATE row, col, 1.0 / val AS val;
    nhoods_tmp      =   Graph__ShortestPathsTwoSteps(distance_mat, $neighborhood_size);
    nhoods_tmp_inv  =   FOREACH nhoods_tmp GENERATE row, col, 1.0 / val AS val;
    $ii_nhoods      =   Matrix__NormalizeRows(nhoods_tmp_inv);
};

DEFINE Recsys__IILinksShortestPathsThreeSteps(ii_links, neighborhood_size)
RETURNS ii_nhoods {
    distance_mat    =   FOREACH $ii_links  GENERATE row, col, 1.0 / val AS val;
    nhoods_tmp      =   Graph__ShortestPathsThreeSteps(distance_mat, $neighborhood_size);
    nhoods_tmp_inv  =   FOREACH nhoods_tmp GENERATE row, col, 1.0 / val AS val;
    $ii_nhoods      =   Matrix__NormalizeRows(nhoods_tmp_inv);
};

----------------------------------------------------------------------------------------------------

DEFINE Recsys__IILinksPagerankTwoSteps(ii_links, teleport_prob, neighborhood_size)
RETURNS ii_nhoods {
    trans_mat       =   Matrix__NormalizeRows($ii_links);
    walk_step_1     =   Graph__RandomWalk_Init(trans_mat);
    walk_step_2     =   Graph__PersonalizedPagerank_Iterate(walk_step_1, trans_mat, $teleport_prob, $neighborhood_size);
    $ii_nhoods      =   Graph__RandomWalk_Complete(walk_step_2);
};

DEFINE Recsys__IILinksPagerankThreeSteps(ii_links, teleport_prob, neighborhood_size)
RETURNS ii_nhoods {
    trans_mat       =   Matrix__NormalizeRows($ii_links);
    walk_step_1     =   Graph__RandomWalk_Init(trans_mat);
    walk_step_2     =   Graph__PersonalizedPagerank_Iterate(walk_step_1, trans_mat, $teleport_prob, $neighborhood_size);
    walk_step_3     =   Graph__PersonalizedPagerank_Iterate(walk_step_2, trans_mat, $teleport_prob, $neighborhood_size);
    $ii_nhoods      =   Graph__RandomWalk_Complete(walk_step_3);
};
