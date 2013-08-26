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

REGISTER 'datafu-0.0.10.jar';
DEFINE Recsys__Enumerate
        datafu.pig.bags.Enumerate('1');

REGISTER 'trove4j-3.0.3.jar';
REGISTER 'bacon-bits.jar';
DEFINE Recsys__UIScores_To_IITerms_Helper
    com.mortardata.pig.recsys.UIScores_To_IITerms_Helper();
DEFINE Recsys__FollowPaths
    com.mortardata.pig.recsys.FollowPaths();

REGISTER 'recsys.py' USING jython AS recsys_udfs;

----------------------------------------------------------------------------------------------------
/*
 * This file contains macros for building recommender systems.
 *
 * The basic pipline for a collaborative filter goes:
 *    1) Recsys__UISignals_To_IILinks
 *    2) Recsys__IILinksRaw_To_IILinksBayes
 *    -- if you want to boost certain links to account for domain-specific considerations,
 *    -- you should do it at this point (between steps 2 and 3)
 *    3) Recsys__IILinksShortestPathsTwoSteps (or Recsys__IILinksShortestPathsThreeSteps or other alternatives)
 *    -- by this point you get item-to-item recommendations
 *    4) Recsys__UserItemNeighborhoods
 *    5) Recsys__FilterItemsAlreadySeenByUser
 *    6) Recsys__TopNUserRecs
 *    -- by this point you get user-to-item recommendations
 *
 * See the comments for each macro for more details.
 */
----------------------------------------------------------------------------------------------------

/*
 * Step 1 of the default collaborative filter.
 *
 * Given a relation consisting of user-item interaction events,
 * constructs a similarity matrix between items. It also outputs
 * an index of popularity for each item.
 *
 * First, the events are aggregated by (user, item), the scores summed up,
 * and a logistic scale applied so a user with tons of events with a particular item
 * faces diminishing returns. The popularity index (output "item_scores")
 * is the sum of these scaled user-item scores for each item.
 * 
 * Algorithmically, this is "contracting" the bipartite user-item graph
 * into a regular graph of item similarities. If a user U has affinity
 * with items I1 and I2 of weights W1 and W2 respectively, than a link
 * between I1 and I2 is formed with the weight MIN(W1, W2).
 *
 * ui_signals:         {user: int, item: int, score: float}
 * logistic_param:     float
 * min_link_weight:    int/float    item-item links of less weight than this
 *                                  will be filtered out for better performance
 *                                  note that this weight is using the ui-values
 *                                  AFTER THE LOGISTIC SCALE HAS BEEN APPLIED
 * max_links_per_user: int          if any one user has an anomalously high number of links
 *                                  to items, this will dramatically hurt the performance
 *                                  of later steps in the default collab filter. so for users
 *                                  with more than this number of links, we only keep their
 *                                  top [this many] links. ties are broken randomly.
 * -->
 * ii_links: {item_A: int, item_B: int, weight: float}
 * item_scores: {item: int, score: float}
 */
DEFINE Recsys__UISignals_To_IILinks(ui_signals,
                                    logistic_param,
                                    min_link_weight,
                                    max_links_per_user)
RETURNS ii_links, item_scores {
    ii_link_terms, $item_scores =   Recsys__UISignals_To_IITerms(
                                        $ui_signals,
                                        $logistic_param,
                                        $min_link_weight,
                                        $max_links_per_user
                                    );
    $ii_links                   =   Recsys__IITerms_To_IILinks(
                                        ii_link_terms, $min_link_weight
                                    );
};

/*
 * Helper macro for Recsys__UISignals_To_IILinks
 *
 * ui_signals: {user: int, item: int, score: float}
 * logistic_param:  float
 * min_link_weight: float
 * -->
 * ii_link_terms: {item_A: int, item_B: int, weight: float}
 */
DEFINE Recsys__UISignals_To_IITerms(ui_signals,
                                    logistic_param,
                                    min_link_weight,
                                    max_links_per_user)
RETURNS ii_link_terms, item_scores {
    ui_agg          =   FOREACH (GROUP $ui_signals BY (user, item)) GENERATE
                            FLATTEN(group) AS (user, item),
                            (float) SUM($1.score) AS score;
    ui_scaled       =   FOREACH ui_agg GENERATE
                            user, item,
                            (float) recsys_udfs.logistic_scale(score, $logistic_param)
                            AS score;

    item_scores_tmp =   FOREACH (GROUP ui_scaled BY item) GENERATE
                            group AS item, (float) SUM($1.score) AS score, $1 AS ui;
    $item_scores    =   FOREACH item_scores_tmp GENERATE item, score;
    ui_filt         =   FOREACH (FILTER item_scores_tmp BY score >= $min_link_weight) GENERATE
                            FLATTEN(ui) AS (user, item, score);

    $ii_link_terms  =   FOREACH (GROUP ui_filt BY user) {
                            top_for_user = TOP($max_links_per_user, 2, $1);
                            GENERATE FLATTEN(Recsys__UIScores_To_IITerms_Helper(top_for_user));
                        }
};

/*
 * Helper macro for Recsys__UIScores_To_IILinks
 *
 * ii_link_terms: {item_A: int, item_B: int, weight: float}
 * min_link_weight: int/float
 * -->
 * ii_links: {item_A: int, item_B: int, weight: float}
 */
DEFINE Recsys__IITerms_To_IILinks(ii_link_terms, min_link_weight)
RETURNS ii_links {
    agg_ii_links    =   FOREACH (GROUP $ii_link_terms BY (item_A, item_B)) GENERATE
                            FLATTEN(group) AS (item_A, item_B),
                            (float) SUM($ii_link_terms.weight) AS weight;

    $ii_links       =   FILTER agg_ii_links BY weight >= $min_link_weight;
};

/*
 * Step 2 of the default collaborative filter.
 *
 * The item-to-item links outputted by Step 1 (Recsys__UIScores_To_IILinks)
 * do not account for the popularity of the item linked to. Without accounting
 * for this, popular items will be considered "most-similar" for every other item,
 * since users of the other items frequently interact wit the popular item.
 *
 * This macro uses Bayes theorem to avoid this problem, and also scaled the
 * item-to-item links to all be within the range [0, 1]. It sets the similarity
 * of repos A and B to be an estimate of the probability that a random user U
 * will interact with A given that they interacted with B. In mathematical notation,
 * it similarity(A, B) = P(A | B). This way, if B is very popular, you need a lot
 * of users co-interacting with it and A for the link to be statistically significant.
 *
 * This estimation breaks down if B is very unpopular, with only a few users interacting with it.
 * If B only has 2 users and they all interacted with A, that is most likely due to chance, not similarity.
 * The macro therefore takes a Bayesian Prior, which guards against these small sample sizes.
 * Intuitively, it represents a number of "pseudo-observations" of the non-similarity of A and B;
 * or in other words, A is "innocent of B until proven guilty beyond a reasonable doubt".
 *
 * ii_links_raw: {item_A: int, item_B: int, weight: float}
 * item_scores:  {item: int, score: float}
 * bayesian_prior: int/float
 * -->
 * ii_links_bayes: {item_A: int, item_B: int, weight: float}
 */
DEFINE Recsys__IILinksRaw_To_IILinksBayes(ii_links_raw, item_scores, prior)
RETURNS ii_links_bayes {
    $ii_links_bayes =   FOREACH (JOIN $item_scores BY item, $ii_links_raw BY item_B) GENERATE
                            item_A AS item_A,
                            item_B AS item_B,
                            (float) (weight / (score + $prior))
                            AS weight;
};

----------------------------------------------------------------------------------------------------

/*
 * Step 3 of the default collaborative filter.
 *
 * The graph generated by Step 2 (Recsys__IILinksRaw_To_IILinksBayes)
 * is a good guess of similarity between items, but it can only contain
 * edges between items that were directly interacted with by the same user.
 * For obscure items with only a handful of users interacting with it,
 * it might not have as many links as we would like to make recommendations
 *
 * To get around this, we follow shortest paths on the graph, defining
 * distance to be the inverse of the similarity scores. This macro explores
 * the 2-neighborhood of each source node; the macro below this explores
 * the 3-neighborhood.
 *
 * This also has the effect that if there is a path from items
 * A -> B -> C that has a total distance of less than A -> D,
 * then the former path is recognized is more relevant; that is,
 * the link A -> C will be ranked higher than A -> D.
 *
 * For performance reasons, only the neighborhood_size most promising
 * paths at each iteration step are considered.
 *
 * In the output, the field "steps" is the minimum number of steps on the graph
 * it took to get to item_B (this is either 1 or 2).
 * "[n]_step_paths" is the number [n]-step paths from item_A to item_B.
 *
 * ii_links: {item_A: int, item_B: int, weight: float}
 * initial_nhood_size: int
 * itermed_nhood_size: int
 * -->
 * item_nhoods: {item_A: int, item_B: int, weight: float, steps: int, rank: int}
 */
DEFINE Recsys__IILinksShortestPathsTwoSteps(ii_links,
                                            initial_nhood_size,
                                            final_nhood_size)
RETURNS item_nhoods {
    graph, paths        =   Recsys__InitShortestPaths($ii_links, $initial_nhood_size);
    shortest_paths      =   Recsys__RunShortestPaths_TwoSteps(
                                graph, paths, $final_nhood_size
                            );
    $item_nhoods        =   Recsys__PostprocessShortestPaths(shortest_paths, $final_nhood_size);
};

DEFINE Recsys__IILinksShortestPathsTwoSteps_InMemory(ii_links,
                                                     initial_nhood_size,
                                                     final_nhood_size)
RETURNS item_nhoods {
    graph, paths        =   Recsys__InitShortestPaths($ii_links, $initial_nhood_size);
    shortest_paths      =   Recsys__RunShortestPaths_TwoSteps_InMemory(
                                graph, paths, $final_nhood_size
                            );
    $item_nhoods        =   Recsys__PostprocessShortestPaths(shortest_paths, $final_nhood_size);
};

/*
 * See notes for Recsys__IILinksShortestPathsTwoSteps.
 *
 * ii_links: {item_A: int, item_B: int, weight: float}
 * initial_nhood_size: int
 * intermediate_nhood_size: int
 * final_nhood_size: int
 * -->
 * ii_nhoods: {item_A: int, item_B: int, weight: float, steps: int, rank: int}
 */
DEFINE Recsys__IILinksShortestPathsThreeSteps(ii_links,
                                              initial_nhood_size,
                                              intermediate_nhood_size,
                                              final_nhood_size)
RETURNS item_nhoods {
    graph, paths        =   Recsys__InitShortestPaths($ii_links, $initial_nhood_size);
    shortest_paths      =   Recsys__RunShortestPaths_ThreeSteps(
                                graph, paths, $intermediate_nhood_size, $final_nhood_size
                            );
    $item_nhoods        =   Recsys__PostprocessShortestPaths(shortest_paths, $final_nhood_size);
};

DEFINE Recsys__IILinksShortestPathsThreeSteps_InMemory(ii_links,
                                                       initial_nhood_size,
                                                       intermediate_nhood_size,
                                                       final_nhood_size)
RETURNS item_nhoods {
    graph, paths        =   Recsys__InitShortestPaths($ii_links, $initial_nhood_size);
    shortest_paths      =   Recsys__RunShortestPaths_ThreeSteps_InMemory(
                                graph, paths, $intermediate_nhood_size, $final_nhood_size
                            );
    $item_nhoods        =   Recsys__PostprocessShortestPaths(shortest_paths, $final_nhood_size);
};


/*
 * ii_links: {item_A: int, item_B: int, weight: float}
 * neighborhood_size: int
 * -->
 * graph: {item_A: int, item_B: int, dist: float}
 * paths: {item_A: int, item_B: int, dist: float}
 */
DEFINE Recsys__InitShortestPaths(ii_links, neighborhood_size)
RETURNS graph, paths {
    distance_mat        =   FOREACH $ii_links GENERATE
                                item_A, item_B, 1.0f / weight AS dist;

    $graph              =   FOREACH (GROUP distance_mat BY item_A) {
                                sorted = ORDER $1 BY dist ASC;
                                   top = LIMIT sorted $neighborhood_size;
                                GENERATE FLATTEN(top)
                                      AS (item_A, item_B, dist),
                                    1 AS steps: int;
                            }

    graph_copy          =   FOREACH $graph GENERATE item_A, item_B, dist;
    dest_verts_dup      =   FOREACH graph_copy GENERATE item_B AS id;
    dest_verts          =   DISTINCT dest_verts_dup;
    self_loops          =   FOREACH dest_verts GENERATE
                                id AS item_A, id AS item_B, 0.0f AS dist;
    $paths              =   UNION graph_copy, self_loops;
};

/*
 * ii_links: {item_A: int, item_B: int, weight: float}
 * source_items: {item: int}
 * neighborhood_size: int
 * -->
 * graph: {item_A: int, item_B: int, dist: float}
 * paths: {item_A: int, item_B: int, dist: float}
 */
DEFINE Recsys__InitShortestPaths_FromSourceItems(ii_links,
                                                 source_items,
                                                 neighborhood_size)
RETURNS graph, paths {
    distance_mat        =   FOREACH $ii_links GENERATE
                                item_A, item_B, 1.0f / weight AS dist;

    graph_tmp           =   FOREACH (GROUP distance_mat BY item_A) {
                                sorted = ORDER $1 BY dist ASC;
                                   top = LIMIT sorted $neighborhood_size;
                                GENERATE FLATTEN(top)
                                      AS (item_A, item_B, dist);
                            }

    $graph              =   FOREACH (JOIN source_items BY item, graph_tmp BY item_A) GENERATE
                                item_A AS item_A, item_B AS item_B, dist AS dist, 1 AS steps: int;

    graph_copy          =   FOREACH graph_tmp GENERATE item_A, item_B, dist;
    dest_verts_dup      =   FOREACH graph_copy GENERATE item_B AS id;
    dest_verts          =   DISTINCT dest_verts_dup;
    self_loops          =   FOREACH dest_verts GENERATE
                                id AS item_A, id AS item_B, 0.0f AS dist;
    $paths              =   UNION graph_copy, self_loops;
};

/*
 * graph: {item_A: int, item_B: int, dist: float}
 * paths: {item_A: int, item_B: int, dist: float}
 * intermediate_nhood_size: int
 * final_nhood_size: int
 * -->
 * shortest_paths: {item_A: int, item_B: int, dist: float, steps: int}
 */
DEFINE Recsys__RunShortestPaths_TwoSteps(graph,
                                         paths,
                                         neighborhood_size)
RETURNS shortest_paths {
    two_step_terms      =   FOREACH (JOIN $graph BY item_B, $paths BY item_A) GENERATE
                                $graph::item_A AS item_A,
                                $paths::item_B AS item_B,
                                $graph::dist + $paths::dist AS dist,
                                ($paths::item_A == $paths::item_B ? 1 : 2) AS steps;

    $shortest_paths     =   FOREACH (GROUP two_step_terms BY (item_A, item_B)) GENERATE
                                           FLATTEN(group) AS (item_A, item_B),
                                     (float) MIN($1.dist) AS dist,
                                            MIN($1.steps) AS steps;
};

/*
 * graph: {item_A: int, item_B: int, dist: float}
 * paths: {item_A: int, item_B: int, dist: float}
 * intermediate_nhood_size: int
 * final_nhood_size: int
 * -->
 * item_nhoods: {item_A: int, item_B: int, dist: float, steps: int}
 */
DEFINE Recsys__RunShortestPaths_ThreeSteps(graph,
                                           paths,
                                           intermediate_nhood_size,
                                           final_nhood_size)
RETURNS shortest_paths {
    two_step_terms      =   FOREACH (JOIN $graph BY item_B, $paths BY item_A) GENERATE
                                $graph::item_A AS item_A,
                                $paths::item_B AS item_B,
                                $graph::dist + $paths::dist AS dist,
                                ($paths::item_A == $paths::item_B ? 1 : 2) AS steps;

    two_steps_out       =   FOREACH (GROUP two_step_terms BY (item_A, item_B)) GENERATE
                                      FLATTEN(group) AS (item_A, item_B),
                                (float) MIN($1.dist) AS dist,
                                       MIN($1.steps) AS steps;

    two_steps_trim      =   FOREACH (GROUP two_steps_out BY item_A) {
                                sorted = ORDER $1 BY dist ASC;
                                   top = LIMIT sorted $intermediate_nhood_size;
                                GENERATE FLATTEN(top)
                                      AS (item_A, item_B, dist, steps);
                            }

    three_step_terms    =   FOREACH (JOIN two_steps_trim BY item_B, $paths BY item_A) GENERATE
                                two_steps_trim::item_A AS item_A,
                                        $paths::item_B AS item_B,
                                two_steps_trim::dist + $paths::dist AS dist,
                                ($paths::item_A == $paths::item_B ?
                                 two_steps_trim::steps :
                                 two_steps_trim::steps + 1) AS steps;

    $shortest_paths     =   FOREACH (GROUP three_step_terms BY (item_A, item_B)) GENERATE
                                             FLATTEN(group) AS (item_A, item_B),
                                       (float) MIN($1.dist) AS dist,
                                              MIN($1.steps) AS steps;
};

/*
 * graph: {item_A: int, item_B: int, dist: float}
 * paths: {item_A: int, item_B: int, dist: float}
 * neighborhood_size: int
 * -->
 * item_nhoods: {item_A: int, item_B: int, dist: float, steps: int}
 */
DEFINE Recsys__RunShortestPaths_TwoSteps_InMemory(graph,
                                                  paths,
                                                  neighborhood_size)
RETURNS item_nhoods {
    two_steps       =   FOREACH (COGROUP $graph BY item_B, $paths BY item_A) GENERATE
                            FLATTEN(Recsys__FollowPaths($graph, $paths))
                            AS (item_A: int, local_paths: bytearray);
    $item_nhoods    =   FOREACH (GROUP two_steps BY item_A) GENERATE
                            group AS item_A,
                            FLATTEN(com.mortardata.pig.recsys.SelectShortestPaths(
                                $neighborhood_size, $1.local_paths)
                            ) AS (item_B, dist, steps);
};

/*
 * graph: {item_A: int, item_B: int, dist: float}
 * paths: {item_A: int, item_B: int, dist: float}
 * intermediate_nhood_size: int
 * final_nhood_size: int
 * -->
 * item_nhoods: {item_A: int, item_B: int, dist: float, steps: int}
 */
DEFINE Recsys__RunShortestPaths_ThreeSteps_InMemory(graph,
                                                    paths,
                                                    intermediate_nhood_size,
                                                    final_nhood_size)
RETURNS item_nhoods {
    two_steps           =   FOREACH (COGROUP $graph BY item_B, $paths BY item_A) GENERATE
                                FLATTEN(Recsys__FollowPaths($graph, $paths))
                                AS (item_A: int, local_paths: bytearray);
    two_step_nhoods     =   FOREACH (GROUP two_steps BY item_A) GENERATE
                                group AS item_A,
                                FLATTEN(com.mortardata.pig.recsys.SelectShortestPaths(
                                    $intermediate_nhood_size, $1.local_paths)
                                ) AS (item_B, dist, steps);

    three_steps         =   FOREACH (COGROUP two_step_nhoods BY item_B, $paths BY item_A) GENERATE
                                FLATTEN(Recsys__FollowPaths(two_step_nhoods, $paths))
                                AS (item_A: int, local_paths: bytearray);
    $item_nhoods        =   FOREACH (GROUP three_steps BY item_A) GENERATE
                                group AS item_A,
                                FLATTEN(com.mortardata.pig.recsys.SelectShortestPaths(
                                    $final_nhood_size, $1.local_paths)
                                ) AS (item_B, dist, steps);
};

/*
 * shortest_paths: { item_A: int, item_B: int, dist: float, steps: int }
 * neighborhood_size: int
 * -->
 * item_nhoods: { item_A: int item_B: int, weight: float, steps: int, rank: int }
 */
DEFINE Recsys__PostprocessShortestPaths(shortest_paths, neighborhood_size)
RETURNS item_nhoods {
    no_self_loops       =   FILTER $shortest_paths BY item_A != item_B;

    nhoods_tmp          =   FOREACH (GROUP no_self_loops BY item_A) {
                                ordered = ORDER $1 BY dist ASC;
                                    top = LIMIT ordered $neighborhood_size;
                                GENERATE FLATTEN(Recsys__Enumerate(top))
                                      AS (item_A, item_B, dist, steps, rank);
                            }

    $item_nhoods        =   FOREACH nhoods_tmp GENERATE
                                item_A, item_B, 1.0f / dist AS weight, steps, rank;
};

----------------------------------------------------------------------------------------------------

/*
 * Step 4 for the default collaborative filter.
 *
 * Steps 1-3 gave us item-to-item recommendations.
 * Steps 4-6 will use those item-to-item recommendations to generate personalized recommendations.
 * This macro takes links between users and items, and the item-to-item recommendations,
 * and generates "user neighborhoods" consisting of all the items recommended for any item
 * the user has a link to.
 *
 * The "reason" for a recommendation is the item that a user was linked to that caused
 * the neighboring item to be recommended. "reason_flag" can contain additional
 * metadata about why the item was recommended: for example, in the Github Recommender,
 * if a user was linked to a fork of a repo, we wanted to give recommendations based
 * on the original repo that that forked, since that would have more data.
 * The reason_flag field was used to note when we were doing this mapping,
 * so the user could be informed of this on the front-end.
 *
 * ui_scores:   {user: int, item: int, score: float, reason_flag: int/chararray}
 * item_nhoods: {item: int, neighbor: int, score: float}
 * -->
 * user_nhoods: {user: int, item: int, score: float, reason: int, reason_flag: int/chararray} 
 */
DEFINE Recsys__UserItemNeighborhoods(ui_scores, item_nhoods)
RETURNS user_nhoods {
    user_nhoods_tmp =   FOREACH (JOIN $ui_scores BY item, $item_nhoods BY item) GENERATE
                                              user AS user,
                            $item_nhoods::neighbor AS item,
                            (float) SQRT($ui_scores::score *
                                         $item_nhoods::score) AS score,
                                $item_nhoods::item AS reason,
                                       reason_flag AS reason_flag;

    -- hack to get around a NullPointerException bug in the TOP builtin UDF
    $user_nhoods    =   FOREACH (GROUP user_nhoods_tmp BY
                                 ((((long) (user + item) * (long) (user + item + 1)) / 2) + item)) {
                            sorted = ORDER user_nhoods_tmp BY score DESC;
                            best   = LIMIT sorted 1;
                            GENERATE FLATTEN(best) AS (user, item, score, reason, reason_flag);
                        }
};

/*
 * Step 5 in the default collaborative filter.
 *
 * This macro filters the output of Step 4 (Recsys__UserItemNeighborhoods)
 * so that a user is never shown an item they are known to have already seen.
 * You may also wish to apply your own domain-specific filters to the user neighborhoods.
 *
 * user_nhoods: {user: int, item: int, score: float, reason: int, reason_flag: int/chararray}
 * ui_scores:   {user: int, item: int, score: float, reason_flag: int/chararray}
 *              or just {user: int, item: int} the other fields are optional
 * -->
 * user_nhoods_filt: {user: int, item: int, score: float, reason: int, reason_flag: int/chararray}
 */
DEFINE Recsys__FilterItemsAlreadySeenByUser(user_nhoods, ui_scores)
RETURNS filtered {
    joined      =   JOIN $user_nhoods BY (user, item) LEFT OUTER,
                         $ui_scores   BY (user, item);
    $filtered   =   FOREACH (FILTER joined BY $ui_scores::item IS null) GENERATE
                               $user_nhoods::user AS user,
                               $user_nhoods::item AS item,
                              $user_nhoods::score AS score,
                             $user_nhoods::reason AS reason,
                        $user_nhoods::reason_flag AS reason_flag;
};

/*
 * Step 6 of the default collaborative filter.
 *
 * Takes the top N recommendations from a user's neighborhood after any filtering has been applied.
 *
 * user_nhoods: {user: int, item: int, score: float, reason: int, reason_flag: int/chararray}
 * num_recs:    int
 * --> 
 * user_recs: {user: int, item: int, score: float, reason: int, reason_flag: int/chararray}
 */
DEFINE Recsys__TopNUserRecs(user_nhoods, num_recs)
RETURNS user_recs {
    $user_recs  =   FOREACH (GROUP $user_nhoods BY user) {
                        sorted = ORDER $user_nhoods BY score DESC;
                        best   = LIMIT sorted $num_recs;
                        GENERATE FLATTEN(best) AS (user, item, score, reason, reason_flag);
                    }
};

/*
 * This is a utility to return the output of the default collaborative filter
 * from integer ids to names for debugging.
 *
 * user_nhoods: {user: int, item: int, score: float, reason: int, reason_flag: int/chararray}
 * user_names:  {id: int, name: chararray}
 * item_names:  {id: int, name: chararray}
 * -->
 * with_names:  {user: chararray, item: chararray, score: float, reason: chararray, reason_flag: int/chararray}
 */
DEFINE Recsys__UserRecsIntegerIdsToNames(user_nhoods, user_names, item_names)
RETURNS with_names {
    join_1      =   FOREACH (JOIN $user_names BY id, $user_nhoods BY user) GENERATE
                        name AS user, item AS item, score AS score,
                        reason AS reason, reason_flag AS reason_flag;    
    join_2      =   FOREACH (JOIN $item_names BY id, join_1 BY item) GENERATE
                        user AS user, name AS item, score AS score,
                        reason AS reason, reason_flag AS reason_flag;
    $with_names =   FOREACH (JOIN $item_names BY id, join_2 BY reason) GENERATE
                        user AS user, item AS item, score AS score,
                        name AS reason, reason_flag AS reason_flag;
};

----------------------------------------------------------------------------------------------------

/*
 * Modifications of the II neighborhoods -> UI recommendations steps of the collab filter
 * to better handle cases where there is low variance in the user-item scores,
 * such as for video sharing where a user shares a video almost always only once.
 */

/*
 * Same as Recsys__UserItemNeighborhoods except
 *    1) Do not take a sqrt when multiplying ui_scores::score by item_nhoods::score by score
 *    2) Instead of taking the best link to an item in a user's neighborhood,
 *       sum up the weight of all links to that item.
 *    3) output top 2 reasons instead of 1
 *
 * These changes are conjectured to be more effective when the variance of the ui scores
 * is very low (i.e. every signal has the same or almost the same weight);
 *
 * ui_scores:   {user: int, item: int, score: float}
 * item_nhoods: {item: int, neighbor: int, score: float}
 * -->
 * user_nhoods: {user: int, item: int, score: float, reason_1: int, reason_2: int} 
 */
DEFINE Recsys__UserItemNeighborhoods_LowUIScoreVariance(ui_scores, item_nhoods)
RETURNS user_nhoods {
    user_nhoods_tmp =   FOREACH (JOIN $ui_scores BY item, $item_nhoods BY item) GENERATE
                                              user AS user,
                            $item_nhoods::neighbor AS item,
                             ($ui_scores::score *
                              $item_nhoods::score) AS score,
                                $item_nhoods::item AS reason;

    $user_nhoods    =   FOREACH (GROUP user_nhoods_tmp BY (user, item)) GENERATE
                                FLATTEN(group) AS (user, item),
                                (float) SUM($1.score) AS score,
                                FLATTEN(recsys_udfs.top_two_reasons__int($1.(score, reason)))
                                AS (reason_1, reason_2);
};

/*
 * Same as Recsys__FilterItemsAlreadySeenByUser except that it supports
 * input with 2 reason fields instead of 1
 *
 * user_nhoods: {user: int, item: int, score: float, reason: int, reason_2: int}
 * ui_scores:   {user: int, item: int, ...}
 * -->
 * user_nhoods_filt: {user: int, item: int, score: float, reason: int, reason_2: int}
 */
DEFINE Recsys__FilterItemsAlreadySeenByUser_TwoReasonFields(user_nhoods, ui_scores)
RETURNS filtered {
    joined      =   JOIN $user_nhoods BY (user, item) LEFT OUTER,
                         $ui_scores   BY (user, item);
    $filtered   =   FOREACH (FILTER joined BY $ui_scores::item IS NULL) GENERATE
                            $user_nhoods::user AS user,
                            $user_nhoods::item AS item,
                           $user_nhoods::score AS score,
                        $user_nhoods::reason_1 AS reason_1,
                        $user_nhoods::reason_2 AS reason_2;
};
