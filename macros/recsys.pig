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

IMPORT 'matrix.pig';
IMPORT 'graph.pig';
IMPORT 'normalization.pig';

%default RECSYS_KNN_K 10
DEFINE Recsys__FromPigCollectionToBag         com.mortardata.pig.collections.FromPigCollectionToBag();
DEFINE Recsys__KNearestNeighbors              com.mortardata.pig.geometry.KNearestNeighbors('$RECSYS_KNN_K');
DEFINE Recsys__LoadBalancingReducerAllocation com.mortardata.pig.partitioners.LoadBalancingReducerAllocation();
DEFINE Recsys__MergeAndKeepTopN               com.mortardata.pig.collections.MergeAndKeepTopN('$RECSYS_KNN_K');
DEFINE Recsys__ReplicateByKey                 com.mortardata.pig.collections.ReplicateByKey();

REGISTER 'datafu-0.0.10.jar';
DEFINE Recsys__Enumerate datafu.pig.bags.Enumerate('1');

REGISTER 'bacon-bits-0.1.0.jar';

REGISTER 'recsys.py' USING jython AS recsys_udfs;

----------------------------------------------------------------------------------------------------
/*
 * This file contains macros for building recommender systems.
 *
 * The basic pipline for a collaborative filter goes:
 *    1) Recsys__UIScores_To_IILinks
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
 * Optionally to help you generate the User-Item scores that the CF takes as input, there are the macros:
 *    1) Recsys__AggregateUIScoresAndApplyLogisticScale
 *    2) Recsys__LimitSignalsPerUser
 *
 * See the comments for each macro for more details.
 */
----------------------------------------------------------------------------------------------------

/*
 * Step 1 of the default collaborative filter.
 *
 * Given a relation with weighted mappings of users to items
 * (representing how much evidence there is that the user likes that item,
 *  assumed to be 0 if a user-item pair is not present in the relation),
 * contstructs a similarity matrix between items.
 * 
 * Algorithmically, this is "contracting" the bipartite user-item graph
 * into a regular graph of item similarities. If a user U has affinity
 * with items I1 and I2 of weights W1 and W2 respectively, than a link
 * between I1 and I2 is formed with the weight MIN(W1, W2).
 *
 * ui_scores: {user: int, item: int, score: float}
 * min_link_weight: int/float, item-item links of less weight than this
 *                             will be filtered out for better performance
 * -->
 * ii_links: {row: int, col: int, float: val}
 */
DEFINE Recsys__UIScores_To_IILinks(ui_scores, min_link_weight)
RETURNS ii_links {
    ii_link_terms   =   Recsys__UIScores_To_IITerms($ui_scores);
    $ii_links       =   Recsys__IITerms_To_IILinks(ii_link_terms, $min_link_weight);
};

/*
 * Helper macro for Recsys__UIScores_To_IILinks
 *
 * ui_scores: {user: int, item: int, score: float}
 * -->
 * ii_link_terms: {row: int, col: int, val: float}
 */
DEFINE Recsys__UIScores_To_IITerms(ui_scores)
RETURNS ii_link_terms {
    ui_copy         =   FOREACH $ui_scores GENERATE *;
    ui_joined       =   JOIN    $ui_scores BY user, ui_copy BY user;
    ui_filtered     =   FILTER   ui_joined BY $ui_scores::item != ui_copy::item;

    $ii_link_terms  =   FOREACH ui_filtered GENERATE
                            $ui_scores::item AS row,
                            ui_copy::item    AS col,
                            ($ui_scores::score < ui_copy::score ?
                                $ui_scores::score : ui_copy::score
                            ) AS val;
};

/*
 * Helper macro for Recsys__UIScores_To_IILinks
 *
 * ii_link_terms: {row: int, col: int, val: float}
 * min_link_weight: int/float
 * -->
 * ii_links: {row: int, col: int, val: float}
 */
DEFINE Recsys__IITerms_To_IILinks(ii_link_terms, min_link_weight)
RETURNS ii_links {
    agg_ii_links    =   FOREACH (GROUP $ii_link_terms BY (row, col)) GENERATE
                            FLATTEN(group) AS (row, col),
                            (float) SUM($ii_link_terms.val) AS val;

    $ii_links       =   FILTER agg_ii_links BY val >= $min_link_weight;
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
 * ii_links_raw: {row: int, col: int, val: float}
 * bayesian_prior: int/float
 * -->
 * ii_links_bayes: {row: int, col: int, val: float}
 */
DEFINE Recsys__IILinksRaw_To_IILinksBayes(ii_links_raw, bayesian_prior)
RETURNS ii_links_bayes {
    item_totals     =   FOREACH (GROUP $ii_links_raw BY col) GENERATE
                            group AS item,
                            (float) SUM($ii_links_raw.val) AS total;

    $ii_links_bayes =   FOREACH (JOIN item_totals BY item, $ii_links_raw BY col) GENERATE
                            $ii_links_raw::row AS row,
                            $ii_links_raw::col AS col,
                            (float) ($ii_links_raw::val / (item_totals::total + $bayesian_prior)) AS val;
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
 * ii_links: {row: int, col: int, val: float}
 * neighborhood_size: int
 * -->
 * ii_nhoods: {row: int, col: int, val: float}
 */
DEFINE Recsys__IILinksShortestPathsTwoSteps(ii_links, neighborhood_size)
RETURNS ii_nhoods {
    distance_mat    =   FOREACH $ii_links  GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp      =   Graph__ShortestPaths_TwoSteps(distance_mat, $neighborhood_size);
    nhoods_tmp_inv  =   FOREACH nhoods_tmp GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp_norm =   Normalization__LinearTransform(nhoods_tmp_inv, 'val', 'row, col');
    $ii_nhoods      =   FOREACH nhoods_tmp_norm GENERATE row, col, (float) val AS val;
};

/*
 * See notes for Recsys__IILinksShortestPathsTwoSteps.
 *
 * ii_links: {row: int, col: int, val: float}
 * neighborhood_size: int
 * -->
 * ii_nhoods: {row: int, col: int, val: float}
 */
DEFINE Recsys__IILinksShortestPathsThreeSteps(ii_links, neighborhood_size)
RETURNS ii_nhoods {
    distance_mat    =   FOREACH $ii_links  GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp      =   Graph__ShortestPaths_ThreeSteps(distance_mat, $neighborhood_size);
    nhoods_tmp_inv  =   FOREACH nhoods_tmp GENERATE row, col, 1.0f / val AS val;
    nhoods_tmp_norm =   Normalization__LinearTransform(nhoods_tmp_inv, 'val', 'row, col');
    $ii_nhoods      =   FOREACH nhoods_tmp_norm GENERATE row, col, (float) val AS val;
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
 * 1) aggregates all scores for each unique user-item pair
 * 2) applies a logistic scale so that user-item pairs with huge scores face diminishing returns
 *    (maps all scores into the range [0, 1])
 *
 * ui_scores: {user: int, item: int, score: float}
 * logistic_param: float
 * -->
 * out: {user: int, item: int, score: float}
 */
DEFINE Recsys__AggregateUIScoresAndApplyLogisticScale(ui_scores, logistic_param)
RETURNS out {
    tmp         =   FOREACH (GROUP $ui_scores BY (user, item)) GENERATE
                        FLATTEN(group) AS (user, item),
                        SUM($1.score) AS score;
    $out        =   FOREACH tmp GENERATE
                        user, item,
                        recsys_udfs.logistic_scale(score, $logistic_param) AS score;
};

/*
 * The default collaborative filter algorithm is O([max num edges per user]^2).
 * If any single user has thousands of edges, this will result in huge reducer skew.
 * This macro only takes a certain maximum number of signals from these outlier users
 * for purposes of graph generation. Once you have item-item recs from the CF, you can still
 * give the outlier users recommendations based on the unfiltered ui_scores that was the input
 * to this macro.
 *
 * TODO: make this take the most recent signals instead of just a random sample
 *
 * ui_scores: {user: int, item: int, score: float}
 * max_signals_per_user: int
 * -->
 * out: {user: int, item: int, score: float}
 */
DEFINE Recsys__LimitSignalsPerUser(ui_scores, max_signals_per_user)
RETURNS out {
    DEFINE Recsys__ResevoirSample com.mortardata.pig.sampling.ResevoirSample('$max_signals_per_user');
    $out        =   FOREACH (GROUP ui_scores BY user) GENERATE
                        FLATTEN(Recsys__ResevoirSample($1))
                        AS (user, item, score);
};

----------------------------------------------------------------------------------------------------

-- Experimental KNN implementation for content-based filtering

DEFINE Recsys__KNearestNeighbors(items)
RETURNS global_knns { 
    replicated      =   FOREACH $items GENERATE
                            id, FLATTEN(ReplicateByKey(features)) AS (key, features);

    by_key          =     GROUP replicated BY key;
    by_key          =    FILTER by_key BY COUNT($1) > 1;
    partition_knns  =   FOREACH by_key GENERATE
                            group AS partition_id,
                            FLATTEN(KNearestNeighbors(replicated.(id, features)))
                            AS (item_id, knn);

    $global_knns    =   FOREACH (GROUP partition_knns BY item_id) GENERATE
                            group AS row,              -- document
                            FLATTEN(FromPigCollectionToBag(MergeAndKeepTopN(partition_knns.knn)))
                            AS (col: int, val: float); -- neighbor, distance
};

DEFINE Recsys__KNearestNeighbors_ReducerLoadBalanced(items)
RETURNS global_knns { 
    replicated      =   FOREACH $items GENERATE
                        id, FLATTEN(ReplicateByKey(features)) AS (key, features);

    -- TODO: optionally take a sample of replicated so reducer_allocs doesn't take as long

    key_counts      =   FOREACH (GROUP replicated BY key) GENERATE
                            group AS key, COUNT($1) AS count;
    key_counts      =   FILTER key_counts BY count > 1;

    reducer_allocs  =   FOREACH (GROUP key_counts ALL) GENERATE
                            FLATTEN(com.mortardata.pig.partitioners.LoadBalancingReducerAllocation($1))
                            AS (key, reducer);
    joined          =   FOREACH (JOIN reducer_allocs BY key, replicated BY key) GENERATE
                            replicated::id          AS id,
                            reducer_allocs::key     AS key,
                            reducer_allocs::reducer AS reducer,
                            replicated::features    AS features;

    balanced_groups =   GROUP joined BY (reducer, key)
                        PARTITION BY com.mortardata.pig.partitioners.PrecalculatedPartitioner;
    partition_knns  =   FOREACH balanced_groups GENERATE
                            group.key AS partition_id,
                            FLATTEN(KNearestNeighbors(joined.(id, features)))
                            AS (item_id, knn);

    $global_knns    =   FOREACH (GROUP partition_knns BY item_id) GENERATE
                            group AS row,              -- item
                            FLATTEN(FromPigCollectionToBag(MergeAndKeepTopN(partition_knns.knn)))
                            AS (col: int, val: float); -- neighbor, distance
};

----------------------------------------------------------------------------------------------------

-- Experimental system to automatically assign weights to (user, item, signal) triples
-- to use in the collaborative filter

/*
 * Estimate appropriate weights for each (user, item, signal) triple
 * based on three factors:
 *     1) the relative frequencies of the signals across all users
 *     2) the relative frequencies of the signals for each specific user
 *     3) the proportion of users who have at least one instance of a given signal
 *
 * (1) is the basic principle: rare events are usually more important than frequent ones
 * (2) accounts for the fact that the same signal can be more or less significant
 *     depending on the particular user's habits
 * (3) accounts for the fact that some signals may be rare not because they require
 *     real user engagement, but rather because they are unappealing to users
 *     for some other reason, such as having a bad user interface, or being
 *     a generally unsuitable feature for the product space.
 *
 * ui_signals: { user, item, signal }
 * prior_quantile: String in the for 'qNN', where NN is a multiple of 5,
 *                 i.e. q50 for median, q25 for 25th percentile.
 *                 This is a Bayesian prior which allows better estimation of signal weights
 *                 for users with small sample sizes of events (not much engagement).
 * -->
 * final_sig_weights: { user, signal, weight }
 */
DEFINE Recsys__WeightSignals(ui_signals, prior_quantile)
RETURNS final_sig_weights {
    -- ind the "sample size" for each user
    user_totals         =   FOREACH (GROUP $ui_signals BY user) GENERATE
                                group AS user, COUNT($1) AS total;

    -- Count the number of signals for each (user, signal type) pair
    user_sig_counts     =   FOREACH (GROUP $ui_signals BY (user, signal)) GENERATE
                                FLATTEN(group) AS (user, signal),
                                COUNT($1) AS count;

    -- Join the signal counts with the sample sizes and disambiguate aliases
    user_sig_counts     =   FOREACH (JOIN user_totals BY user, user_sig_counts BY user) GENERATE
                                $2 AS user,  $3 AS signal,
                                $4 AS count, $1 AS total;

    -- The number of signals that will be observer for a given (user, signal type) pair
    -- after an arbitary number of events is modelled as a binomial random variable.
    -- To estimate the probability parameter of this binomial, we use a beta-distributed prior,
    -- where the hyperparameters alpha and beta are empirically derived.
    --
    -- * alpha is the median (or some other quantile) number of signals across all (user, signal type) pairs
    -- * beta is the median (or some other quantile) total number of signals across all users
    DEFINE Recsys__WeightSignals_Quantiles datafu.pig.stats.StreamingQuantile('21');

    prior_alpha         =   FOREACH (GROUP user_sig_counts ALL) GENERATE
                                FLATTEN(Recsys__WeightSignals_Quantiles($1.count)) AS (
                                     q0,  q5, q10, q15, q20, q25, q30, q35, q40, q45,
                                    q50, q55, q60, q65, q70, q75, q80, q85, q90, q95, q100
                                );
    prior_alpha         =   FOREACH prior_alpha GENERATE $prior_quantile AS alpha;

    prior_beta          =   FOREACH (GROUP user_totals ALL) GENERATE
                                FLATTEN(Recsys__WeightSignals_Quantiles($1.total)) AS (
                                    q00, q05, q10, q15, q20, q25, q30, q35, q40, q45,
                                    q50, q55, q60, q65, q70, q75, q80, q85, q90, q95, q100
                                );
    prior_beta          =   FOREACH prior_beta GENERATE $prior_quantile - prior_alpha.alpha AS beta;

    -- weight(user, signal) = 1 / P(arbitrary_event == signal | user)
    bayes_sig_weights   =   FOREACH user_sig_counts GENERATE
                                user, signal,
                                (float) (total + prior_alpha.alpha) /
                                (float) (count + prior_alpha.alpha + prior_beta.beta) AS weight;

    -- Multiply each signal weight by a "coverage score":
    -- the fraction of users who have at least one instance of this signal.
    num_users           =   FOREACH (GROUP user_sig_counts ALL) GENERATE COUNT($1) AS count;
    sig_coverage        =   FOREACH (GROUP user_sig_counts BY signal) GENERATE
                                group AS signal,
                                (float) COUNT($1) / (float) num_users.count AS coverage;
    $final_sig_weights  =   FOREACH (JOIN sig_coverage BY signal, bayes_sig_weights BY signal) GENERATE
                                bayes_sig_weights::user AS user,
                                bayes_sig_weights::signal AS signal,
                                weight * coverage AS weight;
};

/*
 * Evaluate/characterize weights generated by Recsys__WeightSignals
 *  
 * ui_signals:  {user, item, signal}
 * sig_weights: {user, signal, weight}
 * -->
 * sig_stats: {signal, fraction_of_events, fraction_of_total_weight}
 */
DEFINE Recsys__EvaluateSignalWeights(ui_signals, sig_weights)
RETURNS sig_stats {
    ui_sigs_w_weights   =   FOREACH (JOIN $sig_weights BY (user, signal), $ui_signals BY (user, signal)) GENERATE
                                ui_signals::user AS user,
                                ui_signals::item AS item,
                                ui_signals::signal AS signal,
                                $sig_weights::weight AS weight;

    global_stats        =   FOREACH (GROUP ui_sigs_w_weights ALL) GENERATE
                                COUNT($1) as count, SUM($1.weight) AS weight;

    $sig_stats          =   FOREACH (GROUP ui_sigs_w_weights BY signal) GENERATE
                                group AS signal,
                                (float) COUNT($1) / (float) global_stats.count AS fraction_of_events,
                                (float) (SUM($1.weight) / global_stats.weight) AS fraction_of_total_weight;
};

----------------------------------------------------------------------------------------------------

/*
 * These macros are modifications of the standard collaborative filtering macros
 * that support having "reasons" for each item-to-item recommendation.
 * The input ii_links are (row, col, val, reason) instead of (row, col, val).
 * The final output is (row, col, val, reason_1, reason_2),
 * reason_1 is the reason from the primary signal that caused the recommendation to be made,
 * and reason_2 is the reason from the second most prominent signal for that item-item pair.
 */

/*
 * ii_link_terms: {row: int, col: int, val: float, reason: chararray}
 * min_link_weight: float
 * -->
 * ii_links: {row: int, col: int, val: float, reason: chararray}
 */
DEFINE Recsys__IITerms_To_IILinks_With_Reasons(ii_link_terms, min_link_weight)
RETURNS ii_links {
    agg_ii_links    =   FOREACH (GROUP $ii_link_terms BY (row, col)) GENERATE
                            FLATTEN(group) AS (row, col),
                            (float) SUM($ii_link_terms.val) AS val,
                            FLATTEN(recsys_udfs.top_two_reasons($ii_link_terms.(val, reason)))
                            AS (reason_1, reason_2);

    $ii_links       =   FILTER agg_ii_links BY val >= $min_link_weight;
};

/*
 * ii_links_raw: {row: int, col: int, val: float, reason: chararray}
 * bayesian_prior: float
 * -->
 * ii_links_bayes: {row: int, col: int, val: float, reason: chararray}
 */
DEFINE Recsys__IILinksRaw_To_IILinksBayes_With_Reasons(ii_links_raw, bayesian_prior)
RETURNS ii_links_bayes {
    item_totals     =   FOREACH (GROUP $ii_links_raw BY col) GENERATE
                            group AS item,
                            (float) SUM($ii_links_raw.val) AS total;

    $ii_links_bayes =   FOREACH (JOIN item_totals BY item, $ii_links_raw BY col) GENERATE
                                 $ii_links_raw::row AS row,
                                 $ii_links_raw::col AS col,
                            (float) ($ii_links_raw::val / (item_totals::total + $bayesian_prior)) AS val,
                            $ii_links_raw::reason_1 AS reason_1,
                            $ii_links_raw::reason_2 AS reason_2;
};

/*
 * mat: {row: int, col: int, val: float, reason: chararray}
 * order_direction: 'ASC' or 'DESC'
 * max_elems_per_row: int
 * -->
 * trimmed: {row: int, col: int, val: float, reason: chararray}
 */
DEFINE Recsys__TrimRows_With_Reasons(mat, order_direction, max_elems_per_row)
RETURNS trimmed {
    $trimmed    =   FOREACH (GROUP $mat BY row) {
                        ordered = ORDER $mat BY val $order_direction;
                        top     = LIMIT ordered $max_elems_per_row;
                        GENERATE FLATTEN(top) AS (row, col, val, reason_1, reason_2);
                    }
};

/*
 * ii_links: {row: int, col: int, val: float, reason: chararray}
 * neighborhood_size: int
 * -->
 * ii_nhoods: {row: int, col: int, val: float, reason_1: chararray, reason_2: chararray}
 */
DEFINE Recsys__IILinksShortestPathsTwoSteps_With_Reasons(ii_links, neighborhood_size)
RETURNS ii_nhoods {
    distance_mat    =   FOREACH $ii_links GENERATE row, col, 1.0f / val AS val, reason_1, reason_2;
    
    from_vertices   =   FOREACH distance_mat GENERATE row AS id;
    to_vertices     =   FOREACH distance_mat GENERATE col AS id;
    vertices_dups   =   UNION from_vertices, to_vertices;
    vertices        =   DISTINCT vertices_dups;
    self_loops      =   FOREACH vertices GENERATE
                            id AS row, id AS col, 0.0f AS val,
                            NULL AS reason_1, NULL AS reason_2;

    graph           =   UNION self_loops, distance_mat;
    copy            =   FOREACH graph GENERATE *;
    sq_terms        =   FOREACH (JOIN graph BY col, copy BY row) GENERATE
                            graph::row AS row,
                             copy::col AS col,
                            graph::val + copy::val AS val,
                            -- is copy a self-loop?
                            (copy::row == copy::col ?
                                graph::reason_1 :
                                -- is graph a self-loop?
                                (graph::row == graph::col ? copy::reason_1 : 'GRAPH')
                            ) AS reason_1,
                            -- is copy a self-loop?
                            (copy::row == copy::col ?
                                graph::reason_2 :
                                -- is graph a self-loop?
                                (graph::row == graph::col ? copy::reason_2 : NULL)
                            ) AS reason_2;
    squared         =   FOREACH (GROUP sq_terms BY (row, col)) GENERATE
                            FLATTEN(group) AS (row, col),
                            FLATTEN(recsys_udfs.shortest_path(sq_terms.(val, reason_1, reason_2)))
                            AS (val, reason_1, reason_2);

    no_self_loops   =   FILTER squared BY row != col;
    nhoods_tmp      =   Recsys__TrimRows_With_Reasons(no_self_loops, 'ASC', $neighborhood_size);

    nhoods_tmp_inv  =   FOREACH nhoods_tmp GENERATE row, col, 1.0f / val AS val, reason_1, reason_2;
    nhoods_tmp_norm =   Normalization__LinearTransform(nhoods_tmp_inv, 'val', 'row, col, reason_1, reason_2');
    $ii_nhoods      =   FOREACH nhoods_tmp_norm GENERATE row, col, (float) val AS val, reason_1, reason_2;
};

/*
 * mat: {row: int, col: int, val: float, reason_1: chararray, reason_2: chararray}
 * order_direction: 'ASC' or 'DESC'
 * -->
 * ranked: {row: int, col: int, val: float, reason_1: chararray, reason_2: chararray, rank: int}
 */
DEFINE Recsys__RankRows_With_Reasons(mat, order_direction)
RETURNS ranked {
    $ranked         =   FOREACH (GROUP $mat BY row) {
                            ordered = ORDER $mat BY val $order_direction;
                            GENERATE FLATTEN(Recsys__Enumerate(ordered))
                                     AS (row, col, val, reason_1, reason_2, rank);
                        }
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
 *    2) instead of taking the best link to an item in a user's neighborhood,
 *       sum up the weight of all links to that item.
 *    3) output top 2 reasons instead of 1
 *
 * These changes are conjectured to be more effective when the variance of the ui scores
 * is very low (i.e. every signal has the same or almost the same weight);
 *
 * ui_scores:   {user: int, item: int, score: float, reason_flag: int/chararray}
 * item_nhoods: {item: int, neighbor: int, score: float}
 * -->
 * user_nhoods: {user: int, item: int, score: float, reason_1: int, reason_2: int, reason_flag: int/chararray} 
 */
DEFINE Recsys__UserItemNeighborhoods_LowUIScoreVariance(ui_scores, item_nhoods)
RETURNS user_nhoods {
    user_nhoods_tmp =   FOREACH (JOIN $ui_scores BY item, $item_nhoods BY item) GENERATE
                                              user AS user,
                            $item_nhoods::neighbor AS item,
                             ($ui_scores::score *
                              $item_nhoods::score) AS score,
                                $item_nhoods::item AS reason,
                                       reason_flag AS reason_flag;

    $user_nhoods    =   FOREACH (GROUP user_nhoods_tmp BY (user, item)) {
                            top_2 = TOP(2, 2, $1);
                            GENERATE FLATTEN(group) AS (user, item),
                                      SUM($1.score) AS score,
                                      FLATTEN(top_2.reason) AS (reason_1, reason_2);

};

/*
 * Same as Recsys__FilterItemsAlreadySeenByUser except that it supports
 * input with 2 reason fields instead of 1
 *
 * user_nhoods: {user: int, item: int, score: float, reason: int, reason_2: int, reason_flag: int/chararray}
 * ui_scores:   {user: int, item: int, ...}
 * -->
 * user_nhoods_filt: {user: int, item: int, score: float, reason: int, reason_2: int, reason_flag: int/chararray}
 */
DEFINE Recsys__FilterItemsAlreadySeenByUser_TwoReasonFields(user_nhoods, ui_scores)
RETURNS filtered {
    joined      =   JOIN $user_nhoods BY (user, item) LEFT OUTER,
                         $ui_scores   BY (user, item);
    $filtered   =   FOREACH (FILTER joined BY $ui_scores::item IS null) GENERATE
                               $user_nhoods::user AS user,
                               $user_nhoods::item AS item,
                              $user_nhoods::score AS score,
                           $user_nhoods::reason_1 AS reason_1,
                           $user_nhoods::reason_2 AS reason_2,
                        $user_nhoods::reason_flag AS reason_flag;
};