%default LOGISTIC_PARAM 1
%default MIN_LINK_WEIGHT 0.0
%default MAX_LINKS_PER_USER 5

IMPORT 'recsys.pig';

ui_signals  =   LOAD '../input/test_recsys_ui_to_ii.tsv' USING PigStorage()
                AS (user: int, item: int, score: float);

ii_links, ii_scores =   Recsys__UISignals_To_IILinks(
                            ui_signals,
                            $LOGISTIC_PARAM,
                            $MIN_LINK_WEIGHT,
                            $MAX_LINKS_PER_USER
                        );

DUMP ii_links;