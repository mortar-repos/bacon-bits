%default NEIGHBORHOOD_SIZE 5

IMPORT 'recsys.pig';

ii_links        =   LOAD '../input/test_recsys_shortest_paths.tsv' USING PigStorage()
                    AS (item_A: int, item_B: int, weight: float);

--item_nhoods     =   Recsys__IILinksShortestPathsTwoSteps(ii_links, $NEIGHBORHOOD_SIZE, $NEIGHBORHOOD_SIZE);
item_nhoods     =   Recsys__IILinksShortestPathsThreeSteps(ii_links, $NEIGHBORHOOD_SIZE, $NEIGHBORHOOD_SIZE, $NEIGHBORHOOD_SIZE);

DUMP item_nhoods;
