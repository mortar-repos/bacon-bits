%default INPUT_PATH  '../example_input/100_random_points.txt'
%default OUTPUT_PATH '../example_output/kmeans'

REGISTER 'datafu-0.0.10.jar';
DEFINE Enumerate datafu.pig.bags.Enumerate('1');

IMPORT '../macros/clustering.pig';

----------------------------------------------------------------------------------------------------

points              =   LOAD '$INPUT_PATH' USING PigStorage()
                        AS (x: double, y: double);

points_enum         =   FOREACH (GROUP points ALL) GENERATE
                            FLATTEN(Enumerate(points))
                            AS (x, y, i);

-- use the points to similate items with dense feature vectors
items               =   FOREACH points_enum GENERATE
                            i AS item,
                            TOTUPLE(x, y) AS features;

----------------------------------------------------------------------------------------------------

pivots              =   Clustering__KMeans_ChoosePivots(items);

clusters_1          =   Clustering__KMeans_Clusters(items, pivots);
cluster_stats_1     =   Clustering__ClusterStats(clusters_1);

pivots_it_1         =   Clustering__KMeans_Iterate(items, pivots);
pivots_it_2         =   Clustering__KMeans_Iterate(items, pivots_it_1);
pivots_it_3         =   Clustering__KMeans_Iterate(items, pivots_it_2);
pivots_it_4         =   Clustering__KMeans_Iterate(items, pivots_it_3);
pivots_it_5         =   Clustering__KMeans_Iterate(items, pivots_it_4);
pivots_it_6         =   Clustering__KMeans_Iterate(items, pivots_it_5);
pivots_it_7         =   Clustering__KMeans_Iterate(items, pivots_it_6);
pivots_it_8         =   Clustering__KMeans_Iterate(items, pivots_it_7);
pivots_it_9         =   Clustering__KMeans_Iterate(items, pivots_it_8);
pivots_it_10        =   Clustering__KMeans_Iterate(items, pivots_it_9);

clusters_2          =   Clustering__KMeans_Clusters(items, pivots_it_10);
cluster_stats_2     =   Clustering__ClusterStats(clusters_2);

----------------------------------------------------------------------------------------------------

rmf $OUTPUT_PATH/clusters_1;
rmf $OUTPUT_PATH/cluster_stats_1;
rmf $OUTPUT_PATH/clusters_2;
rmf $OUTPUT_PATH/cluster_stats_2;

STORE clusters_1      INTO '$OUTPUT_PATH/clusters_1'      USING PigStorage();
STORE cluster_stats_1 INTO '$OUTPUT_PATH/cluster_stats_1' USING PigStorage();
STORE clusters_2      INTO '$OUTPUT_PATH/clusters_2'      USING PigStorage();
STORE cluster_stats_2 INTO '$OUTPUT_PATH/cluster_stats_2' USING PigStorage();
