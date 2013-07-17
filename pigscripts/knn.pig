%default INPUT_PATH  '../example_output/538_tfidf'
%default OUTPUT_PATH '../example_output/538_knn'
%default KNN_K 5

REGISTER 'bacon-bits-0.1.0.jar';
IMPORT 'matrix.pig';
IMPORT 'recsys.pig';

DEFINE ToPigCollection        com.mortardata.pig.collections.ToPigCollection();
DEFINE ReplicateByKey         com.mortardata.pig.collections.ReplicateByKey();
DEFINE KNearestNeighbors      com.mortardata.pig.geometry.KNearestNeighbors('$KNN_K');
DEFINE MergeAndKeepTopN       com.mortardata.pig.collections.MergeAndKeepTopN('$KNN_K');
DEFINE FromPigCollectionToBag com.mortardata.pig.collections.FromPigCollectionToBag();

----------------------------------------------------------------------------------------------------

items   =   LOAD '$INPUT_PATH/tfidf/*' USING PigStorage()
            AS (id: int, features: {t: (term: chararray, tfidf: float)});

items   =   FOREACH items GENERATE id, ToPigCollection(features);

id_map  =   LOAD '$INPUT_PATH/id_map/*' USING PigStorage()
                AS (id: int, name: chararray);

knns    =   Recsys__KNearestNeighbors_ReducerLoadBalanced(items);
out     =   Matrix__VisualizeByRow(knns, id_map, 'ASC', $KNN_K);

----------------------------------------------------------------------------------------------------

rmf $OUTPUT_PATH;
STORE out INTO '$OUTPUT_PATH' USING PigStorage();
