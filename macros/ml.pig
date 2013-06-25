REGISTER '../vendor/baconbits/udfs/jython/utils.py' USING jython AS ml_jython_utils;

DEFINE ML__CosineSimilarityMatrix(features)
RETURNS cosine_mat {
    copy            =   FOREACH $features GENERATE *;

    magnitudes      =   FOREACH (GROUP $features BY id) GENERATE
                            group AS id,
                            ml_jython_utils.sum_squares($features.weight)
                            AS magnitude;

    dot_prod_terms  =   FOREACH (JOIN $features BY feature, copy BY feature) GENERATE
                            $features::id AS row,
                            copy::id AS col,
                            $features::weight * copy::weight AS val;

    dot_products    =   FOREACH (GROUP dot_prod_terms BY (row, col)) GENERATE
                            FLATTEN(group) AS (row, col),
                            SUM(dot_prod_terms.val) AS val;

    dot_prods_filt  =   FILTER dot_products BY row != col;

    mat_tmp         =   FOREACH (JOIN magnitudes BY id, dot_prods_filt BY col) GENERATE
                            row AS row, col AS col,
                            val / SQRT(magnitude) AS val;

    $cosine_mat     =   FOREACH (JOIN magnitudes BY id, mat_tmp BY row) GENERATE
                            row AS row, col AS col,
                            val / SQRT(magnitude) AS val;

};
