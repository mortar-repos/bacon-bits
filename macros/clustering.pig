IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

/*
 * See http://www.cs.ucsb.edu/~xyan/classes/CS595D-2009winter/MCL_Presentation2.pdf
 * {row, col, val}, double, double ==> {row, col, val}
 */
DEFINE MCLIterate(in_mat, inflation_parameter, epsilon)
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
DEFINE GetClustersFromMCLResult(mcl_result)
returns clusters {
    by_row                  =   GROUP $mcl_result BY row;
    clusters_with_dups      =   FOREACH by_row GENERATE $1.$1 AS cluster;
    clusters_dups_ordered   =   FOREACH clusters_with_dups {
                                    ordered = ORDER cluster BY $0 ASC;
                                    GENERATE ordered AS cluster;
                                }
    $clusters               =   DISTINCT clusters_dups_ordered;
};
