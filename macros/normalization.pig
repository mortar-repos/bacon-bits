/*
 * Map a field into the range [0, 1],
 * with the option of also applying a transform (SQRT or LOG) beforehand
 *
 * samples: {anything}
 * field: chararray, name of the field to normalize
 * projection: comma-delimited list of the other fields in the relation
 * -->
 * normalized: {[schema of 'projection'], field}
 *
 * Example Usage:
 * tf_idfs: { id: int, term: chararray, tf_idf: double }
 * normalized = Stats__LogarithmicNormalization(tf_idfs, 'tf_idf', 'id, term');
 */

DEFINE Normalization__LinearTransform(samples, field, projection)
RETURNS normalized {
    stats           =   FOREACH (GROUP $samples ALL) GENERATE
                            MIN($samples.$field) AS min,
                            MAX($samples.$field) AS max;
    $normalized     =   FOREACH $samples GENERATE
                            $projection,
                            ($field - stats.min) / (stats.max - stats.min) AS $field; 
};

DEFINE Normalization__QuadraticTransform(samples, field, projection)
RETURNS normalized {
    sqrt_transform  =   FOREACH $samples GENERATE $projection, SQRT($field) AS $field;
    $normalized     =   Normalization__LinearTransform(sqrt_transform, '$field', '$projection');  
};

DEFINE Normalization__LogarithmicTransform(samples, field, projection)
RETURNS normalized {
    log_transform   =   FOREACH $samples GENERATE $projection, LOG($field) AS $field;
    $normalized     =   Normalization__LinearTransform(log_transform, '$field', '$projection');  
};

DEFINE Normalization__LogisticTransform(samples, field, projection)
RETURNS normalized {
    -- TODO: automatic logistic transform, with Nth percentile --> 0.9 or some threshold value    
};

-- TODO: normalize feature-vector