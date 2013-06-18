REGISTER 's3n://mhc-software-mirror/datafu/datafu-0.0.9-SNAPSHOT.jar';

DEFINE VAR datafu.pig.stats.VAR();
DEFINE QUANTILES datafu.pig.stats.StreamingQuantile('11');

/*
 * samples: {anything}
 * field: chararray, name of the field to characterize the distribution of
 * -->
 * dist_stats: {mean, std_dev, quantiles}
 */
DEFINE Stats__CharacterizeDistribution(samples, field)
RETURNS dist_stats {
    proj            =   FOREACH $samples GENERATE $field;
    $dist_stats     =   FOREACH (GROUP proj ALL) {
                            sorted = ORDER proj BY $field ASC;
                            GENERATE AVG(proj) AS mean,
                                     SQRT(VAR(proj)) AS std_dev,
                                     QUANTILES(sorted) AS quantiles;
                        }
};

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

DEFINE Stats__LinearNormalization(samples, field, projection)
RETURNS normalized {
    stats           =   FOREACH (GROUP $samples ALL) GENERATE
                            MIN($samples.$field) AS min,
                            MAX($samples.$field) AS max;
    $normalized     =   FOREACH $samples GENERATE
                            $projection,
                            ($field - stats.min) / (stats.max - stats.min) AS $field; 
};

DEFINE Stats__QuadraticNormalization(samples, field, projection)
RETURNS normalized {
    sqrt_transform  =   FOREACH $samples GENERATE $projection, SQRT($field) AS $field;
    $normalized     =   Stats__LinearNormalization(sqrt_transform, '$field', '$projection');  
};

DEFINE Stats__LogarithmicNormalization(samples, field, projection)
RETURNS normalized {
    log_transform   =   FOREACH $samples GENERATE $projection, LOG($field) AS $field;
    $normalized     =   Stats__LinearNormalization(log_transform, '$field', '$projection');  
};
