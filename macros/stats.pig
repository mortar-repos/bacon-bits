REGISTER '../vendor/baconbits/udfs/java/datafu-0.0.10.jar';

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
