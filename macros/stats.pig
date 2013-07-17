-- SANDBOX FOR STATISTICS MACROS
-- NOT READY FOR PUBLIC CONSUMPTION

REGISTER 'datafu-0.0.10.jar';

DEFINE VAR datafu.pig.stats.VAR();
DEFINE QUANTILES datafu.pig.stats.StreamingQuantile('$STATS__NUM_QUANTILES');

/*
 * Gives basic statistics about the distribution of a numeric field:
 * mean, standard deviation, and quantiles. The number of quantiles
 * is specified by the global pig parameter STATS__NUM_QUANTILES.
 * For example, setting this to 3 gives (min, median, max);
 * setting this to 5 gives (min, 25%, 50% (median), 75%, max), etc.
 *
 * samples: any relation
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
