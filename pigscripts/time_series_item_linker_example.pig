REGISTER 'bacon-bits-0.1.0.jar';
DEFINE TimeSeriesItemLinker com.mortardata.pig.recsys.TimeSeriesItemLinker('3');

data            =   LOAD '../example_input/time_series_item_linker_test.txt' USING PigStorage()
                    AS (user: int, item: int, score: float, timestamp: chararray);

ii_links        =   FOREACH (GROUP data BY user) {
                        chronological = ORDER data BY timestamp ASC;
                        GENERATE FLATTEN(TimeSeriesItemLinker(chronological.(item, score, timestamp)));
                    }

DUMP ii_links;
