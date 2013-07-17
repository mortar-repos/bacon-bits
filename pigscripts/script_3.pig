data    =   LOAD '$INPUT_PATH' USING PigStorage() AS (val: double);
func    =   FOREACH data GENERATE EXP(SQRT(val));

rmf $OUTPUT_PATH;
STORE func INTO '$OUTPUT_PATH' USING PigStorage();
