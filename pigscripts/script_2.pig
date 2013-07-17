REGISTER '../udfs/jython/utils.py' USING jython AS udfs;

data    =   LOAD '$INPUT_PATH' USING PigStorage() AS (f1: int, f2: int);
data    =   FOREACH data GENERATE (double) f1, (double) f2;
func    =   FOREACH data GENERATE (f1 * f2) / (f1 + f2);
-- func    =   FOREACH data GENERATE udfs.failwhale(f1, f2);

rmf $SHOUTPUT_PATH;
STORE func INTO '$SHOUTPUT_PATH' USING PigStorage();
