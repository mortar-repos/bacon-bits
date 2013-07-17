data    =   LOAD '$INPUT_PATH' USING PigStorage() AS (i: int);
copy    =   FOREACH data GENERATE *;
crossed =   CROSS data, copy;

rmf $OUTPUT_PATH;
STORE crossed INTO '$OUTPUT_PATH' USING PigStorage();
