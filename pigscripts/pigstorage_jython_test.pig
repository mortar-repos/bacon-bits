data        =   LOAD '../example_input/1234.txt' USING PigStorage() AS (i: int);
copy        =   FOREACH data GENERATE *;
crossed     =   CROSS data, copy;
grouped     =   GROUP crossed BY $0;
out         =   FOREACH grouped GENERATE
                    1                AS int_field,
                    3.14159f         AS float_field,
                    'aardvark'       AS string_field,
                    (1, 2.718, 'c')  AS tuple_field: (i: int, d: double, c: chararray),
                    $1               AS bag_field;

rmf ../example_output/tmp;
STORE out INTO '../example_output/pigstorage' USING PigStorage('\t', '-schema');
