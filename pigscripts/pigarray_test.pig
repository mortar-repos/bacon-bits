REGISTER 'bacon-bits-0.1.0.jar';

DEFINE ToPigArray        com.mortardata.pig.array.ToPigArray();
DEFINE FromDensePigArray com.mortardata.pig.array.FromDensePigArray();
DEFINE Distance          com.mortardata.pig.geometry.Distance();

data    =   LOAD '../example_input/100_random_points.txt' USING PigStorage()
            AS (x: double, y: double);

tmp     =   LIMIT data 5;

tmp     =   FOREACH tmp GENERATE ToPigArray(x, y) AS coords;
copy    =   FOREACH tmp GENERATE *;
tmp     =   FOREACH (CROSS tmp, copy) GENERATE
                tmp::coords  AS point_a,
                copy::coords AS point_b,
                Distance(tmp::coords, copy::coords);
tmp     =   FOREACH tmp GENERATE
                FromDensePigArray(point_a),
                FromDensePigArray(point_b),
                distance;

DUMP tmp;
