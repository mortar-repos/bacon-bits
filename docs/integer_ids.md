# Going To And From Integer Ids

All of the matrix and graph algorithms in Bacon Bits require items in your data to be identified with integer ids. If your data has string ids, you will need to convert to integer ids before using some Bacon Bits components, and then convert back to string ids when you are finished. Bacon Bits has utilities for doing both operations.

These following examples assume you have installed Bacon Bits into your project, so you have all of the components in a "vendor" directory.

## Going From Names To Integer Ids

First, process your data into the standard matrix/graph schema `{row: chararray, col: chararray, val: double}`. Then, use the macro `Matrix__NamesToIds`:

    IMPORT '../vendor/macros/matrix.pig';
    -- load and process your data into {row, col, val}
    -- then:
    matrix_with_ids, id_to_name_map = Matrix__NamesToIds($my_data);

## Returning From Integer Ids To Names

    IMPORT '../vendor/macros/matrix.pig';
    -- my_matrix has schema: {row: int, col: int, val: double}
    -- id_to_name_map has schema: {id: int, name: chararray}
    matrix_with_names = Matrix__IdsToNames(my_matrix, id_to_name_map);
