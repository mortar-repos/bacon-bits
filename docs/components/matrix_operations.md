# Sparse Matrix Operations

`macros/matrix.pig` contains macros for performing basic matrix operations on sparse matrices
with the schema: {row: int, col: int, val: double}. These operations are useful building blocks
for many graph and machine learning algorithms.

The following operations are available:
- Matrix__Sum(A, B) returns A + B
- Matrix__Product(A, B) returns A * B
- Matrix__Squared(A) returns A * A. You must use this instead of Matrix__Product when applicable.
- Matrix__MinPlusProduct(A, B) returns A ★ B ([wikipedia link](http://en.wikipedia.org/wiki/Min-plus_matrix_multiplication))
- Matrix__MinPlusSquared(A) returns A ★ A. You must use this instead of Matrix__MinPlusProduct when applicable.
- Matrix__ScalarProduct returns A .* k, where k is a scalar
- Matrix__ElementwiseProduct returns A .^ k, with each element being taken to the power k
- Matrix__Transpose(A) returns A with its rows and columns swapped
- Matrix__NormalizeRows(A) and Matrix__NormalizeCols(A) normalizes the rows or columns of A respectively so that the elements in the row or column sum to 1.
- Matrix__TrimRows(A, order_direction, max_elems_per_row) returns A with all but the top max_elems_per_row elements of each row filtered out. order_direction can be 'ASC' or 'DESC'.
- Matrix__NamesToIds and Matrix__IdsToNames are utilities for converting relations between objects with string ids into matrices and back. See `docs/integer_ids.md`.
