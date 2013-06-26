graph           =   LOAD '/Users/jpacker/data/github/repo_similarity_matrix_sample.txt' USING PigStorage()
                    AS (row: int, col: int, val: double);

repo_ids        =   LOAD '/Users/jpacker/data/github/repo_ids.txt' USING PigStorage()
                    AS (id: int, name: chararray);

rows_dups       =   FOREACH graph GENERATE row AS id;
cols_dups       =   FOREACH graph GENERATE col AS id;
vertices_dups   =   UNION rows_dups, cols_dups;
vertices        =   DISTINCT vertices_dups;

sample_ids      =   FOREACH (JOIN vertices BY id, repo_ids BY id) GENERATE
                        repo_ids::id, repo_ids::name;

STORE sample_ids INTO '/Users/jpacker/data/github/repo_ids_sample' USING PigStorage();
