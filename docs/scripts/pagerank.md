# Pagerank

`controlscripts/pagerank.py` is a tool for finding Pageranks for the vertices of any graph.

## Graph Representation

The input graph should be a list of edges in the format: from_vertex_id TAB to_vertex_id TAB weight, where the vertex ids are integers and the weights are doubles.

The seed vertex list should be a list of integer vertex ids.

If your graph uses string ids instead of integers, see `docs/integer_ids.md` for instructions on how to use Bacon Bits to assign integer ids to your vertices, and how to return from the integer ids to your string ids when you are finished.

## Using Pagerank

First, make a parameter file with the following parameters:

    # input data in the format described above
    INPUT_PATH=/a/path

    # output will be in the form: node_id TAB pagerank
    OUTPUT_PATH=/a/path

    # temporary output will be stored here
    # this directory will be deleted after processing
    TMP_OUTPUT_PATH=/a/path

    # parameter to the Pagerank algorithm, see wikipedia
    DAMPING_FACTOR=0.85

    # iteration will stop if a convergence metric falls below this threshold
    # there is no intuitive way to have a convergence metric that is applicable to all datasets,
    # so you will have to verify what is best for your data
    CONVERGENCE_THRESHOLD=0.001

    # will always stop after this many iterations
    MAX_NUM_ITERATIONS=10

    # path to a file with data of in the format: node_id TAB node_name
    # if uncommented, the script will join the Pagerank output to this dataset
    # to generate a list of human-readable (node, pagerank) pairs
    #ID_NAME_MAP=/a/path

To run on your local machine:

    mortar baconbits:local_run pagerank -f /path/to/param/file

To run on a cluster:

    mortar baconbits:run pagerank -f /path/to/param/file -s CLUSTER_SIZE
