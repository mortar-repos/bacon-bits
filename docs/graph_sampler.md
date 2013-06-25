# Graph Sampler

`controlscripts/graph_sampler.py` is a tool for extracting, small, contiguous portions of a graph to use with local development. Simply taking a random sample of a graph would be useless since the edges would likely be disconnected. Instead, you can use the graph sampler, which takes the graph and a list of "seed" vertices, and returns a sample consisting of all edges either going to or coming from vertices in the N-step-neighborhood of the seed vertices.

## Graph Representation

The input graph should be a list of edges in the format: from_vertex_id TAB to_vertex_id TAB weight, where the vertex ids are integers and the weights are doubles.

The seed vertex list should be a list of integer vertex ids.

If your graph uses string ids instead of integers, see `docs/integer_ids.md` for instructions on how to use Bacon Bits to assign integer ids to your vertices, and how to return from the integer ids to your string ids when you are finished.

## Using The Graph Sampler

First, make a parameter file with the following parameters:

    GRAPH=/a/path         # graph file (can be a glob)
    SEED_VERTICES=/a/path # seed vertex list file
    TMP_DIR=/a/path       # tmp dir for intermediate output. will be deleted upon completion.
    OUTPUT_PATH=/a/path   # dir to store output into
    NEIGHBORHOOD_SIZE=3   # how many steps to go out from the seed vertices

To run on your local machine:

    mortar baconbits:local_run graph_sampler -f /path/to/param/file

To run on a cluster:

    mortar baconbits:run graph_sampler -f /path/to/param/file -s CLUSTER_SIZE
