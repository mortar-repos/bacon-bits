from math                     import ceil, log
from org.apache.pig.scripting import Pig

if __name__ == "__main__":
    params        = Pig.getParameters()
    graph         = params["GRAPH"]
    seed_vertices = params["SEED_VERTICES"]
    tmp_dir       = params["TMP_DIR"]
    output_path   = params["OUTPUT_PATH"]
    nhood_size    = int(params["NEIGHBORHOOD_SIZE"])

    preprocess_graph        = "%s/preprocess/graph"        % tmp_dir
    preprocess_num_vertices = "%s/preprocess/num_vertices" % tmp_dir
    iteration_verts_prefix  = "%s/iteration/vertices_"     % tmp_dir

    print "Graph Sampler: starting preprocessing step."
    preprocessing = Pig.compileFromFile("../pigscripts/graph_sampler_preprocess.pig").bind({
        "GRAPH_INPUT_PATH"         : graph,
        "GRAPH_OUTPUT_PATH"        : preprocess_graph,
        "NUM_VERTICES_OUTPUT_PATH" : preprocess_num_vertices
    }).runSingle()

    iteration_script = Pig.compileFromFile("../pigscripts/graph_sampler_iterate.pig")
    num_iterations   = nhood_size - 1
    num_vertices     = long(str(preprocessing.result("num_vertices").iterator().next().get(0)))

    print "Graph Sampler: scheduling %d iterations" % num_iterations
    for i in range(num_iterations):
        print "Graph Sampler: starting iteration step %d" % (i+1)
        iteration = iteration_script.bind({
            "VERTICES_INPUT_PATH"  : seed_vertices if i == 0 else (iteration_verts_prefix + str(i-1)),
            "GRAPH_INPUT_PATH"     : preprocess_graph,
            "VERTICES_OUTPUT_PATH" : iteration_verts_prefix + str(i)
        }).runSingle()
    iteration_result = iteration_verts_prefix + str(i)

    print "Graph Sampler: starting postprocessing step."
    postprocessing = Pig.compileFromFile("../pigscripts/graph_sampler_postprocess.pig").bind({
        "GRAPH_INPUT_PATH"     : graph,
        "VERTICES_INPUT_PATH"  : iteration_result,
        "SAMPLE_OUTPUT_PATH"   : output_path,
    }).runSingle()

    print "Graph Sampler: deleting temporary output directory"
    Pig.fs("rmr " + tmp_dir)
