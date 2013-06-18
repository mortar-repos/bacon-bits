from math import ceil, log
from org.apache.pig.scripting import Pig

class GraphSampler:
    def __init__(self, graph,
                       seed_vertices,
                       output_path,
                       neighborhood_size=4,
                       tmp_output_dir="hdfs:///mortar-graph-sampler",
                       preprocessing_script="../pigscripts/graph_sampler_preprocess.pig",
                       iteration_script="../pigscripts/graph_sampler_iterate.pig",
                       postprocessing_script="../pigscripts/graph_sampler_postprocess.pig"):

        self.graph = graph
        self.seed_vertices = seed_vertices
        self.output_path = output_path

        self.neighborhood_size = neighborhood_size
        
        self.preprocessing_script = preprocessing_script
        self.iteration_script = iteration_script
        self.postprocessing_script = postprocessing_script

        self.tmp_output_dir = tmp_output_dir
        self.preprocess_graph = tmp_output_dir + "/preprocess/graph"
        self.preprocess_num_vertices = tmp_output_dir + "/preprocess/num_vertices"
        self.iteration_verts_prefix = tmp_output_dir + "/iteration/vertices"

    def sample_graph(self):
        print "Graph Sampler: starting preprocessing step."
        preprocess = Pig.compileFromFile(self.preprocessing_script)
        preprocess_stats = preprocess.bind({
            "GRAPH_INPUT_PATH": self.graph,
            "GRAPH_OUTPUT_PATH": self.preprocess_graph,
            "NUM_VERTICES_OUTPUT_PATH": self.preprocess_num_vertices
        }).runSingle()

        iteration = Pig.compileFromFile(self.iteration_script)
        num_iterations = self.neighborhood_size - 1
        graph_num_vertices = long(str(preprocess_stats.result("num_vertices").iterator().next().get(0)))

        print "Graph Sampler: scheduling %d iterations" % num_iterations

        for i in range(num_iterations):
            print "Graph Sampler: starting iteration step %d" % (i+1)

            iteration_input = self.seed_vertices if i == 0 else (self.iteration_verts_prefix + str(i-1))
            iteration_output = self.iteration_verts_prefix + str(i)

            iteration_stats = iteration.bind({
                "VERTICES_INPUT_PATH": iteration_input,
                "GRAPH_INPUT_PATH": self.preprocess_graph,
                "VERTICES_OUTPUT_PATH": iteration_output
            }).runSingle()

        iteration_result = self.iteration_verts_prefix + str(i)

        print "Graph Sampler: starting postprocessing step."
        postprocess = Pig.compileFromFile(self.postprocessing_script)
        postprocess_stats = postprocess.bind({
            "GRAPH_INPUT_PATH": self.graph,
            "VERTICES_INPUT_PATH": iteration_result,
            "SAMPLE_OUTPUT_PATH": self.output_path,
        }).runSingle()

        Pig.fs("rmr " + self.tmp_output_dir)
