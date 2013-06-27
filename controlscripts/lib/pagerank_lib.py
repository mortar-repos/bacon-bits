from org.apache.pig.scripting import Pig
from iteration_utils          import IterationUtils

class Pagerank:
    @staticmethod
    def run_pagerank(edges_input,
                     output_path,
                     tmp_output_dir,
                     damping_factor=0.85,
                     convergence_threshold=0.0001,
                     max_num_iterations=10,
                     id_name_map=None,
                     preprocessing_script="../pigscripts/pagerank_preprocess.pig",
                     iteration_script="../pigscripts/pagerank_iterate.pig"
                    ):

        """
        Calculates pageranks for directed graph of nodes and edges.

        Three main steps:
            1. Preprocessing: Process input data to:
                 a) Count the total number of nodes.
                 b) Prepare initial pagerank values for all nodes.
            2. Iteration: Calculate new pageranks for each node based on the previous pageranks of the
                          nodes with edges going into the given node.
            3. Postprocessing: Order nodes by pagerank
                               Optionally join (id, pagerank) pairs to a dataset of (id, name) pairs
                               to get human-readable names
        """

        preprocess_dir = "%s/preprocess" % tmp_output_dir
        iteration_dir  = "%s/iteration"  % tmp_output_dir

        # Preprocessing step:
        print "Starting preprocessing step."
        preprocess = Pig.compileFromFile("../pigscripts/pagerank_preprocess.pig").bind({
            "INPUT_PATH"            : edges_input,
            "PAGERANKS_OUTPUT_PATH" : "%s/pageranks" % preprocess_dir,
            "NUM_NODES_OUTPUT_PATH" : "%s/num_nodes" % preprocess_dir
        }).runSingle()

        # Update convergence threshold based on the size of the graph (number of nodes)
        num_nodes             = long(str(preprocess.result("num_nodes").iterator().next().get(0)))
        convergence_threshold = long(convergence_threshold * num_nodes * num_nodes)
        print "Calculated convergence threshold for %d nodes: %d" % (num_nodes, convergence_threshold) 

        # Iteration step:
        def iteration_param_func(it_num, it_dir):
            if it_num == 1:
                iteration_input = "%s/pageranks" % preprocess_dir
            else:
                iteration_input = "%s/%d/pageranks" % (it_dir, it_num - 1)

            return {
                "INPUT_PATH"                  : iteration_input,
                "DAMPING_FACTOR"              : damping_factor,
                "NUM_NODES"                   : num_nodes,
                "PAGERANKS_OUTPUT_PATH"       : "%s/%d/pageranks"    % (it_dir, it_num),
                "AGG_RANK_CHANGE_OUTPUT_PATH" : "%s/%d/rank_changes" % (it_dir, it_num)
            }

        iteration_result = IterationUtils.iterate_until_convergence(
            "../pigscripts/pagerank_iterate.pig", # the pigscript to iterate
            iteration_dir,                        # temporary iteration outputs will be stored here
            iteration_param_func,                 # takes iteration #, returns Pig parameter dictionary
            "Sum of ordering-rank changes",       # name of the convergence metric
            int,                                  # Python type of the convergence metric
            "aggregate_rank_change",              # alias in the pigscript where the metric is stored to
            convergence_threshold,                # stop when metric less than this
            max_num_iterations                    # or if this many iterations have been performed
        )

        # Postprocesing step:
        print "Starting postprocessing step."

        postprocess_script = """
            pageranks   =   LOAD '$PAGERANKS_INPUT_PATH'   USING PigStorage() AS (id: int, pagerank: double);
            pageranks   =   FILTER pageranks BY pagerank IS NOT NULL;
        """

        if id_name_map:
            postprocess_script += """
                id_name_map =   LOAD '$ID_NAME_MAP_INPUT_PATH' USING PigStorage() AS (id: int, name: chararray);
                with_names  =   FOREACH (JOIN id_name_map BY id, pageranks BY id) GENERATE name, pagerank;
                ordered     =   ORDER with_names BY pagerank DESC;
                rmf $OUTPUT_PATH;
                STORE ordered INTO '$OUTPUT_PATH' USING PigStorage();
            """

            postprocess = Pig.compile(postprocess_script).bind({
                "PAGERANKS_INPUT_PATH"   : "%s/%d/pageranks" % (iteration_dir, iteration_result["num_iterations"]),
                "ID_NAME_MAP_INPUT_PATH" : id_name_map,
                "OUTPUT_PATH"            : output_path
            }).runSingle()
        else:
            postprocess_script += """
                ordered     =   ORDER pageranks BY pagerank DESC;
                rmf $OUTPUT_PATH;
                STORE ordered INTO '$OUTPUT_PATH' USING PigStorage();
            """

            postprocess = Pig.compile(postprocess_script).bind({
                "PAGERANKS_INPUT_PATH"   : "%s/%d/pageranks" % (iteration_dir, iteration_result["num_iterations"]),
                "OUTPUT_PATH"            : output_path
            }).runSingle()

        Pig.fs("rmr %s" % preprocess_dir)
        Pig.fs("rmr %s" % iteration_dir)
