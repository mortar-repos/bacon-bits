from org.apache.pig.scripting import Pig

class IterationUtils:
    @staticmethod
    def iterate_until_convergence(script_path,
                                  iteration_dir,
                                  param_generator_func,
                                  metric_name,
                                  metric_type,
                                  metric_alias,
                                  metric_threshold,
                                  max_num_iterations):

    """
    Utility for running a pigscript which outputs data in the same schema as its input iteratively,
    with the output of the previous run being the input of the next run. Stops when some convergence
    metric has been reached or if a maximum number of iterations has been reached.

    Example usage:

    iteration_result = IterationUtils.iterate_until_convergence(
            "../pigscripts/pagerank_iterate.pig", # the pigscript to iterate
            iteration_dir,                        # temporary iteration outputs will be stored here
            iteration_param_func,                 # takes iteration #, returns Pig parameter dictionary
            "Sum of ordering-rank changes",       # name of the convergence metric
            int,                                  # Python type of the convergence metric
            "aggregate_rank_change",              # alias in the pigscript where the metric is stored to
            convergence_threshold,                # stop when metric less than this
            max_num_iterations                    # or if this many iterations have been performed

    iteration_result is a PigStats object for the results of the last iteration.

    Example iteration_param_func:

    def iteration_param_func(it_num, it_dir):
        if it_num == 1:
            iteration_input = preprocess_dir + "/pageranks"
        else:
            iteration_input = it_dir + "/" + str(it_num-1) + "/pageranks"

        return {
            "INPUT_PATH"                  : iteration_input,
            "DAMPING_FACTOR"              : damping_factor,
            "NUM_NODES"                   : num_nodes,
            "PAGERANKS_OUTPUT_PATH"       : it_dir + "/" + str(it_num) + "/pageranks"
            "AGG_RANK_CHANGE_OUTPUT_PATH" : it_dir + "/" + str(it_num) + "/rank_changes"
        }
    )
    """

        script = Pig.compileFromFile(script_path)
        for i in range(1, max_num_iterations+1):
            print "Starting iteration step: %d" % i

            iteration    = script.bind(param_generator_func(i, iteration_dir)).runSingle()
            metric_value = metric_type(str(iteration.result(metric_alias).iterator().next().get(0)))

            if metric_value < metric_threshold:
                print "%s %s under convergence threshold %s. Stopping." \
                       % (metric_name, str(metric_value), str(metric_threshold))
                return { "num_iterations": i, "stop_reason": "CONVERGED" }
            elif i == max_num_iterations:
                print "%s %s above convergence threshold %s but hit max number of iterations. Stopping" \
                       % (metric_name, str(metric_value), str(metric_threshold))
                return { "num_iterations": i, "stop_reason": "MAX_ITERATIONS" }
            else:
                print "%s %s above convergence threshold %s. Continuing." \
                       % (metric_name, str(metric_value), str(metric_threshold))
