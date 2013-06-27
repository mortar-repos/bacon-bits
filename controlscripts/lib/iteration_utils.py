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
