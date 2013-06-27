from org.apache.pig.scripting import Pig
from pagerank_lib             import Pagerank

if __name__ == "__main__":
    params = Pig.getParameters()

    try:
        input_path     = params["INPUT_PATH"]
        output_path    = params["OUTPUT_PATH"]
        tmp_output_dir = params["TMP_OUTPUT_DIR"]
    except:
        print "Usage: mortar baconbits:[local_]run pagerank " + \
              "-p INPUT_PATH=<...> -p OUTPUT_PATH=<...> -p TMP_OUTPUT_DIR=<...> "

    damping_factor = 0.85
    if "DAMPING_FACTOR" in params:
        damping_factor = float(params["DAMPING_FACTOR"])

    convergence_threshold = 0.001
    if "CONVERGENCE_THRESHOLD" in params:
        convergence_threshold = float(params["CONVERGENCE_THRESHOLD"])

    max_num_iterations = 10
    if "MAX_NUM_ITERATIONS" in params:
        max_num_iterations = int(params["MAX_NUM_ITERATIONS"])

    id_name_map = None
    if "ID_NAME_MAP" in params:
        id_name_map = params["ID_NAME_MAP"]

    Pagerank.run_pagerank(input_path,
                          output_path,
                          tmp_output_dir,
                          damping_factor=damping_factor,
                          convergence_threshold=convergence_threshold,
                          max_num_iterations=max_num_iterations,
                          id_name_map=id_name_map)
