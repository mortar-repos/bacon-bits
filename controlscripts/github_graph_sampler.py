from org.apache.pig.scripting import Pig
from graph_sampler_lib import GraphSampler

graph             = "/home/jpacker/data/github/item_distance_matrix.txt"
seed_vertices     = "/home/jpacker/data/github/item_distance_matrix_sample_seed_vertices.txt"
output_path       = "/home/jpacker/data/github/item_distance_matrix_sample"
tmp_out_dir       = "/home/jpacker/data/github/graph_sampler_tmp"

neighborhood_size = 4

if __name__ == "__main__":
    graph_sampler = GraphSampler(graph=graph,
                                 seed_vertices=seed_vertices,
                                 output_path=output_path,
                                 neighborhood_size=neighborhood_size,
                                 tmp_output_dir=tmp_out_dir)
    graph_sampler.sample_graph()
