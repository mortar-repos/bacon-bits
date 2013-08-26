from math import exp

@outputSchema("scaled: double")
def logistic_scale(val, logistic_param):
    return -1.0 + 2.0 / (1.0 + exp(-logistic_param * val))

@outputSchema("path_info: (raw_direct_weight: int, raw_indirect_weight: int)")
def path_info(input_):
    if input_ is None:
        return None

    raw_direct_weight = 0
    raw_indirect_weight = 0

    for t in input_:
        if t[1] == 1:
            raw_direct_weight += t[0]
        else:
            raw_indirect_weight += t[0]

    return (raw_direct_weight, raw_indirect_weight)
