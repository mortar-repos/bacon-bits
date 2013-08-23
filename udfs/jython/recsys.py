from math import exp

@outputSchema("scaled: double")
def logistic_scale(val, logistic_param):
    return -1.0 + 2.0 / (1.0 + exp(-logistic_param * val))
