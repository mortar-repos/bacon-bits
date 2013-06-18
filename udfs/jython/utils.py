@outputSchema("sum_sq: double")
def sum_squares(input_):
    return sum([t[0] * t[0] for t in input_])
