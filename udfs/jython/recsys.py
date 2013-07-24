# input:  {val: float, reason: chararray}
# output: (reason_1: chararray, reason_2: chararray)
@outputSchema("reasons: (reason_1: chararray, reason_2: chararray)")
def top_two_reasons(input_):
    if input_ is None:
        return None

    input_len = len(input_)
    if input_len == 1:
        return (input_[0][1], None)
    elif input_len == 2:
        if input_[0][0] > input_[1][0]:
            return (input_[0][1], input_[1][1])
        else:
            return (input_[1][1], input_[0][1])
    elif input_len == 0:
        return None
    else:
        input_.sort(key=lambda t: t[0], reverse=True)
        return (input_[0][1], input_[1][1])

# input: {val: float, reason_1: chararray, reason_2: chararray}
# output: (val: float, reason_1: chararray, reason_2: chararray)
@outputSchema("shortest_path: (val: double, reason_1: chararray, reason_2: chararray)")
def shortest_path(input_):
    if input_ is None:
        return None

    if len(input_) > 0:
        return min(input_, key=lambda t: t[0])
    else:
        return None
