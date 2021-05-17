
def all_equal(values: list):
    """ Check that all values in given list are equal """
    return all(values[0] == v for v in values)
