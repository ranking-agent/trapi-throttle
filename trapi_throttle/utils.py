import asyncio

def all_equal(values: list):
    """ Check that all values in given list are equal """
    return all(values[0] == v for v in values)

def get_equal_dict_values(dct: dict):
    """ Return values that are equal to the first list element """
    first_value = next(iter(dct.values()))
    return {
        k:v for k,v in dct.items() if
        v == first_value
    }


async def gather_dict(dct):
    """ Gather a dict of coroutines """
    values = await asyncio.gather(*dct.values())
    return {
        k:v for k,v in
        zip(dct.keys(), values)
    }
