import asyncio


def all_equal(values: list):
    """ Check that all values in given list are equal """
    return all(values[0] == v for v in values)


def get_keys_with_value(dct: dict, value):
    """ Return keys where the value matches the given """
    return [
        k for k, v in dct.items() if
        v == value
    ]


async def gather_dict(dct):
    """ Gather a dict of coroutines """
    values = await asyncio.gather(*dct.values())
    return {
        k: v for k, v in
        zip(dct.keys(), values)
    }
