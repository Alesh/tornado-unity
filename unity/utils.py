""" Helpers and utilites
"""


def fqc_name(inst):
    """ Returns fully qualified class name
    """
    if not isinstance(inst, type):
        inst = inst.__class__
    return inst.__module__ + "." + inst.__name__
