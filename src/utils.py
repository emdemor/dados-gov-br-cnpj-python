import numpy


def intclip(value, min, max):
    return int(numpy.clip(value, min, max))