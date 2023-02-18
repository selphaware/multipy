import logging
from functools import wraps


def logit(fun):
    """

    :param fun:
    :return:
    """

    @wraps(fun)
    def inner(*args, **kwargs):
        """

        :param args:
        :param kwargs:
        :return:
        """
        if hasattr(fun, "__name__"):
            fname = fun.__name__
        elif hasattr(fun.func, "__name__"):
            fname = fun.func.__name__
        else:
            fname = "<NA>"

        logging.info(f"START -> {fname}")
        ret = fun(*args, **kwargs)
        logging.info(f"END -> {fname}")
        return ret

    return inner
