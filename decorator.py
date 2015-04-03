from functools import wraps
import time


def debug(func):
    msg = "method {0}".format(func.__qualname__)

    @wraps(func)
    def wrapper(*args, **kwargs):
        ts = time.time()
        res = func(*args, **kwargs)
        te = time.time()
        print(msg, ": %2.2f sec" % (te-ts))
        return res
    #def wrapper(*args, **kwargs):
    #    print(msg)
    #    #print(msg + " *args = {0}; **kwargs = {1}".format(args, kwargs))
    #    return func(*args, **kwargs)
    return wrapper


def debugclass(cls):
    for name, val in vars(cls).items():
        if callable(val):
            setattr(cls, name, debug(val))
    return cls

