import logging
import os
import os.path as op

def rm_rf(d):
    logging.debug("Removing directory %s" % d)
    for path in (op.join(d, f) for f in os.listdir(d)):
        if op.isdir(path):
            rm_rf(path)
        else:
            os.unlink(path)
    os.rmdir(d)