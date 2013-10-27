import os
import logging


levels = {'debug': logging.DEBUG, \
          'info': logging.INFO, \
          'warning': logging.WARNING, \
          'error': logging.ERROR, \
          'critical': logging.CRITICAL}


def get_logger():
    """Get a logger instance. The name of the logger
        is automatically determined using the current
        processes ID.

        Inputs:
            None

        Output:
            logger: A logging.Logger instance.
    """
    name = str(os.getpid())
    logger = logging.getLogger(name)
    return logger


def setup_logger(logfn):
    """Set up a logging.Logger instance for the current thread.
        Install a FileHandler using the file name provided.

        Input:
            logfn: File to receive log entries.

        Outputs:
            None
    """
    logger = get_logger()
    # Remove any existing handlers
    disconnect_logger() 
    # What gets logged is determined by if-clauses that 
    # check the current verbosity level and debugging state, 
    # not logging's level, so let everything be logged. 
    logger.setlevel(logging.DEBUG) 
    logfile = logging.FileHandler(filename=logfn)
    formatter = logging.Formatter(datefmt="%Y-%m-%d %H:%M:%S", \
                fmt="%(levelname)s - %(asctime)s\n%(message)s\n")
    logfile.SetFormatter(formatter)
    logger.addHandler(logfile)


def disconnect_logger():
    """Disconnect logger from any handlers.

        Inputs:
            None

        Outputs:
            None
    """
    logger = get_logger()
    for handler in logger.handlers:
        logger.removeHandler(handler)


def log(msg, level):
    logger = get_logger()
    logger.log(level, msg)
