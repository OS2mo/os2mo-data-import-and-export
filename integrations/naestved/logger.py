import logging


def start_logging(loglevel=10, logfile="debug.log"):
    # Set logger name
    logger = logging.getLogger()

    # Set logger handler
    handler = logging.FileHandler(logfile)
    logger.addHandler(handler)

    # Set logger formatter
    default = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    formatter = logging.Formatter(
        "%(message)s"
    )

    handler.setFormatter(formatter)

    # Set loglevel
    logger.setLevel(loglevel)
    handler.setLevel(loglevel)

    # Return logger
    return logger