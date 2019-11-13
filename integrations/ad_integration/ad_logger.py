import logging

LOG_LEVEL = logging.DEBUG


def start_logging(log_file, detail_logging=None):
    if detail_logging is None:
        detail_logging = ('MoAdSync', 'AdReader', 'AdWriter', 'mora-helper', 'AdSyncRead')

    for name in logging.root.manager.loggerDict:
        if name in detail_logging:
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format='%(levelname)s %(asctime)s %(name)s %(message)s',
        level=LOG_LEVEL,
        filename=log_file
    )
