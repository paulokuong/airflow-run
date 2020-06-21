import logging


def logger_factory(module_name=__name__, handler=None, level=logging.DEBUG):
    """Produce logger object.

    Args:
        handler (logging Handler): logging handler object.
        level (str): level of logging.
        module_name (str): name of the module.
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(level)
    if not handler:
        handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(filename)s %(funcName)s.%(lineno)d: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
