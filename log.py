import logging

def setup_custom_logger(fileName):
    formatter = logging.Formatter(fmt='%(message)s')

    filename = "logs/" + fileName + ".log"
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    logger = logging.getLogger('root')
    logger.setLevel(logging.DEBUG)

    logger.addHandler(fh)
    
    return logger