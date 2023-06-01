import logging

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s')
datasaurus_logger = logging.getLogger('datasaurus')
datasaurus_logger.setLevel(logging.DEBUG)
