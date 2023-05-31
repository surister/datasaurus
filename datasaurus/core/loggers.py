import logging

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s')
datasaurus_logger = logging.getLogger('Datasaurus_logger')
datasaurus_logger.setLevel(logging.DEBUG)

