import logging
import logging.config

from logger_config import LOGGER_CONFIG
logging.config.dictConfig(LOGGER_CONFIG)

logger = logging.getLogger()