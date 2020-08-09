import logging
import logging.config
from pathlib import Path

from logger_config import LOGGER_CONFIG

def set_logger(filename, logger_conf=LOGGER_CONFIG):
    logger_conf["handlers"]["file"]["filename"] = str(Path("logs") / f"{filename}.log")
    logging.config.dictConfig(logger_conf)
    return logging.getLogger(str(filename))

def default_logger():
    logger_path = Path("logs") / Path(__file__).stem \
        .upper()
    return set_logger(logger_path)

class NoLog():
    def info(self, msg):
        print(f"INFO: {msg}")
    def warning(self, msg):
        print(f"WARNING: {msg}")
    def error(self, msg):
        print(f"ERROR: {msg}")