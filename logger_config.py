from pathlib import Path

LOGGER_CONFIG = {
    "version": 1,
    "loggers": {
        "": {  # root logger
            "handlers": ["console","file"],
            "level": "DEBUG",
            "propagate": False
        }
    },
    "formatters": {
        "console": {
            "class": "logging.Formatter",
            "format": "%(levelname)s - %(message)s"
        },
        "file": {
            "class": "logging.Formatter",
            "format": "%(asctime)s - %(levelname)s - %(message)s",
            "datefmt": "%y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "console",
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "file",
            "filename": Path("logs") / "default.log"
        }
    },
    "root": {
        "level": "DEBUG",
        "handlers": ["console", "file"]
    }
}