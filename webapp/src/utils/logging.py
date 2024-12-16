import logging
import logging.config


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
        "file": {
            "level": "DEBUG",
            "mode": "w",
            "formatter": "standard",
            "filename": "logs.log",
            "class": "logging.handlers.RotatingFileHandler",
        },
    },
    "loggers": {
        "": {"handlers": ["default", "file"], "level": "DEBUG", "propagate": False},
        "__main__": {
            "handlers": ["default", "file"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
