import logging.config

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "filename": "/tmp/logs.log",
            "mode": "w",
            "maxBytes": 1_000_000_000,
            "backupCount": 20,
        },
    },
    "root": {"level": "DEBUG", "handlers": ["console", "file"]},
    "loggers": {
        "uvicorn": {"level": "WARNING", "handlers": [], "propagate": False},
        "uvicorn.error": {"level": "WARNING", "handlers": [], "propagate": False},
        "uvicorn.access": {"level": "WARNING", "handlers": [], "propagate": False},
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
