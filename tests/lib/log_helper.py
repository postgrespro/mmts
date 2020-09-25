import logging
import logging.config
import sys
import time

# FWIW I've attempted to keep the cfg in json/yaml file but sank in 'could not
# resolve UTCFormatter class' issue

# timestamp in UTC+-00:00 aka GMT
class UTCFormatter(logging.Formatter):
    converter = time.gmtime

LOGGING = {
    "version": 1,
    "formatters": {
	"defaultFormatter": {
	    "()": UTCFormatter,
	    "format": "%(asctime)s.%(msecs)-3d %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
	    "datefmt": "%Y-%m-%d:%H:%M:%S"
	}
    },
    "handlers": {
	"console": {
	    "class": "logging.StreamHandler",
	    "formatter": "defaultFormatter",
	    "level": "DEBUG",
	    "stream": "ext://sys.stderr"
	}
    },
    "loggers": {
	"root": {
	    "level": "DEBUG",
	    "handlers": ["console"]
	},
	"root.test_helper": {
	    "level": "INFO"
	},
	"root.bank_client": {
	    "level": "INFO"
	}
    }
}

logging.config.dictConfig(LOGGING)
