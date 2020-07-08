import os
from collections import namedtuple

_curr_dir = os.path.dirname(os.path.realpath(__file__))
_constants = {
    'params': os.path.join(_curr_dir, 'params.yaml'),
    'creds': os.path.join(_curr_dir, 'credentials.yaml'),
    'alerts_input': os.path.join(_curr_dir, 'alerts_input'),
    'alerts_output': os.path.join(_curr_dir, 'alerts_output'),
    'activefires_input': os.path.join(_curr_dir, 'activefires_input'),
    'activefires_output': os.path.join(_curr_dir, 'activefires_output')
}

constants = (namedtuple('Constants', _constants)(**_constants))

logger_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s-%(name)s-%(levelname)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'filename': 'logs.log',
            'maxBytes': 20971520, 
            'backupCount': 9,
            'encoding': 'utf8'
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console', 'file']
    }
}
