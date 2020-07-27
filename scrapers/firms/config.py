import os
from collections import namedtuple

_curr_dir = os.path.dirname(os.path.realpath(__file__))
_data_urls = {'MODIS': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/shapes/zips/MODIS_C6_USA_contiguous_and_Hawaii_24h.zip',
              'SUOMI_VIIRS': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_USA_contiguous_and_Hawaii_24h.zip',
              'JI_VIIRS': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/shapes/zips/J1_VIIRS_C2_USA_contiguous_and_Hawaii_24h.zip'}

_constants = {
    'params': os.path.join(_curr_dir, 'params.yaml'),
    'creds': os.path.join(_curr_dir, 'credentials.yaml'),
    'alerts_input': os.path.join(_curr_dir, 'alerts_input'),
    'alerts_output': os.path.join(_curr_dir, 'alerts_output'),
    'activefires_input': os.path.join(_curr_dir, 'activefires_input'),
    'activefires_output': os.path.join(_curr_dir, 'activefires_output')
    }

constants = (namedtuple('Constants', _constants)(**_constants))
data_urls = (namedtuple('Data_Urls', _data_urls)(**_data_urls))

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
