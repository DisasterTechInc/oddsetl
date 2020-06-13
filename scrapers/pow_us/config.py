import os
from collections import namedtuple

_curr_dir = os.path.dirname(os.path.realpath(__file__))
_constants = {
    'params': os.path.join(_curr_dir, 'params.yaml'),
    'creds': os.path.join(_curr_dir, 'credentials.yaml'),
    'data_dir': os.path.join(_curr_dir, 'data'),
    'output_dir': os.path.join(_curr_dir, 'output'),
}

constants = (namedtuple('Constants', _constants)(**_constants))

