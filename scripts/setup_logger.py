import os
import logging.config

import logging

RECORD_FORMAT = "[%(asctime)s][%(filename)s:%(lineno)d][%(process)d:%(threadName)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class BasicFormatter(logging.Formatter):
    def __init__(self):
        logging.Formatter.__init__(self, datefmt=DATE_FORMAT)

    def format(self, record):
        fmt = RECORD_FORMAT
        self._fmt = "%(levelname)s: " + fmt
        return logging.Formatter.format(self, record)


def setup_logger(filename=None, debug=False):
    handlers = []
    LOGGING = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'basic': {
                '()': BasicFormatter,
            }
        },
        'handlers': {
            'std_debug': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'basic',
            },
            'std_info': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'basic',
            },
        },
        'loggers': {
            '': {
                'level': 'DEBUG',
                'handlers': [],
                'propagate': True,
            },
        },
    }

    if filename:
        base_name, ext = os.path.splitext(filename)
        debug_filename = '%s-debug%s' % (base_name, ext)
        local = {
            'local_debug': {
                'level': 'DEBUG',
                'class': 'logging.FileHandler',
                'filename': filename,
                'formatter': 'basic',
            },
            'local_info': {
                'level': 'INFO',
                'class': 'logging.FileHandler',
                'filename': filename,
                'formatter': 'basic',
            },
        }

        LOGGING['handlers'].update(local)
        if debug:
            handlers.append('local_debug')
        else:
            handlers.append('local_info')
    else:
        if debug:
            handlers.append('std_debug')
        else:
            handlers.append('std_info')

    for k, v in LOGGING['loggers'].items():
        if k != 'nginx':
            v['handlers'] = handlers

    logging.config.dictConfig(LOGGING)
    return LOGGING

if __name__ == '__main__':
    from pprint import pprint
    setup_logger()
    logging.info("test sql")
