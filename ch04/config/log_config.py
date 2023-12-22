import logging
import logging.config
import os.path


def log_config():
    # If the 'logs' path does not exist, it will be created.
    if not os.path.exists(os.path.join('logs')):
        os.mkdir(os.path.join('logs'))

    # If the file 'etl_pipeline' does not exist, it will be created as an empty file.
    if not os.path.exists(os.path.join('logs', 'etl_pipeline.log')):
        f = open(os.path.join('logs', 'etl_pipeline.log'), 'a').close()

    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        },
        'handlers': {
            'default_handler': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                'formatter': 'standard',
                'filename': os.path.join('logs', 'etl_pipeline.log'),
                'encoding': 'utf8'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default_handler'],
                'level': 'DEBUG',
                'propagate': False
            }
        }
    }
    logging.config.dictConfig(config)


if __name__ == '__main__':
    log_config()
