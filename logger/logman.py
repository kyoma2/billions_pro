import logging

def logger(name_app):
    # Configure the logging system
    LOG_FORMAT = "%(asctime)s  - %(message)s"
    logging.basicConfig(
        filename='{}.log'.format(name_app),
        level=logging.INFO,
        format=LOG_FORMAT
    )

    # Variables (to make the calls that follow work)
    # hostname = 'www.python.org'
    # item = 'spam'
    # filename = 'data.csv'
    # mode = 'r'

    # Example logging calls (insert into your program)
    # logging.critical('Host %s unknown', hostname)
    # logging.error("Couldn't find %r", item)
    # logging.warning('Feature is deprecated')
    # logging.info('Opening file %r, mode=%r', filename, mode)
    # logging.debug('Got here')

if __name__ == '__main__':
    logger("app.log")