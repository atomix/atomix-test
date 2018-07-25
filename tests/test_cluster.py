from atomixtest.logging import logger

def test_setup_shutdown_cluster():
    logger.debug('This is a debug message')
    logger.info('This is an info message')
    logger.warn('This is a warn message')
    logger.error('This is an error message')

def test_add_node():
    logger.debug('This is another debug message')
    logger.info('This is another info message')
    logger.warn('This is another warn message')
    logger.error('This is another error message')
    raise ValueError("Something went wrong!")
