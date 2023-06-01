import logging
#FORMAT = '%(asctime)s - %(levelname)s %(message)s'
#logging.basicConfig(format=FORMAT)
logger = logging.getLogger('Fake Streamer')
logger.propagate = False
logger.setLevel('INFO')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)