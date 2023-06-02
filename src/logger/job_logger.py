import logging
#FORMAT = '%(asctime)s - %(levelname)s %(message)s'
#logging.basicConfig(format=FORMAT)
logger = logging.getLogger('Airflow')
logger.propagate = False
logger.setLevel('INFO')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%y/%m/%d %H:%M:%S')
ch.setFormatter(formatter)
logger.addHandler(ch)