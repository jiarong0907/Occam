import logging

logging.basicConfig(
    # filename='sch_occam1.log',
    # filemode='w',
    format="%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d:%H:%M:%S",
)
logging.getLogger().setLevel(logging.ERROR)
logger = logging.getLogger()
