import datetime
import logging
import azure.functions as func

from ETL_classes import Transformer, Uploader

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Function triggered')

    T = Transformer()
    U = Uploader(T.get_weather(), T.get_day())
