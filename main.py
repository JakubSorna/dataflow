from ETL_classes import Extractor, Transformer, Uploader

E = Extractor()
T = Transformer(E.get_data())
U = Uploader(T.get_weather(), T.get_day())
U.upload_data()