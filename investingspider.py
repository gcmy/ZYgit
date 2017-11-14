from selenium import webdriver
import datetime
import pandas as pd
from mongo import ToolMongoClient

class fx678():
    def __init__(self,**kwargs):

        self._biz_mongo_client = ToolMongoClient(kwargs.get("base_cfg_file"))
        self._ta_collection_name = kwargs.get("_ta_collection_name")
        self.fx618_new = kwargs.get("fx618_new")
        self.today = datetime.datetime(2017,3,9,10,10)