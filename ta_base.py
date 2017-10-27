#!/usr/bin/python
# -*- coding:utf-8 -*-

from abc import abstractmethod
from zhangyang.mongo import ToolMongoClient
from zhangyang.tool_date import ToolDate


TIME_CLASS_DAY = "day"
TIME_CLASS_15MINS = "15mins"
TIME_CLASS_5MINS = "5mins"
TIME_CLASS_MIN = "min"

TIME_CLASS_DICT = {
    TIME_CLASS_DAY: 1,
    TIME_CLASS_15MINS: 15,
    TIME_CLASS_5MINS: 5,
    TIME_CLASS_MIN: 1,
}


class TABase(object):
    def __init__(self, **kwargs):
        self._VAL_TYPE = kwargs.get("_VAL_TYPE")
        self._year_str = ToolDate.get_y_string()
        self._time_class = kwargs.get("time_class")
        self.class_type = kwargs.get("class_type")
        self._biz_mongo_client = ToolMongoClient(kwargs.get("biz_cfg_file"))
        self._base_mongo_client = ToolMongoClient(kwargs.get("base_cfg_file"))
        self._ta_collection_name = kwargs.get("_ta_collection_name")
        self._quote_collection_name = kwargs.get("_quote_collection_name")
        # self._ta_collection_name = "ZMingCMTTARecord_test_%s" % self._year_str
        # self._quote_collection_name = "ZMingCMTMarketQuote_%s" % self._year_str

    @abstractmethod
    def cal_val(self, class_type):
        pass

    @abstractmethod
    def get_behavior(self, class_type):
        pass

    """
    return
    {"class_type":标的, "date_time":行情时间, "time_class":时间刻度级别, "open_price":开盘价, "close_price":收盘价, "min_price":最低价, "max_price":最高价}
    """
    """
    def get_new_market_quote(self, class_type):
        # 计算取的行情点
        if self._time_class == TIME_CLASS_15MINS:
            # 15分钟
            date_time_str = ToolDate.get_ymdh_string()
            mins = datetime.now().minute
            multi = int(mins / TIME_CLASS_DICT.get(self._time_class))
            date_time_str = "%s:%d" % (date_time_str, multi * TIME_CLASS_DICT.get(self._time_class))

        elif self._time_class == TIME_CLASS_DAY:
            # 日级
            date_time_str = str(ToolDate.get_delta_ago_datetime(date.today(), TIME_CLASS_DICT.get(self._time_class)))

        else:
            date_time_str = str(ToolDate.get_delta_ago_datetime(date.today(), TIME_CLASS_DICT.get(self._time_class)))

        filters = {"class_type": class_type, "date_time": date_time_str, "time_class": self._time_class}
        record = self._base_mongo_client.find_one(self._quote_collection_name, filter=filters)
        return record
    """

    def get_batch_new_market_quote(self, class_type, nums=None):
        # 获取过去的nums条行情记录
        filters = {"_id": {"$regex": "_%s_%s" % (self._time_class, class_type)}}
        if nums:
            quote_records = list(self._base_mongo_client.find_many(self._quote_collection_name, filter=filters, sort=[("_id", -1)], limit=nums))
        else:
            quote_records = list(self._base_mongo_client.find_many(self._quote_collection_name, filter=filters, sort=[("_id", -1)]))
        return quote_records


    def get_batch_norm_market_quote(self, class_type, nums=None):
        # 获取过去的nums条行情记录
        filters = {"_id": {"$regex": "_%s_%s" % (self._time_class, class_type)}}
        if nums:
            quote_records = list(self._base_mongo_client.find_many(self._quote_collection_name, filter=filters, sort=[("_id", 1)], limit=nums))
        else:
            quote_records = list(self._base_mongo_client.find_many(self._quote_collection_name, filter=filters, sort=[("_id", 1)]))
        return quote_records

    def get_batch_new_market_quote_gte_datetime(self, class_type, date_time):
        # 获取过去的nums条行情记录
        filters = {"$and": [{"_id": {"$gte": "%s_%s_%s" % (date_time, self._time_class, class_type)}}, {"_id": {"$regex": "_%s_%s" % (self._time_class, class_type)}}]}
        quote_records = list(self._base_mongo_client.find_many(self._quote_collection_name, filter=filters, sort=[("_id", -1)]))
        return quote_records


    def get_batch_new_ta_records(self, class_type, ta_type, nums=None):
        # 获取过去的nums条行情记录
        filters = {"_id": {"$regex": "_%s_%s_%s"%(self._time_class, class_type,ta_type)}}
        if nums:
            ta_records = list(self._biz_mongo_client.find_many(self._ta_collection_name, filter=filters, sort=[("_id", -1)], limit=nums))
        else:
            ta_records = list(self._biz_mongo_client.find_many(self._ta_collection_name, filter=filters, sort=[("_id", -1)]))
        return ta_records

    def set_record_tech_feature(self, record, feature, operation, common):
        record["feature"].append(feature)
        record["operation"].append(operation)
        record["common"].append(common)
        record["update_time"]=ToolDate.get_ymdhms_string()

        return record

    def save_record(self, record, collection_name):
        self._biz_mongo_client.save_record(record, collection_name)

    def get_zming_float(self, f, precision=2):
        if isinstance(f, float):
            # 保留的位数
            i = round(f * (10 ** precision))
            f = (i / (10 ** precision))
        return f