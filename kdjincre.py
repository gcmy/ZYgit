#!/usr/bin/python
# -*- coding:utf-8 -*-
from zhangyang.ta_base import TABase
from zhangyang.tool_date import ToolDate


class KDJ(TABase):
    def __init__(self, **kwargs):
        self._QUOTE_M = 9
        self._RSV_N = 3
        self.time_gap2 = 6
        self.time_gap3 = 10
        TABase.__init__(self, **kwargs)
        
    """
    {
        "close_price": 收盘价,
        "open_price": 开盘价,
        "max_price": 最高价,
        "min_price": 最低价,
        "time_class": 时间刻度级别,
        "class_type": 标的,
        "ta_type": 技术指标类型KDJ_val,
        "date_time": 时间,
        "K": K值,
        "D": D值,
        "J": J值,
        "N_RSV": RSV(N),
        "rsv_N": N值,代表多少个精度,
        "quote_M":M值,代表多少个精度,
        "_id": "date_time_time_class_class_type_ta_type",
        "common":[技术特征注释],
        "feature": [技术特征],
        "operation": [出现特征后推荐操作]
    }
    QUOTE_M：rsv计算天数参数
    RSV_N:kdj计算天数参数

    计算公式：rsv = （第n天收盘价-n日内最低价）/（n日最高价-n日最低价） × 100
            k = 前日rsv / 3 + 前日k*2/3(此式子是在前3天rsv的指数移动平均化简而来，虽然化简后只涉及前日rsv，但化简前提建立在前3天rsv的条件下，所以仍然要有3天rsv才可以用这个式子）
            d = 当日k + 前日d*2/ 3
            j = 3*当日d - 2*当日k
    """
    def cal_val(self, class_type):
        # 取当前最新行情
        quote_records = self.get_batch_new_market_quote(class_type, self._QUOTE_M)
        if len(quote_records) == 0:
            return
        else:
            market_quote = quote_records[0]
        # 获取过去QUOTE_M - 1最新KDJ记录2
        kdj_records = self.get_batch_new_ta_records(class_type, self._VAL_TYPE, self._QUOTE_M - 1)
        if len(kdj_records) < self._QUOTE_M - 1:
            m_rsv_val = None
            k_val = None
            d_val = None
            j_val = None
        else:
            last_kdj_record = kdj_records[0]
            last_last_kdj_record = kdj_records[1]
            # 已经计算
            if last_kdj_record.get("date_time") == market_quote.get("date_time"):
                return
            # 计算rsv
            rsv_m_max_price_list = []
            rsv_m_min_price_list = []
            for i in range(len(quote_records)):
                record = quote_records[i]
                rsv_m_max_price_list.append(record.get("max_price"))
                rsv_m_min_price_list.append(record.get("min_price"))

            m_min_price = min(rsv_m_min_price_list)
            m_max_price = max(rsv_m_max_price_list)
            m_rsv_val = (market_quote.get("close_price") - m_min_price) / (m_max_price - m_min_price) * 100

            if last_kdj_record.get("N_RSV") is not None and last_last_kdj_record.get("N_RSV") is not None:
                k_val = last_kdj_record.get("K") * 2 / 3 + m_rsv_val / 3
                d_val = last_kdj_record.get("D") * 2 / 3 + k_val / 3
                j_val = 3 * d_val - 2 * k_val
            else:
                k_val = 50.0
                d_val = 50.0
                j_val = 50.0

        kdj_record = {
            # "close_price": market_quote.get("close_price"),
            # "open_price": market_quote.get("open_price"),
            # "max_price": market_quote.get("max_price"),
            # "min_price": market_quote.get("min_price"),
            "time_class": self._time_class,
            "class_type": class_type,
            "ta_type": self._VAL_TYPE,
            "date_time": market_quote.get("date_time"),
            # "update_time": ToolDate.get_ymdhms_string(),
            "K": self.get_zming_float(k_val),
            "D": self.get_zming_float(d_val),
            "J": self.get_zming_float(j_val),

            "N_RSV": self.get_zming_float(m_rsv_val),
            "rsv_N": self._RSV_N,
            "quote_M": self._QUOTE_M,
            "_id": "%s_%s_%s_%s" % (market_quote.get("date_time"), self._time_class, class_type, self._VAL_TYPE),
            "common": [],
            "feature": [],
            "operation": [],
            "real_times": []
        }

        # 存储最新的kdj值
        self.save_record(kdj_record, self._ta_collection_name)
        # print "save kdj val"

    # 获取feature等值
    def get_behavior(self, class_type):
        length = self.time_gap3
        # 获取过去的最长周期KDJ记录
        kdj_records = self.get_batch_new_ta_records(class_type, self._VAL_TYPE, length)
        if len(kdj_records) > 0:
            new_kdj_record = kdj_records[0]
            # 如果已经计算
            if len(new_kdj_record.get("feature")) != 0:
                return

            k = new_kdj_record.get("K")
            d = new_kdj_record.get("D")
            j = new_kdj_record.get("J")

            if k is not None and k > 90:
                new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"K线超买", operation=u"看空",
                                                              common=u"K线是快速确认线，数值在90以上为超买，数值在10以下为超卖，K线超买为卖空信号。")
            elif k is not None and k < 10:
                new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"K线超卖", operation=u"看多",
                                                              common=u"K线是快速确认线，数值在90以上为超买，数值在10以下为超卖，K线超卖为买多信号。")

            if d is not None and d > 80:
                new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"D线超买", operation=u"看空",
                                                              common=u"D线是慢速主干线，数值在80以上为超买，数值在20以下为超卖，D线超买为卖空信号。")
            elif d is not None and d < 20:
                new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"D线超卖", operation=u"看多",
                                                              common=u"D线是慢速主干线，数值在80以上为超买，数值在20以下为超卖，D线超卖为买多信号。")

            if j is not None and j > 100:
                new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"J线超买", operation=u"看空",
                                                              common=u"J线是方向敏感线，数值在100以上为超买，数值在0以下为超卖，J线超卖为卖空信号。")
            elif j is not None and j < 0:
                new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"J线超卖", operation=u"看多",
                                                              common=u"J线是方向敏感线，数值在100以上为超买，数值在0以下为超卖，J线超买为买多信号。")
        else:
            return

        if len(kdj_records) >= 2:
            last_kdj_record = kdj_records[1]
            # 金叉死叉
            # 金叉
            if last_kdj_record.get("K") < last_kdj_record.get("D") and new_kdj_record.get("K") > new_kdj_record.get("D"):
                feature2 = KDJ.twojincha(kdj_records,1,min(self.time_gap2,len(kdj_records)), True)
                feature3 = KDJ.twojincha(kdj_records,1,min(self.time_gap3,len(kdj_records)), True)
                if feature3 == 'KDJ三次金叉':
                    new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"KDJ三次金叉", operation=u"看多",
                                common = u"在%s周期内出现KDJ三次金叉，为强买信号。" % self.time_gap3)
                elif feature2 == 'KDJ二次金叉':
                    new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"KDJ二次金叉", operation=u"看多",
                                common = u"在%s周期内出现KDJ二次金叉，为强买信号。" % self.time_gap2)
                else:
                    new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"KDJ金叉", operation=u"看多",
                                common = u"KDJ金叉是指以K线从下向上与D线交叉为例，K线上穿D线并形成有效的向上突破是金叉，为买多信号。")
            # 死叉
            elif last_kdj_record.get("K") > last_kdj_record.get("D") and new_kdj_record.get("K") < new_kdj_record.get("D"):
                feature2 = KDJ.twojincha(kdj_records, 1, min(self.time_gap2,len(kdj_records)), False)
                feature3 = KDJ.twojincha(kdj_records, 1, min(self.time_gap3,len(kdj_records)), False)
                if feature3 == 'KDJ三次死叉':
                    new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"KDJ三次死叉", operation=u"看空",
                                common= u"在%s周期内出现KDJ三次死叉，为强卖信号。" % self.time_gap3)
                elif feature2 == 'KDJ二次死叉':
                    new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"KDJ二次死叉", operation=u"看空",
                                common= u"在%s周期内出现KDJ二次死叉，为强卖信号。" % self.time_gap2)
                else:

                    new_kdj_record = self.set_record_tech_feature(new_kdj_record, feature=u"KDJ死叉", operation=u"看空",
                                common= u"KDJ死叉是指以K线从上向下与D线交叉为例，K线下穿D线并形成有效的向下突破是死叉，为卖空信号。")

        # 存储最新的kdj值
        self.save_record(new_kdj_record, self._ta_collection_name)
        return new_kdj_record

    # 判断为二次金叉还是三次金叉
    @staticmethod
    def twojincha(kdj_records, start, stop, booler):
        K = 0
        if booler:
            str = set([u'KDJ金叉', u'KDJ二次金叉', u'KDJ三次金叉'])
            result = u"金叉"
        else:
            str = set([u'KDJ死叉', u'KDJ二次死叉', u'KDJ三次死叉'])
            result = u"死叉"
        while start<stop:
            X = set(kdj_records[start]["feature"])
            if len(X & str) > 0:
                K = K + 1
            start = start + 1
        if K == 0:
            return u"KDJ" + result
        elif K == 1:
            return u"KDJ二次" + result
        else:
            return u"KDJ三次" + result


if __name__ == '__main__':

    cfg = {
        "_VAL_TYPE":"KDJ_val",
        "biz_cfg_file": "test.conf",
        "base_cfg_file": "test.conf",
        "_ta_collection_name":"kdjtext",
        "_quote_collection_name":"ZMingCMTMarketQuote_2017"

    }
    class_types = ["CU25000P", "MY1000T", "NG10000", "SI156KG"]
    time_classes = ["15mins", "day"]
    for class_type in class_types:
        for time_class in time_classes:
            cfg["class_type"] = class_type
            cfg["time_class"] = time_class
            kdj = KDJ(**cfg)
            kdj.cal_val(class_type)
            kdj.get_behavior(class_type)