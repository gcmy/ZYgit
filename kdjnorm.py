#!/usr/bin/env python 3.5.2
# -*- coding: utf-8 -*-
"""
公式：
RSV(n) = (close_price-min_price)/(max_price-min_price)*100
close_price为第n日收盘价,min_price为N日内,max_price为N日内最大值 N取9
K(n)=2/3*k(n-1)+1/3*RSV(n)
D(n)=2/3*D(n-1)+1/3*K(n)
J(n)=3D-2K
K.D初始值设为50

判断方法：
1. k从下向上突破D，形成金叉，买多
2. K从上向下突破D，形成死叉，卖空
3.二次、三次金叉判断周期分别为6、10
"""
import pymongo
import pandas as pd
from zhangyang.tool_date import ToolDate


class KDJ(object):

    def __init__(self):
        self.market_conn = pymongo.MongoClient("192.168.1.15", 10110)
        self.db = self.market_conn["GDGJE"]
        self.db.authenticate("gdgjeRW", "gdgjeRW2016")
        self.market = self.db.ZMingCMTMarketQuote_2017  # 行情数据
        self.class_type_list = self.market.find().distinct("class_type")  # 获取种类
        self.time_class_list = ["15mins", "day"]
        self._QUOTE_M = 9
        self._RSV_N = 3
        self.time_gap2 = 6
        self.time_gap3 = 10
        self.record_list = []

    # 保留小数
    @staticmethod
    def get_zming_float(f, precision=2):
        if isinstance(f, float):
            # 保留的位数
            i = round(f * (10 ** precision))
            f = (i / (10 ** precision))
        return f

    def kdj(self, time_class, class_type):
        mongodDB = self.market_conn["GDGJE"]
        mongodTable = mongodDB["kdjtext"]
        kdj_records = []
        max_price_list = []
        min_price_list = []
        i = 0
        for record in self.market.find({"class_type": class_type, "time_class": time_class}).sort("_id"):
            close_price = record.get("close_price")
            max_price_list.append(record.get("max_price"))
            min_price_list.append(record.get("min_price"))
            if len(max_price_list) < self._QUOTE_M:
                m_rsv_val = None
                k_val = None
                d_val = None
                j_val = None
            else:
                if len(max_price_list) > self._QUOTE_M:
                    min_price_list.pop(0)
                    max_price_list.pop(0)
                m_min_price = min(min_price_list)
                m_max_price = max(max_price_list)
                last_kdj_record = kdj_records[i - 1]
                last_last_kdj_record = kdj_records[i - 2]
                if m_max_price == m_min_price:
                    m_rsv_val = last_kdj_record.get("N_RSV")
                else:
                    m_rsv_val = (close_price - m_min_price) / (m_max_price - m_min_price) * 100

                if last_kdj_record.get("N_RSV") is not None and last_last_kdj_record.get("N_RSV") is not None:
                    k_val = last_kdj_record.get("K") * 2 / 3 + m_rsv_val / 3
                    d_val = last_kdj_record.get("D") * 2 / 3 + k_val / 3
                    j_val = 3 * d_val - 2 * k_val
                else:
                    k_val = 50.0
                    d_val = 50.0
                    j_val = 50.0
            retain = {
                "_id": record.get("_id")+"_KDJ_cal",
                "open_price": record.get("open_price"),
                "date_time": record.get("date_time"),
                "close_price": record.get("close_price"),
                "min_price": record.get("min_price"),
                "max_price": record.get("max_price"),
                "rsv_N": 3,
                "class_type": class_type,
                "time_class": time_class,
                "ta_type": "KDJ_val",
                "N_RSV" : self.get_zming_float(m_rsv_val),
                "K" : self.get_zming_float(k_val),
                "D" : self.get_zming_float(d_val),
                "J" : self.get_zming_float(j_val),
                "operation": [],
                "feature": [],
                "common": [],
                "quote_M": self._QUOTE_M,
                "update_time": ToolDate.get_ymdhms_string(),
            }
            k = retain.get("K")
            d = retain.get("D")
            j = retain.get("J")
            if i >= self._QUOTE_M-1:  # Kdj值在_QUOTE_M周期后存在值
                if k > 90:
                    retain["feature"].append(u"K线超买")
                    retain["operation"].append(u"看空")
                    retain["common"].append(u"K线是快速确认线，数值在90以上为超买，数值在10以下为超卖，K线超买为卖空信号。")
                elif k is not None and k < 10:
                    retain["feature"].append(u"K线超卖")
                    retain["operation"] .append( u"看多")
                    retain["common"].append(u"K线是快速确认线，数值在90以上为超买，数值在10以下为超卖，K线超卖为买多信号。")
                if d > 80:
                    retain["feature"].append(u"D线超买")
                    retain["operation"].append(u"看空")
                    retain["common"].append(u"D线是慢速主干线，数值在80以上为超买，数值在20以下为超卖，D线超买为卖空信号。")
                elif d is not None and d < 20:
                    retain["feature"].append( u"D线超卖")
                    retain["operation"].append(u"看多")
                    retain["common"] .append(u"D线是慢速主干线，数值在80以上为超买，数值在20以下为超卖，D线超买为买多信号。")

                if j > 100:
                    retain["feature"].append(u"J线超买")
                    retain["operation"] .append(u"看空")
                    retain["common"].append( u"J线是方向敏感线，数值在100以上为超买，数值在0以下为超卖，J线超卖为卖空信号。")
                elif j is not None and j < 0:
                    retain["feature"].append(u"J线超卖")
                    retain["operation"].append(u"看多")
                    retain["common"].append(u"J线是方向敏感线，数值在100以上为超买，数值在0以下为超卖，J线超卖为买多信号。")

                last_kdj_record = kdj_records[i - 1]  # 取出倒数第二个记录判断来金叉
                if last_kdj_record.get("K"):
                    # 金叉死叉
                    if last_kdj_record.get("K") < last_kdj_record.get("D") and retain.get("K") > retain.get("D"):  # 金叉
                        retain["operation"].append(u"看多")
                        if i - self._QUOTE_M < self.time_gap2 and i > self._QUOTE_M + 1:
                            feature2 = twojincha(kdj_records, self._QUOTE_M + 1, i, True)
                        else:
                            feature2 = twojincha(kdj_records, i - self.time_gap2+1, i, True)
                        if i - self._QUOTE_M < self.time_gap3 and i > self._QUOTE_M + 1:
                            feature3 = twojincha(kdj_records, self._QUOTE_M + 1, i, True)
                        else:
                            feature3 = twojincha(kdj_records, i - self.time_gap3+1, i, True)
                        if feature3 == "KDJ三次金叉":
                            retain["feature"].append(feature3)
                            retain["common"].append(u"KDJ三次金叉，在%d周期内出现三次金叉，为强卖空信号" % self.time_gap3)
                        else:
                            retain["feature"].append(feature2)
                            if feature2 == u"KDJ二次金叉":
                                retain["common"].append(u"KDJ二次金叉，在%d周期内出现二次金叉，为强卖空信号" % self.time_gap2)
                            else:
                                retain["common"].append(u"KDJ金叉是指以K线从上向下与D线交叉为例，K线下穿D线并形成有效的向下突破是金叉，为卖空信号。")

                    elif last_kdj_record.get("K") > last_kdj_record.get("D") and retain.get("K") < retain.get("D"):  # 死叉
                        retain["operation"].append(u"看空")
                        if i - self._QUOTE_M < self.time_gap2 and i > self._QUOTE_M + 1:
                            feature2 = twojincha(kdj_records, self._QUOTE_M + 1, i, False)
                        else:
                            feature2 = twojincha(kdj_records, i - self.time_gap2+1, i, False)
                        if i - self._QUOTE_M < self.time_gap3 and i > self._QUOTE_M + 1:
                            feature3 = twojincha(kdj_records, self.time_gap3, i, False)
                        else:
                            feature3 = twojincha(kdj_records, i - self.time_gap3+1, i, False)
                        if feature3 == "KDJ三次死叉":
                            retain["feature"].append(feature3)
                            retain["common"].append(u"KDJ三次死叉，在%d周期内出现三次死叉，为强卖空信号" % self.time_gap3)
                        else:
                            retain["feature"].append(feature2)
                            if feature2 == u"KDJ二次死叉":
                                retain["common"].append(u"KDJ二次死叉，在%d周期内出现二次死叉，为强卖空信号" % self.time_gap2)
                            else:
                                retain["common"].append(u"KDJ死叉是指以K线从上向下与D线交叉为例，K线下穿D线并形成有效的向下突破是死叉，为卖空信号。")

            kdj_records.append(retain)
            mongodTable.save(retain)
            i = i+1
        return kdj_records


# 判断为二次金叉还是三次金叉
def twojincha(kdj_records,start,stop,booler):
    K = 0    
    if booler:
        str = set([u"KDJ金叉", u"KDJ二次金叉",u"KDJ三次金叉"])
        result = u"金叉"
    else:
        str = set([u"KDJ死叉", u"KDJ二次死叉",u"KDJ三次死叉"])
        result = u"死叉"
    for i in range(start, stop):
        X = set(kdj_records[i]["feature"])  # 取交集判断出现金叉或死叉
        if len(X & str) > 0:
            K = K + 1

    if K == 0:
        return u"KDJ"+result
    elif K == 1:
        return u"KDJ二次"+result
    else:
        return u"KDJ三次"+result


if __name__ == "__main__":
    f = KDJ()
    for time_class in f.time_class_list:
        for class_type in f.class_type_list:
            kdj_records = f.kdj(time_class, class_type)
            #pd.DataFrame(kdj_records).to_csv("1026"+class_type+time_class + ".csv", index=None)  # 写入csv
