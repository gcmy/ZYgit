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
from zhangyang.ta_base import TABase
from zhangyang.kdjincre import KDJ

class KDJnorm(KDJ):

    def __init__(self,**kwargs):
        KDJ.__init__(self, **kwargs)


    # 保留小数
    @staticmethod
    def get_zming_float(f, precision=2):
        if isinstance(f, float):
            # 保留的位数
            i = round(f * (10 ** precision))
            f = (i / (10 ** precision))
        return f

    def kdj(self):
        a=3
        quote_records = self.get_batch_norm_market_quote(self.class_type)


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
    cfg = {
        "_VAL_TYPE":"KDJ_val",
        "biz_cfg_file": "test.conf",
        "base_cfg_file": "test.conf",
        "_ta_collection_name":"kdj2",
        "_quote_collection_name":"ZMingCMTMarketQuote_2017"

    }
    f = KDJnorm(**cfg)
    for time_class in f.time_class_list:
        for class_type in f.class_type_list:
            kdj_records = f.kdj(time_class, class_type)
            #pd.DataFrame(kdj_records).to_csv("1026"+class_type+time_class + ".csv", index=None)  # 写入csv
