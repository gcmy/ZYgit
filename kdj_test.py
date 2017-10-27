#!/usr/bin/env python 2.7
# -*- coding: utf-8 -*-
import pymongo
from zhangyang.kdjincre import KDJ
mongod_host = "192.168.1.15"
mongod_port = 10110
mon_conn = pymongo.MongoClient("mongodb://%s:%s@%s:%d/%s" % ("gdgjeRW", "gdgjeRW2016", mongod_host, mongod_port, "GDGJE"))
mon_conn["GDGJE"]["KDJ1"].remove()
dbTable = mon_conn["GDGJE"]["KDJ1"]
cfg = {
        "_VAL_TYPE": "KDJ_val",
        "biz_cfg_file": "test.conf",
        "base_cfg_file": "test.conf",
        "_ta_collection_name": "KDJ1",
        "class_type": "CU25000P",
        "time_class": "15mins"

}
kdj = KDJ(**cfg)
data1 = {"_id": "2017-08-14 08:15_15mins_CU25000P_KDJ_val", "K": 75.2, "D": 88.1, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data1)
x=kdj.get_behavior("CU25000P")
print(x["feature"])
data2 = {"_id": "2017-08-14 08:30_15mins_CU25000P_KDJ_val", "K": 88.1, "D": 75, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data2)

x=kdj.get_behavior("CU25000P")
print(x["feature"])
data3 = {"_id": "2017-08-14 08:45_15mins_CU25000P_KDJ_val", "K": 5, "D": 12, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data3)

x=kdj.get_behavior("CU25000P")
print(x["feature"])
data4 = {"_id": "2017-08-14 09:00_15mins_CU25000P_KDJ_val", "K": 25, "D": 12, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data4)

x=kdj.get_behavior("CU25000P")
print(x["feature"])
data5 = {"_id": "2017-08-14 09:15_15mins_CU25000P_KDJ_val", "K": 25, "D": 12, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data5)

x=kdj.get_behavior("CU25000P")
print(x["feature"])
data6 = {"_id": "2017-08-14 09:30_15mins_CU25000P_KDJ_val", "K": 33, "D": 12, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data6)

x=kdj.get_behavior("CU25000P")
print(x["feature"])
data7 = {"_id": "2017-08-14 10:00_15mins_CU25000P_KDJ_val", "K": 12, "D": 25, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data7)

x=kdj.get_behavior("CU25000P")
print(x["feature"])
data8 = {"_id": "2017-08-14 10:15_15mins_CU25000P_KDJ_val", "K": 25, "D": 12, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data8)

x=kdj.get_behavior("CU25000P")
print(x["feature"])

data9 = {"_id": "2017-08-14 10:30_15mins_CU25000P_KDJ_val", "K": 12, "D": 26, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data9)

x=kdj.get_behavior("CU25000P")
print(x["feature"])

data10 = {"_id": "2017-08-14 10:45_15mins_CU25000P_KDJ_val", "K": 44, "D": 12, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data10)
x=kdj.get_behavior("CU25000P")
print(x["feature"])

data11 = {"_id": "2017-08-14 11:00_15mins_CU25000P_KDJ_val", "K": 44, "D": 12, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data11)

x=kdj.get_behavior("CU25000P")
print(x["feature"])

data12 = {"_id": "2017-08-14 11:15_15mins_CU25000P_KDJ_val", "K": 12, "D": 44, "feature": [], "class_type": "CU25000P","time_class":"15mins","ta_type":"KDJ_val","operation":[],"common":[]}
dbTable.insert(data12)

x=kdj.get_behavior("CU25000P")
print(x["feature"])
kdj_records0=[{"feature":""},{"feature":""},{"feature":""}]
print("出现次数"+kdj.twojincha(kdj_records0, 0, 3, 0))
print("出现次数"+kdj.twojincha(kdj_records0, 0, 3, 1))

kdj_records0=[{"feature":""},{"feature":""},{"feature":""},{"feature":["KDJ金叉"]}]
print("出现次数"+kdj.twojincha(kdj_records0, 0, 4, 1))
kdj_records0=[{"feature":""},{"feature":""},{"feature":""},{"feature":["KDJ金叉"]},{"feature":["KDJ金叉"]}]
print("出现次数"+kdj.twojincha(kdj_records0, 0, 5, 1))
