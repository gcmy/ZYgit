
import pandas as pd
from mongo import ToolMongoClient
import numpy as np
if __name__ == "__main__":

    biz_mongo_client = ToolMongoClient("test.conf")
    newurl = list(biz_mongo_client.find_many("fx618result", projection={"url":1,"_id":0}))
    rowurl = list(biz_mongo_client.find_many("fx618url", projection={"url":1,"_id":0}))
    url = np.array(pd.read_csv("followmetxt.csv")).tolist()
    for item in url:
        record = {
            "url":item[1],
            "text": item[2]
        }

        biz_mongo_client.save_record(record, "followmenews")
    url=[]
    urlrow = []
    for item in newurl:
        url.append(item['url'])
    for item in rowurl:
        urlrow.append(item['url'])
    urlset=set(url)
    urlrowset = set(urlrow)
    record=urlrowset - urlset
    for item in record:
        biz_mongo_client.save_record({"url":item},"urlcha")
    # for item in urlrowset:
    #     i=urlrow.count(item)
    #     if i > 1:
    #         # record.append({"url":item,
    #         #                "count":i})
    #         while 1:
    #             biz_mongo_client.delete_one("fx618url", filter={"url": item})
    #             i =i - 1
    #             if i == 1:
    #                 break


    a=3