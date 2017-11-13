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
        #self.today = datetime.datetime.today()
        self.dr = webdriver.Firefox()
        self.base_url = "http://news.fx678.com/dateNews/"
        self.newlisthrefs = []


    def get_newslist(self):

        if self.isElementExist("more_new"):
            more_news = self.dr.find_element_by_id("more_new")
            while 1:
                if more_news.is_displayed():
                    more_news.click()
                else:
                    break
                # self.get_newstext(item.get_attribute("href"))
        newlisttag = self.dr.find_elements_by_class_name("content")
        for item in newlisttag:
            record = {
                "url":item.get_attribute("href")
            }
            self._biz_mongo_client.save_record(record, self._ta_collection_name)
            self.newlisthrefs.append(item.get_attribute("href"))

    def get_newstext(self):
        fx.newlisthrefs = list(self._biz_mongo_client.find_many(fx._ta_collection_name,sort=[("_id", 1)]))
        newslist = list(self._biz_mongo_client.find_many(fx.fx618_new,sort=[("_id", 1)]))
        i=0
        for item in fx.newlisthrefs:
            print(i,item['url'])
            i = i + 1
            if i > 608:
                text = self.isTextExist(item)
                if text:
                    newstext={
                        "url":item['url'],
                        "text":text
                    }
                    self._biz_mongo_client.save_record(newstext, self.fx618_new)



    def isElementExist(self, element):
        flag = True
        browser = self.dr
        try:
            browser.find_element_by_id("more_new")
            return flag

        except:
            flag = False
            return flag

    def isTextExist(self,item):
        flag = True
        try:
            self.dr.get(item['url'])
            text = self.dr.find_element_by_id("content").text.replace("\n", "")
            return text
        except:
            flag = False
            return flag


if __name__ == "__main__":
    cfg = {
        "base_cfg_file": "test.conf",
        "_ta_collection_name":"fx618",
        "fx618_new": "fx618result"
    }
    fx = fx678(**cfg)
    fx.get_newstext()
    # fx.newlisthrefs = list(ToolMongoClient.find_many(fx._ta_collection_name,filter=None, sort=[("_id", -1)]))
    a=3
    # i=0
    # while 1:
    #     print(i,fx.today.date())
    #     i = i+1
    #     fx.dr.get(fx.base_url+fx.today.strftime("%F"))
    #     if fx.dr.find_element_by_tag_name("body").text == "":
    #         break
    #     if fx.dr.find_element_by_id("newest2").text == "暂无数据！":
    #         fx.today=fx.today - datetime.timedelta(days=1)
    #         continue
    #     else:
    #         fx.get_newslist()
    #         fx.today = fx.today - datetime.timedelta(days=1)
    # # newstext=fx.get_newstext(fx.newlisthrefs)
    # # pd.DataFrame(newstext).to_csv("newstext.csv",index=None)







a=3