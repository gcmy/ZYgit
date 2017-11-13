from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import time
import datetime
import pandas as pd
from mongo import ToolMongoClient


class followme():

    def __init__(self,**kwargs):
        self._biz_mongo_client = ToolMongoClient(kwargs.get("base_cfg_file"))
        self._ta_collection_name = kwargs.get("_ta_collection_name")
        self.followmetxt= kwargs.get("followmetxt")
        self.dr = webdriver.Firefox()
        self.base_url = "http://www.followme.com/social/financenews/"
        self.newlisthrefs = []

    def get_newslist(self):

        self.dr.get(self.base_url)
        ActionChains(self.dr).click().perform()
        url = []
        #z=dr.find_elements_by_tag_name("a")
        while 1:
            ActionChains(self.dr).key_down(Keys.PAGE_DOWN).perform()
            # time.sleep(0.5)
            if len(self.dr.find_elements_by_class_name("ffitem")) >= 500:
                break
        for item in self.dr.find_elements_by_class_name("ffitem"):
            if self.isElementExist("leftimg",item):

                newsurl = item.find_element_by_class_name("leftimg").find_element_by_tag_name("a").get_attribute("href")
                record = {
                    "url": newsurl
                }
                self._biz_mongo_client.save_record(record, self._ta_collection_name)
        newstext=[]

    def get_newstext(self):
        fx.newlisthrefs = list(self._biz_mongo_client.find_many(fx._ta_collection_name,sort=[("_id", 1)]))
        newslist = list(self._biz_mongo_client.find_many(fx._ta_collection_name,sort=[("_id", 1)]))
        for item in fx.newlisthrefs:
            self.dr.get(item['url'])
            text = self.dr.find_element_by_id("long_content").text.replace("\n", "")
            newstext={
                "_id":item['url'],
                "text":text
            }

            self._biz_mongo_client.save_record(newstext, self.followmetxt)
# for item in url:
#     webnews = dr.get(item)
#     text = dr.find_element_by_id("long_content").text.replace("\n", "")
#     newstext.append({
#         "url": item,
#         "text": text
#     })
# pd.DataFrame(newstext).to_csv("newstext.csv",index=None)


    def isElementExist(self, element,browser):
        flag = True
        try:
            browser.find_element_by_class_name(element)
            return flag

        except:
            flag = False
            return flag

if __name__ == "__main__":
    cfg = {
        "base_cfg_file": "test.conf",
        "_ta_collection_name":"followme",
        "followmetxt": "followmetxt"
    }
    fx = followme(**cfg)
    # fx.get_newslist()
    fx.get_newstext()


a=3