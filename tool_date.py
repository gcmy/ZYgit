#!/usr/bin/python2.7
#-*- coding:utf-8 -*-

import time
from datetime import date
from datetime import timedelta

class ToolDate(object):
    @staticmethod
    def get_ymdhms_string():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

    @staticmethod
    def get_ymdh_string():
        return time.strftime("%Y-%m-%d %H", time.localtime(time.time()))

    @staticmethod
    def time_to_ymdhms_string(date_time):
        return time.strftime("%Y-%m-%d %H:%M:%S", date_time)

    @staticmethod
    def datetime_to_ymdhms_string(date_time):
        return date_time.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def get_ymd_string():
        return time.strftime("%Y-%m-%d", time.localtime(time.time()))

    @staticmethod
    def get_y_string():
        return time.strftime("%Y", time.localtime(time.time()))

    @staticmethod
    def get_delta_ago_datetime(date_time=date.today(), days=1):
        fewdays = timedelta(days=days)
        return date_time - fewdays
