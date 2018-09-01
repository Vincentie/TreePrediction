__author__ = 'Jianxuan Liu'

import sqlite3
from tradeDays import getTradeDays
from datetime import datetime 
from datetime import timedelta
import pandas as pd
from bs4 import *
import urllib.request
import tushare as ts
import snownlp as sn
import numpy as np


class computeSentiments:
    """ Compute the sentimental grade of market every day in the time window.
    And in this part sentimental anaylysis, and contexts and words partioning 
    are implemented by Package SnowNLP.
    """

    def __init__(self, beginDate, endDate, saveUrl):
    	self.beginDate = beginDate
    	self.endDate = endDate
		self.con = sqlite3.connect(self.saveUrl)
		self.sql = "SELECT * from Message"
		self.messageDataFrame = pd.read_sql(self.sql, self.con)
		self.messageSentiments = {}
		self.__computeSentiments

	def __computeOneDaySentiments(self, date):
		meassageInOneDay = self.messageDataFrame[self.messageDataFrame.date == date + ' ' + '00:00:00']
		sentimentsGrades = []

		# If there is no news, we set this date's sentimental grade to be 0.5 (which is a neutral value).
		if len(meassageInOneDay.message) == 0:
			self.messageSentiments[date] = 0.5
		# If there is news, we get the sentimental grade of every item of news, and count their mean to be day's final grade.
		else:
			for Message in meassageInOneDay.message:
				s = sn.SnowNLP(Message)
				MessageSentGrades = np.mean([sn.SnowNLP(word).sentiments for word in s.words]) 	# The item of news' sentimental grade.
				sentimentsGrades.append(MessageSentGrades) 										# Append every item of news sentimental grade to the day's list.
			self.messageSentiments[date] = np.mean(sentimentsGrades)

	def __computeSentiments(self):
	    for date in getTradeDays(self.beginDate, self.endDate):
			self.__computeOneDaySentiments(date)

	def getSentiments(self):
		return self.messageSentiments