__author__ = 'JianxuanLiu'

import sqlite3
from datetime import datetime 
from datetime import timedelta
import pandas as pd
from bs4 import *
import urllib.request
import tushare as ts
import snownlp as sn
import numpy as np

#SentimentsFromSina为爬取新浪财经一日新闻的class
class SentimentsFromSina:
    #define the attributes
    def __init__(self,date,SaveUrl):
        self.date=date
        self.url="http://roll.finance.sina.com.cn/finance/gjs/hjzx/"+self.date+".shtml"
        self.Sentiments={}
        self.SaveUrl=SaveUrl
  
    #get the html page
    def GetWebPage(self):
        MyPage=urllib.request.urlopen(self.url).read() #.decode("utf-8")
        if not MyPage:
            print("Dear Host,I can not find the web page")
        return (MyPage)

    ##convert str to datetime type
    def GetTheTradingDay(self):
        TradingDay=datetime(int(self.date[0:4]),int(self.date[5:7]),int(self.date[-2:]))
        return (TradingDay)
    
    #Get all pieces of news through BeautifulSoup when parsing pages of SinaFinance
    def GetNeededData(self):
        web = self.GetWebPage()  
        soup = BeautifulSoup(web, 'lxml')
        source = soup.findAll('a', {'target':'_blank'})
        
        TradingDay=self.GetTheTradingDay()
        #抓取文本并存入Sentiments字典
        for item in source:
            text = item.get_text()
			
            if (u"财经博客" in text) or (u"股票博客" in text) or (u"财经" in text):
                continue	
            key1 = text
            values1 = TradingDay
		
            try: self.Sentiments.setdefault(key1,values1)
            except: pass

    #Insert into DB
    def InsertDataToDB(self):
        self.GetNeededData()
        conna=sqlite3.connect(self.SaveUrl)
        cursor=conna.cursor();
        SQLquery1="create table if not exists Message(date datetime, message nvarchar(200) )"
        cursor.execute(SQLquery1)
		#日期与该日期爬取文本存入数据库
        for key,value in self.Sentiments.items():
            Iter=(value,key)
            SQLquery2="insert into Message"+" "+"values(?,?)"
            cursor.execute(SQLquery2,Iter)
        conna.commit()
        conna.close()

#GetTDays与TryDate是为获取一个time horizon里的所有交易日列表的函数
#如: GetTDays('2017-04-01','2017-04-10')-->['2017-04-05', '2017-04-06', '2017-04-07']
def GetTDays(beginDate, endDate):
    data = ts.trade_cal()
    result = []
    for item in data[data.isOpen==1].calendarDate:
        result.append(item)

    beginloc = result.index( TryDate(beginDate,result) )
    endloc = result.index( TryDate(endDate,result) )
    
    return (result[beginloc:endloc])
#试错函数，迭代直到返回交易日为止
def TryDate(date,result):
    iteration = 1
    while(date not in result):
        try: date = datetime.strptime(date,"%Y-%m-%d")
        except: pass
        date += timedelta(iteration)
        date = date.strftime("%Y-%m-%d")
    
    return (date)
	
#对所选time window进行每日爬取，并建立message舆情文本数据库
def CreateDB(BeginDate, EndDate, SaveUrl):
	ErrorDate = []
	for date in GetTDays(BeginDate,EndDate):
		try:
			test = SentimentsFromSina(date,SaveUrl)
			MyPage=test.InsertDataToDB()
		except Exception as e:
			print("something wrong")
			print(e)
			ErrorDate.append(date)
	while len(ErrorDate) != 0:
		for date in ErrorDate:
			try:
				test = SentimentsFromSina(date,SaveUrl)
				MyPage=test.InsertDataToDB()
				ErrorDate.remove(date)
			except: pass

#计算time window内的舆情文本得分,文本分词与情感打分工具由SnowNLP包完成
def ComputeSentiments(BeginDate, EndDate, SaveUrl):
	con = sqlite3.connect(SaveUrl)
	sql = "SELECT * from Message"
	Message_df = pd.read_sql(sql, con)
	MessageSentiments = {}
	for date in GetTDays(BeginDate, EndDate):
		MessageOfADay = Message_df[Message_df.date == date+' '+'00:00:00']
		SentimentsGrades = []
		#若当天没有舆情，则赋值舆情得分为0.5	
		if len(MessageOfADay.message) == 0:
			MessageSentiments[date] = 0.5
		#有舆情则计算每一则新闻的情感得分，所有得分平均值为当日舆情得分	
		else:
			for Message in MessageOfADay.message:
				s = sn.SnowNLP(Message)
				MessageSentGrades = np.mean([sn.SnowNLP(word).sentiments for word in s.words])
				SentimentsGrades.append(MessageSentGrades)
			MessageSentiments[date] = np.mean(SentimentsGrades)

	return (MessageSentiments)