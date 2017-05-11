__author__ = 'JianxuanLiu'

import urllib.request
import urllib.parse
from pyquery import PyQuery
import sqlite3
import pandas as pd
from datetime import datetime 
from datetime import timedelta
import tushare as ts

#MarketDataFromSHFE为爬取上期所一天合约交易数据的class
class MarketDataFromSHFE:
    #define the attributes
    def __init__(self,date,SaveUrl):
        self.date = date[0:4]+date[5:7]+date[-2:]#"2017-05-10-->20170510"
        self.url = "http://www.shfe.com.cn/data/dailydata/kx/kx"+self.date+".dat"
        self.Data = {}
        self.SaveUrl = SaveUrl
  
    #get the html page
    def GetWebPage(self):
        MyPage = urllib.request.urlopen(self.url).read() #.decode("utf-8")
        if not MyPage:
            print("Dear Host,I can not find the web page")
        return (MyPage)

    #convert str to datetime type
    def GetTheTradingDay(self):
        TradingDay=datetime(int(self.date[0:4]),int(self.date[4:6]),int(self.date[-2:]))
        return (TradingDay)
    
    #Get the data we needed from the origin file
    def GetNeededData(self):
        MyPage=eval( self.GetWebPage() )
        TradingDay=self.GetTheTradingDay()
        
        for dic in MyPage["o_curinstrument"]:
            key=dic["PRODUCTID"].strip()+dic["DELIVERYMONTH"] #关键字选取-如au1705
            if (u"商品名称" in key) or (u"小计" in key) or (u"总计" in key):
                continue
            Symbol=dic["PRODUCTID"].strip()[0:2]
			#仅记录au期货的数据
            if Symbol == 'au':
                key1 = dic["PRODUCTID"].strip()+dic["DELIVERYMONTH"]
                values1=[TradingDay,Symbol,dic["PRODUCTNAME"],dic["PRESETTLEMENTPRICE"],dic["OPENPRICE"],dic["HIGHESTPRICE"],dic["LOWESTPRICE"],dic["CLOSEPRICE"],\
                    dic["SETTLEMENTPRICE"],dic["ZD1_CHG"],dic["ZD2_CHG"],dic["VOLUME"],dic["OPENINTEREST"],dic["OPENINTERESTCHG"]]
            try: self.Data.setdefault(key1,values1)
            except: pass

    #Insert into DB
    def InsertDataToDB(self):
        self.GetNeededData()
        conna=sqlite3.connect(self.SaveUrl)

        cursor=conna.cursor()
        SQLquery1="create table if not exists SHFE(Contracts varchar(20),date datetime,Symbol varchar(10),prodctname nvarchar(30),PreSettlement numeric(15,2),\
                  Open numeric(15,2),High numeric(15,2),Low numeric(15,2),Close numeric(15,2),Settlement numeric(15,2),Change1 numeric(15,2),\
                  Change2 numeric(15,2),Volume numeric(25,2),OpenInt numeric(25,2),ChangeofOpenInt numeric(25,2))"
        cursor.execute(SQLquery1)
        for key,value in self.Data.items():
            Iter=(key,value[0],value[1],value[2],value[3],value[4],value[5],value[6],value[7],value[8],value[9],value[10],value[11],value[12],value[13])
            SQLquery2="insert into SHFE"+" "+"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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

#对所选time window进行每日爬取，并建立黄金期货数据库
def CreateDB(BeginDate, EndDate, SaveUrl):
	ErrorDate = []
	for date in GetTDays(BeginDate,EndDate):
		try:
			test = MarketDataFromSHFE(date,SaveUrl)
			MyPage=test.InsertDataToDB()
		except Exception as e:
			print("something wrong in "+date)
			print(e)
			ErrorDate.append(date)
	while len(ErrorDate) != 0:
		for date in ErrorDate:
			try:
				test = MarketDataFromSHFE(date,SaveUrl)
				MyPage=test.InsertDataToDB()
				ErrorDate.remove(date)
			except: pass