__author__ = 'Jianxuan Liu'

import sqlite3
from tradeDays import getTradeDays
from datetime import datetime 
from bs4 import *
import urllib.request

class parse_and_save_data:
    """ Parse relevant sentimental messages or trading data from SinaFinance 
    from beginDate to endDate and insert the data into a table in a database.

    In this part, beginDate and endDate indicates the time window for BS4 to parse, and
    saveUrl is given to indicate the database's address.
    """
    
    def __init__(self, beginDate='2012-01-01', endDate='2012-01-15', saveUrl='Data/FData.sqlite', option='au_trade'):
        self.beginDate = beginDate
        self.endDate = endDate
        self.dataDict = {}
        self.saveUrl = saveUrl
        self.option = option
  
    def __isRetrivingTradingData(self):
        """ Returns True if option is "au_trade"
        """
        return True if self.option == 'au_trade' else False

    def __getWebPage(self, date):
        """ Get an html page eligible for BeautifulSoup to parse.
        """
        if self.__isRetrivingTradingData():
            date = date[0:4] + date[5:7] + date[-2:] #"2017-05-10-->20170510"
            url = "http://www.shfe.com.cn/data/dailydata/kx/kx" + date + ".dat"
        else:
            url = "http://roll.finance.sina.com.cn/finance/gjs/hjzx/" + date + ".shtml"
        page = urllib.request.urlopen(url).read()
        if not page:
            raise ValueError("Nothing gets returned. Maybe a wrong date is selected.")
        return page

    def __convertDatetime(self, date):
        """ Get the datetime type of self.date(which is a str type).
        """
        return datetime(int(date[0:4]), int(date[5:7]), int(date[-2:]))

    def __updateSentiments(self, date):
        """ Parse all pieces of news / trading data in one day from SinaFinance (through BeautifulSoup), 
        and append them in the sentiments dict accroding to the date key.
        """
        web = self.__getWebPage(date)
        tradeDay = self.__convertDatetime(date)
        if self.__isRetrivingTradingData():
            web = eval(web)
            for dic in web["o_curinstrument"]:
                key = dic["PRODUCTID"].strip() + dic["DELIVERYMONTH"] # The contract name of the Au futures like "au1705".

                if (u"商品名称" in key) or (u"小计" in key) or (u"总计" in key):
                    continue
                
                Symbol = dic["PRODUCTID"].strip()[0:2]
                # Only records the data of Au Futures.
                if Symbol == 'au':
                    key1 = dic["PRODUCTID"].strip() + dic["DELIVERYMONTH"]
                    values1 = [tradeDay, Symbol, dic["PRODUCTNAME"], dic["PRESETTLEMENTPRICE"], dic["OPENPRICE"], dic["HIGHESTPRICE"], dic["LOWESTPRICE"], dic["CLOSEPRICE"], \
                        dic["SETTLEMENTPRICE"], dic["ZD1_CHG"], dic["ZD2_CHG"], dic["VOLUME"], dic["OPENINTEREST"], dic["OPENINTERESTCHG"]]

                try: self.dataDict.setdefault(key1, values1)
                except: pass
        else:
            soup = BeautifulSoup(web, 'lxml')
            source = soup.findAll('a', {'target':'_blank'})
            #Interate the contexts stored in source and insert them in to the sentiments dict.
            for item in source:
                text = item.get_text()
    			
                if (u"财经博客" in text) or (u"股票博客" in text) or (u"财经" in text):
                    continue
                key1 = text
                values1 = tradeDay
    		
                try: self.dataDict.setdefault(key1, values1)
                except: pass

    def __insertIntoTable(self, date):
        """ Insert the data in dataDict which cover all dates' data to a table in DB.
        """
        self.__updateSentiments(date)
        conna = sqlite3.connect(self.saveUrl)
        cursor = conna.cursor();

        if self.__isRetrivingTradingData():
            SQLquery1 = "create table if not exists SHFE(Contracts varchar(20), date datetime, Symbol varchar(10), prodctname nvarchar(30), PreSettlement numeric(15, 2), \
                    Open numeric(15, 2), High numeric(15, 2), Low numeric(15, 2), Close numeric(15, 2), Settlement numeric(15, 2), Change1 numeric(15, 2), \
                    Change2 numeric(15, 2), Volume numeric(25, 2), OpenInt numeric(25, 2), ChangeofOpenInt numeric(25, 2))"
            cursor.execute(SQLquery1)

            for key, value in self.dataDict.items():
                iter = (key, value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7], value[8], \
                    value[9], value[10], value[11], value[12], value[13])
                SQLquery2 = "insert into SHFE" + " " + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                cursor.execute(SQLquery2, iter)
        else:
            SQLquery1 = "create table if not exists Message(date datetime,  message nvarchar(200) )"
            cursor.execute(SQLquery1)

            #Insert the parsed sentimental contents into a sentiment table whose key is date.
            for key, value in self.dataDict.items():
                iter = (value, key)
                SQLquery2 = "insert into Message" + " " + "values(?, ?)"
                cursor.execute(SQLquery2, iter)

        conna.commit()
        conna.close()
    
    def parse_and_save(self):
        """ Parse all messages/trading data of a day and insert them into a table in a database using 
        __insertIntoSentTable method. Iterate all trading days from beginDate to endDate to perform this operation.
        """
        ErrorDate = []
        for date in getTradeDays(self.beginDate, self.endDate):
            try:
                self.__insertIntoTable(date)
            except Exception as e:
                print('Bug {0:^10}, arises in {1:^10}.'.format(str(e), date))
                ErrorDate.append(date)

        while len(ErrorDate) != 0:
            for date in ErrorDate:
                try:
                    self.__insertIntoTable(date)
                    ErrorDate.remove(date)
                except: pass

if __name__ == '__main__':
    r = parse_and_save_data()
    r.parse_and_save()