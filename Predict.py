__author__ = 'JianxuanLiu'

from CreateDB import MarketDataFromSHFE
from CreateDB import SentimentsFromSina
import numpy as np
import pandas as pd
from datetime import datetime 
from datetime import timedelta
import sqlite3
import tushare as ts

#设置参数 并爬取黄金每日数据+黄金每日舆情数据 放入数据库中
#用于训练的数据
BeginDate = '2016-11-01'
EndDate = '2017-05-12'
SaveUrl1 = 'FData_Pre.sqlite'
SaveUrl2 = 'Sentiments_Pre.sqlite'

MarketDataFromSHFE.CreateDB(BeginDate, EndDate, SaveUrl1)
SentimentsFromSina.CreateDB(BeginDate, EndDate, SaveUrl2)

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

#对任一指标按不同合约的Volume进行加权，以计算Volume Weighted平均指标
def ComputeVWFactor(df, factor):
    df = df.replace('',np.nan)
    df.fillna(method = 'pad')
    df.fillna(method = 'bfill')
    
    df1 = df[['Volume']].astype(float)
    df2 = df[[factor]].astype(float)
    df2.columns = ['Volume']
    
    vw = np.sum((df1*df2)['Volume'])/np.sum(df1['Volume'])
    return (vw)
	
#以下为构建time window里黄金每日的平均价格及其他平均指标的数据库
con = sqlite3.connect(SaveUrl1)
sql = "SELECT * from SHFE"
data = pd.read_sql(sql, con)
avgdata = {}

#获取每日舆情得分字典
SentiDict = SentimentsFromSina.ComputeSentiments(BeginDate, EndDate, SaveUrl2)

#将黄金每日各平均指标与黄金每日舆情得分合并，生成一数据字典
for date in GetTDays(BeginDate, EndDate):
    temp_df = data[data.date == date+' '+'00:00:00']
    values = []
    for item in ['Close', 'High', 'Low', 'Change2', 'ChangeofOpenInt']:
        values.append( ComputeVWFactor(temp_df, item) )
    values.append(float(sum(temp_df['Volume'])))#把当日交易总量加入values中
    values.append(SentiDict[date]) #把情绪分加入values中
    key = date
    avgdata.setdefault(key,values)	

#将以上字典输入数据库-得到黄金期货的每日数据指标
conna=sqlite3.connect('FAvgData_Pre.sqlite')
cursor=conna.cursor()
SQLquery1="create table if not exists SHFEAvg(date datetime, Close numeric(15,2), High numeric(15,2), Low numeric(15,2),\
                Change2 numeric(15,2), ChangeofOpenInt numeric(25,2), Volume numeric(15,2), Sentiment numeric(15,2) )"
cursor.execute(SQLquery1)

for key,value in avgdata.items():
    Iter=(key,value[0],value[1],value[2],value[3],value[4],value[5],value[6])
    SQLquery2="insert into SHFEAvg"+" "+"values(?,?,?,?,?,?,?,?)"
    cursor.execute(SQLquery2,Iter)
conna.commit()
conna.close()