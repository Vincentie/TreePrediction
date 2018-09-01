__author__ = 'JianxuanLiu'

from parse_and_save_data import parse_and_save_data
from tradeDays import getTradeDays
from computeSentimentals import computeSentiments
import numpy as np
import pandas as pd
from datetime import datetime 
from datetime import timedelta
import sqlite3

""" This main.py is performed to create a data table whose data can be picked to aplly a prediction tree.
    Specifically, volume weighted average prices of different gold futures' prices and sentimental grades
    processed by NLP methods are those data to be trained in a prediction tree model.
"""

def computeVWFactor(df, factor):
    """ Computer a series of factors in a DataFrame using volume weighted avearge method.
    """
    df = df.replace('', np.nan)
    df.fillna(method='pad')
    df.fillna(method='bfill')
    
    df1 = df[['Volume']].astype(float)
    df2 = df[[factor]].astype(float)
    df2.columns = ['Volume']
    
    vw = np.sum((df1 * df2)['Volume']) / np.sum(df1['Volume'])
    return vw


if __name__ == '__main__':
    beginDate = '2012-01-01'
    endDate = '2017-01-01'
    saveUrl1 = 'Data/FData.sqlite'
    saveUrl2 = 'Data/Sentiments.sqlite'

    """Parse and save data"""
    parse_and_save_data(beginDate, endDate, saveUrl1, option='au_trade').parse_and_save_data()
    parse_and_save_data(beginDate, endDate, saveUrl1, option='au_sentiments').parse_and_save_data()

    """Obatain the trading data and sentimental grades of every day for future use."""
    con = sqlite3.connect(saveUrl1)
    sql = "SELECT * from SHFE"
    data = pd.read_sql(sql, con)
    avgdata = {}
    sentiDict = computeSentiments(beginDate, endDate, saveUrl2).getSentiments()

    """Compute the volume weighted average factors and concatenate them with the sentimental grades."""
    for date in getTradeDays(beginDate, endDate):
        temp_df = data[data.date == date + ' ' + '00:00:00']
        values = []
        for item in ['Close', 'High', 'Low', 'Change2', 'ChangeofOpenInt']:
            values.append(computeVWFactor(temp_df, item))
        values.append(float(sum(temp_df['Volume'])))            #Add the total trading volume to the values.
        values.append(sentiDict[date])                          #Add sentimental grades to the values.
        key = date
        avgdata.setdefault(key, values)	

    """Insert the handled data into a new table."""
    conna = sqlite3.connect('Data/FAvgData.sqlite')
    cursor = conna.cursor()
    SQLquery1 = "create table if not exists SHFEAvg(date datetime, Close numeric(15,2), High numeric(15,2), Low numeric(15,2),\
                    Change2 numeric(15,2), ChangeofOpenInt numeric(25,2), Volume numeric(15,2), Sentiment numeric(15,2) )"
    cursor.execute(SQLquery1)

    for key, value in avgdata.items():
        iter = (key, value[0], value[1], value[2], value[3], value[4], value[5], value[6])
        SQLquery2 = "insert into SHFEAvg" + " " + "values(?, ?, ?, ?, ?, ?, ?, ?)"
        cursor.execute(SQLquery2, iter)
    conna.commit()
    conna.close()