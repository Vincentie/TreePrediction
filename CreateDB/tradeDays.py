__author__ = 'Jianxuan Liu'

from datetime import datetime 
from datetime import timedelta
import tushare as ts


def getTradeDays(beginDate, endDate):
    """ Get a list of trading dates from beginDate to endDate.

    >>> t = getTradeDays('2017-04-01', '2017-04-10')
    >>> t
    ['2017-04-05',  '2017-04-06',  '2017-04-07']
    """

    def tryDate(date, result):
        """ Returns a date if it is in result, or switches to the next date till it is in result.
        """
        if (date in result): 
            return date
        else:
            iteration = 1
            try: date = datetime.strptime(date, "%Y-%m-%d")
            except: pass
            date += timedelta(iteration)
            date = date.strftime("%Y-%m-%d")
            return tryDate(date, result)

    data = ts.trade_cal()
    result = []
    for item in data[data.isOpen == 1].calendarDate:
        result.append(item)

    beginloc = result.index(tryDate(beginDate, result))
    endloc = result.index(tryDate(endDate, result))

    return result[beginloc:endloc]
