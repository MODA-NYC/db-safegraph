from math import ceil
import datetime

PastQs = {'2019Q1':('2019-01-01', '2019-03-31'),
            '2019Q2':('2019-04-01', '2019-06-30'), 
            '2019Q3':('2019-07-01', '2019-09-30'),
            '2019Q4':('2019-10-01', '2019-12-31'),
            '2020Q1':('2020-01-01', '2020-03-31'),
            '2020Q2':('2020-04-01', '2020-06-30'), 
            '2020Q3':('2020-07-01', '2020-09-30'), 
            '2020Q4':('2020-10-01', '2020-12-31'),
            '2021Q1':('2021-01-01', '2021-03-31'),
            '2021Q2':('2021-04-01', '2021-06-30')
            }

def get_quarter():
    today = datetime.date.today()
    year_qrtr = str(today.year) + 'Q' + str(ceil(today.month/3.))
    start = datetime.date(today.year, 3*ceil(today.month/3.) - 2, 1)
    quarters = {year_qrtr:(str(start), str(today))}
    return quarters