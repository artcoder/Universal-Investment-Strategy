# Python code to automate a stock trading strategy
# Based on an idea from *The SPY-TLT Universal Investment Strategy (UIS) - Simple & Universal*
# https://logical-invest.com/universal-investment-strategy/
#
###
# David Guilbeau
# Version 0.0.0

import datetime
from datetime import timedelta
import operator
import pandas as pd
import yfinance as yf
import sqlite3
import pickle
import numpy as np
from sklearn.linear_model import LinearRegression
# import talib
import pandas_ta as ta
import plotly.express as px
import plotly.graph_objects as go

database_filename = r'.\stock_data.sqlite3'
pickle_filename = r'.\stock_group_df_0.0.0.pkl'
download = True
maximum_trading_days_needed = 300
volatility_factor = 2

maximum_calendar_days_needed = maximum_trading_days_needed * 365.25 / 253
# 253 trading days in a year
# 365.25 days in a year

# Set requested date range
finish_date = datetime.date.today()
# finish_date = datetime.datetime(2021, 7, 6)
start_date = finish_date - timedelta(days=maximum_calendar_days_needed)
print("Requested start:", start_date, "finish:", finish_date)

extra_days = 5  # extra days to look at in case the start date is not a trading day


def create_database_if_needed():
    global con

    cur = con.cursor()

    # If table does not exist, create it
    sql = '''
    CREATE TABLE  IF NOT EXISTS stock_data
    (date timestamp NOT NULL,
    ticker text NOT NULL,
    open real,
    high real,
    low real,
    close real,
    volume real,
    primary key(date, ticker)
    )
    '''
    cur.execute(sql)


def find_download_start_date(requested_start_date):
    global con
    # print("In find_download_start_date:", requested_start_date, type(requested_start_date))
    cur = con.cursor()

    # Find the last date in the database:
    sql = '''
    Select date From stock_data
    Order By date Desc
    Limit 1
    '''
    cur.execute(sql)
    rows = cur.fetchall()

    # if no date
    if len(rows) < 1:
        print('No rows found in database table.')
        download_start_date = requested_start_date
    else:
        print('Last date found in database:', rows[0][0])
        # Download the day after the one in the database
        download_start_date = rows[0][0].date() + timedelta(days=1)

    return download_start_date


def download_stock_data(download_start_date, download_finish_date):
    global con
    global stock_list

    if download:
        data = yf.download(stock_list,
                           start=(download_start_date - timedelta(days=extra_days)),
                           end=(download_finish_date + timedelta(days=1)),
                           group_by='ticker')

        data.to_pickle(pickle_filename)
    else:
        pickle_file = open(pickle_filename, 'rb')
        data = pickle.load(pickle_file)

    # https://stackoverflow.com/questions/63107594/how-to-deal-with-multi-level-column-names-downloaded-with-yfinance/63107801#63107801
    t_df = data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)
    t_df = t_df.reset_index()

    # This would insert dataframe data into database, but it fails if a date and ticker already exist
    # t_df.to_sql('stock_data', con, if_exists='append', index=False)

    print('Inserting data into database...')
    for i in range(len(t_df)):
        sql = 'insert into stock_data (date, ticker, close, high, low, open, volume) ' \
              'values (?,?,?,?,?,?,?)'
        try:
            cur.execute(sql, (t_df.iloc[i].get('Date').to_pydatetime(),
                              t_df.iloc[i].get('Ticker'),
                              t_df.iloc[i].get('Adj Close'),
                              t_df.iloc[i].get('High'),
                              t_df.iloc[i].get('Low'),
                              t_df.iloc[i].get('Open'),
                              t_df.iloc[i].get('Volume')))
        except sqlite3.IntegrityError:
            print("\r", "Failed inserting:", str(t_df.iloc[i][0]), t_df.iloc[i][1], end='')

    con.commit()
    print("\r                                                    ")
#


stock_list = ['SPY', 'TLT']

# detect_types is for timestamp support
con = sqlite3.connect(database_filename,
                      detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
cur = con.cursor()

create_database_if_needed()

# print("in main:", start_date, type(start_date))
download_start_date = find_download_start_date(start_date)

download_finish_date = finish_date

if download_start_date <= download_finish_date:
    download_stock_data(download_start_date, download_finish_date)
else:
    print("Not downloading.")

# Find actual start date
query = '''
select date from stock_data
order by date
limit 1
'''
cur.execute(query)
t = cur.fetchone()
print("Database start date:", t[0])

# Find actual finish date
query = '''
Select date From stock_data
Order By date Desc
limit 1
'''
cur.execute(query)
t = cur.fetchone()
print("Database finish date:", t[0])

####
# Calculate indicators
account_value = 100

print('Calculating indicators')

sql = '''
   Select date, ticker, close From stock_data
   Where ticker in (?, ?)
   Order By date Desc
   Limit 50
   '''
cur.execute(sql, stock_list)
stock_df = pd.DataFrame(cur.fetchall(),
                        columns=['date', 'ticker', 'close'])
stock_df = stock_df.set_index(['ticker', 'date']).sort_index()

con.close()

print("\rResults:")

normalized = {}
for stock in stock_list:
    print(stock)

    current_stock = stock_df.loc[stock]
    # print(working_on.head())

    first_day_price = current_stock.iloc[0].values[0]
    last_day_price = current_stock.iloc[-1].values[0]
    # print('first price, last price:', first_day_price, last_day_price)
    # print('absolute return:', last_day_price - first_day_price)
    return_percent = (last_day_price - first_day_price) / first_day_price
    print('return decimal percent:', return_percent)

    normalized[stock] = current_stock / first_day_price

    # print(normalized[stock].tail())

    sharpe_ratio = return_percent / (normalized[stock]['close'].std() ** volatility_factor)
    print('sharpe ratio:', sharpe_ratio)

portfolio = {}
sharpe_ratio = {}
plotly_x = []
plotly_y1 = []
plotly_y2 = []
max_value = 0
max_step = 0
for step in range(0, 11, 1):

    print('step', step, ':', end='')
    investment_1_percent = step * 10
    investment_2_percent = 100 - investment_1_percent
    print(investment_1_percent, investment_2_percent)

    portfolio[step] = \
        (normalized[stock_list[0]] * investment_1_percent / 100) + \
        (normalized[stock_list[1]] * investment_2_percent / 100)

    # print(portfolio[step].tail())

    portfolio_values = portfolio[step]['close']

    # needs to be an annualized percent
    return_percent = (portfolio_values[-1] - portfolio_values[0]) / portfolio_values[0]

    volatility = portfolio_values.std()
    # ta.volatility.ulcer_index(close, window=14, fillna=False)

    sharpe_ratio[step] = return_percent / (volatility ** volatility_factor)
    print('return, volatility:', round(return_percent, 3), round(portfolio_values.std(), 3))
    print('sharpe ratio:', round(sharpe_ratio[step], 3))

    if step == 0:
        max_value = sharpe_ratio[step]
        max_step = 0
    elif max_value < sharpe_ratio[step]:
        max_value = sharpe_ratio[step]
        max_step = step

    plotly_x.append(step)
    plotly_y1.append(sharpe_ratio[step])
    plotly_y2.append(portfolio_values.std() * 10)

# output = sorted(adjusted_slope.items(), key=operator.itemgetter(1), reverse=True)

print('max value of', max_value, 'at step:', max_step)
print(stock_list[0], max_step * 10, ';', stock_list[1], 100 - max_step * 10)

line1 = px.line(x=plotly_x, y=plotly_y1, title='sharpe ratio')
line2 = px.line(x=plotly_x, y=plotly_y2, title='std')
figure = go.Figure(data=line1.data + line2.data)
figure.update_layout(title='sharpe')
figure.show()
