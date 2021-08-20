# Python code to automate a stock trading strategy
# Based on an idea from *The SPY-TLT Universal Investment Strategy (UIS) - Simple & Universal*
# https://logical-invest.com/universal-investment-strategy/
#
###
# David Guilbeau
# Version 0.0.1

import datetime
from datetime import timedelta
import pandas as pd
import yfinance as yf
import ta
import sqlite3
import pickle
import plotly.express as px
import plotly.graph_objects as go

stock_list = ['SPY', 'TLT']
database_filename = r'.\stock_data.sqlite3'
pickle_filename = r'.\stock_group_df_0.0.0.pkl'
download = True
maximum_trading_days_needed = 1200
volatility_factor = 2
trading_days_window = 20

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


# downloads stock data to the database
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


def calculate_allocation(stock_df):
    # Find performance of the component stocks alone
    normalized = {}
    for stock in stock_list:
        # print(stock)

        current_stock = stock_df.loc[stock]
        # print(working_on.head())

        first_day_price = current_stock.iloc[0]['close']
        last_day_price = current_stock.iloc[-1]['close']
        # print('first price, last price:', first_day_price, last_day_price)
        # print('absolute return:', last_day_price - first_day_price)
        return_percent = (last_day_price - first_day_price) / first_day_price
        # print('return decimal percent:', round(return_percent, 3))

        # print('current_stock :', current_stock)
        normalized[stock] = current_stock / first_day_price

        ui_df = ta.volatility.UlcerIndex(current_stock['close'], window=trading_days_window).ulcer_index()
        volatility = ui_df.iloc[-1]

        # print(normalized[stock].tail())

        performance_ratio = return_percent / (volatility ** volatility_factor)
        # print('performance ratio:', round(performance_ratio, 3))

    # Find the performance of portfolios in different ratios
    portfolio = {}
    performance_ratio = {}
    return_percent = {}
    plotly_x = []
    plotly_y1 = []
    plotly_y2 = []
    max_value = 0
    max_step = 0
    for step in range(0, 11, 1):

        # print('step ', step, ': ', end='', sep='')
        investment_1_percent = step * 10
        investment_2_percent = 100 - investment_1_percent
        # print(investment_1_percent, investment_2_percent)

        portfolio[step] = \
            (normalized[stock_list[0]] * investment_1_percent / 100) + \
            (normalized[stock_list[1]] * investment_2_percent / 100)

        # print(portfolio[step].tail())

        portfolio_values = portfolio[step]['close']
        # print('length:', len(portfolio_values))
        # print("portfolio[step]['close']:", portfolio[step]['close'])

        # todo: change to be an annualized percent?
        return_percent[step] = (portfolio_values[-1] - portfolio_values[0]) / portfolio_values[0]

        ui_df = ta.volatility.UlcerIndex(portfolio[step]['close'], window=trading_days_window).ulcer_index()
        # print('ui_df:', ui_df)
        volatility = ui_df.iloc[-1]

        performance_ratio[step] = return_percent[step] / (volatility ** volatility_factor)
        # print('return, volatility, performance ratio:',
        #      round(return_percent, 3),
        #      round(volatility, 3),
        #      round(performance_ratio[step], 3))

        if step == 0:
            max_value = performance_ratio[step]
            max_step = 0
        elif max_value < performance_ratio[step]:
            max_value = performance_ratio[step]
            max_step = step

        plotly_x.append(step)
        plotly_y1.append(performance_ratio[step])
        plotly_y2.append(volatility)

    # output = sorted(adjusted_slope.items(), key=operator.itemgetter(1), reverse=True)
    # print('Max value of', round(max_value, 3), 'at step:', max_step)
    # print('return: ', return_percent[step])
    print(stock_list[0], max_step * 10, ';', stock_list[1], 100 - max_step * 10)

    line1 = px.line(x=plotly_x, y=plotly_y1, title='performance ratio')
    line2 = px.line(x=plotly_x, y=plotly_y2, title='volatility')
    figure = go.Figure(data=line1.data + line2.data)
    figure.update_layout(title='performance')
    # figure.show()

    return max_step * 10
#


def calculate_forward_return(future_df, allocation):

    normalized = {}
    return_percent = {}

    if len(future_df) == 0:
        print('future_df is empty')
        return 0

    stock_df = future_df

    for stock in stock_list:

        current_stock = stock_df.loc[stock]

        first_day_price = current_stock.iloc[0]['close']
        last_day_price = current_stock.iloc[-1]['close']
        # print('first price, last price:', first_day_price, last_day_price)
        # print('absolute return:', last_day_price - first_day_price)
        return_percent[stock] = (last_day_price - first_day_price) / first_day_price
        # print(stock, 'return decimal percent:', round(return_percent[stock], 3))

        # print('current_stock :', current_stock)
        normalized[stock] = current_stock / first_day_price

        # print(normalized[stock].tail())

    # figure the allocations
    investment_1_percent = allocation
    investment_2_percent = 100 - investment_1_percent

    portfolio = \
        (normalized[stock_list[0]] * investment_1_percent / 100) + \
        (normalized[stock_list[1]] * investment_2_percent / 100)

    # print(portfolio[step].tail())

    portfolio_values = portfolio['close']
    # print('length:', len(portfolio_values))
    # print("portfolio[step]['close']:", portfolio[step]['close'])

    return_percent['portfolio'] = (portfolio_values[-1] - portfolio_values[0]) / portfolio_values[0]

    return return_percent
#


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

# Debug output
# Find actual start date
query = '''
select date from stock_data
order by date
limit 1
'''
cur.execute(query)
t = cur.fetchone()
print("Database start date:", t[0])

# Debug output
# Find actual finish date
query = '''
Select date From stock_data
Order By date Desc
limit 1
'''
cur.execute(query)
t = cur.fetchone()
print("Database finish date:", t[0])

# Load the database table into a dataframe
sql = '''
   Select date, ticker, close From stock_data
   Where ticker in (?, ?) 
   Order By date Asc
   '''
sql_arguments = stock_list.copy()
cur.execute(sql, sql_arguments)
input_df = pd.DataFrame(cur.fetchall(),
                        columns=['date', 'ticker', 'close'])
con.close()

####
account_value = 100

# window_start and window_finish are in days relative to dataframe rows
window_start = 0
window_finish = window_start + trading_days_window
end_of_stock_df = len(input_df)
print('end_of_stock_df:', end_of_stock_df)

running_return =\
    {stock_list[0]: 1,
     stock_list[1]: 1,
     'portfolio'  : 1}

quit_loop = False
first_loop = True
while True:
    print('---')
    # print('window_finish:', window_finish)

    # length of stock_list is 2
    stock_df = input_df.iloc[window_start * 2:window_finish * 2]
    print('past window:', stock_df.iloc[0]['date'], stock_df.iloc[-1]['date'])
    stock_df = stock_df.set_index(['ticker', 'date']).sort_index()

    allocation = calculate_allocation(stock_df)

    # make future_df be from the end of stock_df to 5 days after that
    future_df = input_df.iloc[(window_finish - 1) * 2:(window_finish - 1 + 5) * 2]

    if first_loop:
        entire_future_df = input_df.iloc[(window_finish - 1) * 2:]
        first_loop = False

    print('future window:', future_df.iloc[0]['date'], future_df.iloc[-1]['date'])
    # print('future_df:', future_df)
    future_df = future_df.set_index(['ticker', 'date']).sort_index()

    # show the return compared to the component investments
    actual_return = calculate_forward_return(future_df, allocation)
    print(stock_list[0], ':', round(actual_return[stock_list[0]], 3))
    print(stock_list[1], ':', round(actual_return[stock_list[1]], 3))
    # print('actual_return:', round(actual_return['portfolio'], 3))

    running_return[stock_list[0]] = running_return[stock_list[0]] * (1 + actual_return[stock_list[0]])
    running_return[stock_list[1]] = running_return[stock_list[1]] * (1 + actual_return[stock_list[1]])
    running_return['portfolio'] = running_return['portfolio'] * (1 + actual_return['portfolio'])

    window_finish = window_finish + 4
    window_start = window_start + 4

    if quit_loop:
        break

    # MAGIC NUMBER: 2 is the length of stock_list
    if window_finish > end_of_stock_df / 2:
        break
        # print('at the end of the database data')
        # put the end of the window at the end of the stock data frame
        window_finish = int(end_of_stock_df / 2)
        window_start = window_finish - trading_days_window
        quit_loop = True
#
print(running_return)

# debug output
# print('entire_future_df:', entire_future_df)
print('Actual % return:')
s = entire_future_df[(entire_future_df['ticker'] == 'SPY')]
s_first = s.iloc[0]['close']
s_first_date = s.iloc[0]['date']
s_last = s.iloc[-1]['close']
s_last_date = s.iloc[-1]['date']
print('spy', (s_last - s_first) / s_first)

t = entire_future_df[(entire_future_df['ticker'] == 'TLT')]
t_first = t.iloc[0]['close']
t_last = t.iloc[-1]['close']
print('tlt', (t_last - t_first) / t_first)
