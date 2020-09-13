import sys
import datetime
import pandas as pd

class deco:
    @staticmethod
    def low_memory_cache(func):
        def wraper(*args,**kwargs):
            self = args[0]
            tb_name = args[1]
            if tb_name in self.cache.keys():
                return self.cache[tb_name]

            df = func(*args,**kwargs)
            self.cache = {}
            self.cache[tb_name] = df
            return df
        return wraper

    @staticmethod
    def is_table_existed(func):
        def wraper(*args,**kwds):
            self = args[0]
            tb_name = args[1].lower()
            if tb_name not in self.show_tables():
                self.create_table(tb_name)
            func(*args,**kwds)
        return wraper

    @staticmethod
    def is_row_existed(func):
        def wraper(*args,**kwds):
            self = args[0]
            tb_name = args[1].lower()
            if tb_name not in self.show_tables():
                self.create_table(tb_name)
            func(*args,**kwds)
        return wraper


def select_df_by_timestamp(df,timestamp,days = 6,how = "past",today = "include"):
    """days 是股票开市的工作日"""
    if how == "past":
        df = df[:timestamp]
        if today != "include":
            days += 1
            df = df[-days:]
            df = df[:-1]
        if today == "include":
            df = df[-days:]

    elif how == "future":
        df = df[timestamp:]
        df = df[:days]
    else:
        raise IOError("arg 'how' is error")
    return df

def days_range(tmstamp,day_lenght,how = "past"):
    """返回 一段时间内的 日期区间"""
    last_tmstamp = tmstamp
    tmstamp_d = datetime.datetime.strptime(tmstamp,"%Y-%m-%d")

    if how == "past":
        tmstamp_d = tmstamp_d - datetime.timedelta(days=day_lenght)
    elif how =="future":
        tmstamp_d = tmstamp_d + datetime.timedelta(days=day_lenght)
    else:
        raise KeyboardInterrupt("only past or future needed")

    tmstamp = tmstamp_d.strftime("%Y-%m-%d")

    """time index must in order"""
    if last_tmstamp < tmstamp:
        temp = last_tmstamp
        last_tmstamp = tmstamp
        tmstamp = temp

    return tmstamp,last_tmstamp

def check_score(df,timestamp,days,how = "future"):

    """算区间涨幅"""
    df = df.sort_values("date")
    df = select_df_by_timestamp(df,timestamp,days=days,how=how)

    max_end = df['close'].max()

    if df.empty:
        return 0,0
    top_profit = round((max_end - df.iloc[0]['close']) / df.iloc[0]['close'] * 100, 2)
    deadline_profit = round((df.iloc[-1]['close'] - df.iloc[0]['close']) / df.iloc[0]['close'] * 100, 2)
    return deadline_profit,top_profit




def check_bug_series(values):
    """pandas 的一个 bug"""
    if isinstance(values,pd.Series):
        return values.values[0]
    return values

def check_helper(df):
    if df.__len__() == 0:
        return False
    return True

def no_rebound(self):
    pass

def stand_m3_for_days( df, timestamp):
    """ 连续20天 ，站上20日均线"""
    df = select_df_by_timestamp(df,timestamp,25)
    m3 = df['M3']
    end = df['close']
    h = map(lambda x, y: 1 if y > x else 0, m3, end)
    return sum(h) > 10

def day_kdj_100(df, how="before"):
    """kdj 大于100天的前一天日期"""
    if how == "after":
        df = df.sort_values(by=['date_c'], ascending=False)
    elif how == "before":
        pass
    else:
        raise KeyboardInterrupt(" bofore or after expected but {} get".format(how))

    stamp = df.iloc[0]['date_c']
    for j, d in zip(df['kdj_j'], df['date_c']):
        if j <= 100:
            stamp = str(d)
        else:
            return str(stamp)
    return str(stamp)


def have_no_big_increase(df):
    return all(map(lambda x: x<8 ,df['pct_chg'].tolist()))

def is_m1_gt_last_week(df,timestamp,days=6,percent = 1.05):
    """五日线 这个礼拜 强于上个礼拜"""

    df_1 = select_df_by_timestamp(df,timestamp,days = days)
    if df_1.empty:
        return False


    df_2 = select_df_by_timestamp(df,df_1.iloc[0]['date'],days = days,today="no include")
    if df_2.empty:
        return False

    fut_m1_ave = df_1['ma_m1'].mean()
    last_m1_ave = df_2['ma_m1'].mean()

    return fut_m1_ave/last_m1_ave >= percent

def have_lot_of_jump(df):
    """高开一个点 和低开一个点 的数量"""
    df = df.copy()

    df = tomorrow_col_adder(df)
    end = df['close'][:-1]
    open_tomorrow = df['open_tomorrow'][:-1]

    # print(list(map(lambda today,tomo:1 if today/tomo >= 1.01 else 0,end,open_tomorrow)))
    high_jump_nums = sum (map(lambda today,tomo:1 if tomo/today >= 1.02 else 0,end,open_tomorrow) )
    low_jump_nums  = sum (map(lambda today,tomo:1 if tomo/today <= 0.98 else 0,end,open_tomorrow) )

    return high_jump_nums+low_jump_nums

def have_fake_reds(df):
    """ have"""

    df = df.copy()
    fake_reds_nums = sum(map(lambda end,open,increse:1 if increse <0.2 and open < end else 0,df['close'],df['open'],df['pct_chg']))
    return fake_reds_nums

def have_red_of_number(df):
    length_reds = sum(map(lambda x,y:1 if y>= x else 0,df['open'],df['close']))
    return length_reds

def have_green_of_number(df):
    length_reds = sum(map(lambda x,y:1 if y<= x else 0,df['open'],df['close']))
    return length_reds

def have_big_fake_red(df):
    """ 跌的阳线 的个数 """

    df = df.copy()

    fake_reds_nums = sum(map(lambda end,open,increse:1 if increse < -1 and open < end else 0,df['close'],df['open'],df['pct_chg']))
    return fake_reds_nums



def is_first_top(df,timestamp,days= 7):
    """ 阶段第一天 是前期高点"""

    df_1 = select_df_by_timestamp(df,timestamp,days = days)
    if df_1.empty:
        return False

    df_2 = select_df_by_timestamp(df,df_1.iloc[0]['date'],days = days,today="no include")
    if df_2.empty:
        return False
    first_day_max = df_1.iloc[0]['high']


    return first_day_max / df_2['high'].max() > 0.95

def is_m1_steady(df):
    df = df.copy()
    end_ave = df['close'].mean()
    df['end_ave'] = end_ave

    return all(map(lambda x,y: abs(x-y)/y<0.06 ,df['close'],df['end_ave'])) and df['ma_m1'].std()< 0.35 and df.iloc[-1]['close']/end_ave < 1.06

def tail_is_increasing(li,tail_length = 3,ascending = False):
    """li 数组 持续增长"""
    li = li[- tail_length:]

    if len(li)<3:
        return True

    # print(li)

    li = li.to_list()
    if ascending ==True:
        li.reverse()
    li2 = li[1:]+ [li[-1]]

    return all(map(lambda x,y: y >= x, li, li2))

def tomorrow_col_adder(df):
    df['open_tomorrow'] = df['open'].shift(-1)
    df['end_tomorrow'] = df['close'].shift(-1)
    return df

def max_swing_adder(df):
    df = pd.DataFrame(df)
    df['interval'] = df.apply(lambda x:(x['low'],x['high']),axis=1)
    return list(df['interval'])

def increase_adder(df):
    """新增一个 涨幅的参数"""
    df['end_shift_1'] = df['close'].shift(1).fillna(1)
    df['increase'] = df.apply(lambda x:round((x['close']-x['end_shift_1'])/x['end_shift_1'],4)*100,axis =1)
    df.drop(['end_shift_1'],axis =1 )
    return df

def jump_open_adder(df):
    df['end_shift_1'] = df['close'].shift(1).fillna(1)
    df['jump_open'] = df.apply(lambda x:round((x['open']-x['end_shift_1'])/x['end_shift_1'],4)*100,axis =1)
    df = df.drop(['end_shift_1'],axis =1 )
    return df

def three_lines_up(df,days = 4):

    # one = tail_is_increasing(df['ma_m2'],tail_length=8)
    two = tail_is_increasing(df['ma_m3'],tail_length=3)
    three = tail_is_increasing(df['ma_m4'],tail_length=3)
    four = all(map(lambda x,y:0.2>(x-y)/x> 0.01, df['ma_m3'][-days:],df['ma_m4'][-days:]))
    # print(df['ma_m3'][:days],df['ma_m4'][:days])
    return two and three and four




def today_price(df):
    return df.iloc[-1]['close']


def today_amount(df):
    return df.iloc[-1]['amount']


def today_volratio(df):
    # print(df.columns)
    return df.iloc[-1]['volume_ratio']