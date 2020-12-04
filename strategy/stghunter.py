from util.tools import *
import numpy as np
import sys
from masquerade.rader import *


def _satisfiy_callback(df,funcs):
    res = []

    for func in funcs:
        res.append(func(df))

    return all(res)


def two_times_deal(df,timestamp):

    """成交量是前平均20天的2倍以上"""
    df = select_df_by_timestamp(df,timestamp,20)

    if not check_helper(df):
        return False

    now_amount = df.iloc[-1]['deal_amount']
    deal_amount_ave = df['deal_amount'].mean()
    deal_amount_max = df['deal_amount'].max()

    if 4 > now_amount/deal_amount_ave > 2 and 0.95<now_amount/deal_amount_max:
        return df.index.name
    return None



def decrease_deal(df,timestamp):

    """成交量是前平均20天的2倍以上"""
    df= select_df_by_timestamp(df,timestamp,10)

    if not check_helper(df):
        return False

    now_amount = df.iloc[-1]['deal_amount']
    deal_amount_ave = df['deal_amount'].mean()
    deal_amount_max = df['deal_amount'].max()
    deal_amount_min = df['deal_amount'].min()

    li = df['deal_amount']
    scd_amount = li[-1]
    if len(li)> 1:
        scd_amount = li[-2]

    if deal_amount_min == scd_amount and now_amount < deal_amount_ave/1.4:
        return df.index.name
    return None

def break_m3_stg(df,timestamp):
    """触底20日线 不超过2天 现在涨幅没超过5%"""

    if df.empty:
        return None

    break_ma = masquerade(df, [
                                (breakdown_ma(on=('ma_m3')), common(increase_per=(-8, -3.5)))
                               ])
    macd_today_restrict = df['macd'].tolist()[-1] > -0.08

    # 前面连续8日收盘价 不能高于m3超过12个点
    one = list(map(lambda x,y:0.02<(x-y)/y<0.15,df['close'][-8:-1],df['ma_m3'][-8:-1]))

    if one and break_ma and macd_today_restrict:
        return df['ts_code'][0]
    return None

def _kongzhongjianyou(df,days = 4):
    """days 表示几天前开始有一个 跳空高开
    cul_chg_steady , 表示跳空高开后，累积涨跌幅没有超过5个点，
    ma_m1_steady 表示高开后，m1一直很稳定
    """

    end_mean = df['ma_m1'][-(days):].mean()
    cul_chg_steady = all(map(lambda x:abs(x) < 5 ,df['pct_chg'][-days+1:].cumsum()))
    ma_m1_steady = all(map(lambda x:0.98<x/end_mean<1.02,df['ma_m1'][-days+1:]))

    res = masquerade(df, [*[common(increase_per=(-3, 3))] * 2,
                    common(jump=(1.5, 11), red=True,increase_per=(3,11)),
                    *[common(increase_per=(-4, 4))]*(days-2)
                    ])

    # print(days,cul_chg_steady,ma_m1_steady,res)
    return res and ma_m1_steady and cul_chg_steady

def kongzhongjiayou_stg(df):

    if df.empty:
        return None

    one = any([_kongzhongjianyou(df,days=i) for i in range(4,7)])
    if one and df['ma_m2'][-1]> df['ma_m3'][-1]:
        return df['ts_code'][0]
    return None


def weekly_steady_stg(df):
    week_df = PeriodDfConver.get(df,'W')


    # 有8周大于20日线
    days = 10
    one = sum(map(lambda close,m3: close>=m3,week_df['close'][-days:],week_df['ma_m3'][-days:])) >=8

    two = sum(map(lambda m1,m3: m1>=m3,week_df['ma_m1'][-days:],week_df['ma_m3'][-days:])) >=8

    three = sum(map(lambda close,m3: close/m3 < 1.1 ,week_df['close'][-days:],week_df['ma_m3'][-days:])) >=8

    four  = is_ma_steady(week_df,line="ma_m3")

    five = week_df[-1]['ma_m1'] > week_df[-1]['ma_m2'] and week_df[-1]['ma_m3'] > week_df[-1]['ma_m4']

    if one and two and three and four and five:
        return df['ts_code'][0]

def last_clean_stg(df):
    if df.empty:
        return None
    # one = tail_is_increasing(df['volume_ratio'][-3:-1], ascending=True)
    two = masquerade(df, [
        crossing_star(increase_per=(-3, 3), leglen=0.2),
        crossing_star(increase_per=(-2, 2), leglen=6),
        (crossing_star(increase_per=(2, 6), leglen=0.8, bodylen=1, shape="down"))
    ])
    three = three_lines_up(df)
    if two and three:
        return df['ts_code'][0]

def candle_break_m1_stg(df):

    if df.empty:
        return None
    one = masquerade(df, [
        compare_ma("high", "ma_m1", "gt"),
        compare_ma("high", "ma_m1", "gt"),
        compare_ma("high", "ma_m1", "gt"),
        [compare_ma("open","ma_m1","lt"),compare_ma("close","ma_m1","gt"),closes("open","ma_m1"),compare_ma("ma_m1","ma_m2","gt"),crossing_star(increase_per=(-5,4),bodylen=1,leglen=0)]
                                    ])
    if one:
        return df['ts_code'][0]


def quick_up_down_greleg_stg(df):

    # if "603998" in df['ts_code'][0]:
        # print(df['vol'])

    if df.empty:
        return None
    one = masquerade(df, [ common(increase_per=(-9, 8)),
                        common(increase_per=(9.7, 11)),
                            common(increase_per=(-10, -2),red=False),
                          (common(increase_per=(-10, -3),jump=(-10,-1)),crossing_star(leglen=3,color="gre"))
                                    ])

    if len(df)>8:
        two = df['vol'][-7:].max() < df['vol'].to_list()[-3]*1.2
    else:
        two = True

    if one and two :
        return df['ts_code'][0]

def quick_up_down_stg(df):

    if df.empty:
        return None
    one = masquerade(df, [ common(increase_per=(-9, 8)),
                        common(increase_per=(9.7, 11)),
                           common(increase_per=(-10, -2), red=False),
                          (common(increase_per=(-10, -3),jump=(-10,-1)),crossing_star(leglen=1,color="gre"))
                                    ])
    # print(df)
    if len(df)>8:
        two = df['vol'][-7:].max() < df['vol'].to_list()[-3]*1.2
    else:
        two = True

    if one and two :
        return df['ts_code'][0]



def two_head_air_clean_stg(df,days = 6):
    # print(df[-20:])
    one = masquerade(df[-20:],[
                    [crossing_star(increase_per=(-1,10),shape="up",color='red'),field_pct_chg('high',percent=1)],
                    [crossing_star(increase_per=(-1, 10), shape="up", color='red'),field_pct_chg('high',percent=2.5) ]
    ])

    five = masquerade(df[-20:],[
                    [crossing_star(increase_per=(-1,10),shape="up",color='red') ,field_pct_chg('high',percent=2.5)],
                    [crossing_star(increase_per=(-1, 10), shape="up", color='red') ,field_pct_chg('high',percent=1)]
    ])
    two = 20>sum(df['pct_chg'][-days:]) > 6.5
    three = all(map(lambda x: x<9.8,df['pct_chg'][-days:]))
    today_price = df['close'][-1]
    four = today_price<20
    # print(one,two,three,four)
    if (one or five) and two and three and four :
        return df['ts_code'][0]

def one_head_air_clean_stg(df,days = 5):
    # print(df[-20:])
    one = masquerade(df[-20:],[

                    [crossing_star(increase_per=(-1, 10), shape="up", color='red'),field_pct_chg('high',percent=2) ]
    ])


    two = 20>sum(df['pct_chg'][-days:]) > 6.5
    three = all(map(lambda x: x<9.8,df['pct_chg'][-days:]))
    today_price = df['close'][-1]
    four = today_price < 20
    # print(one,two,three,four)
    if one  and two and three and four :
        return df['ts_code'][0]



def continuous_red_stg(df,days = 10):


    one = sum(map(lambda x,y:x>=y,df['close'][-days:],df['open'][-days:]))>=days-4
    two = 20>sum(df['pct_chg'][-days:]) > 6.5
    three = all(map(lambda x: x<9.8,df['pct_chg'][-days:]))
    today_price = df['close'][-1]
    four = today_price<20
    if one and two and three and four:
        return df['ts_code'][0]

def continuous_red_weekly_stg(df,days = 6):
    ts_code = df['ts_code'][0]
    df = df.resample("W")
    df = df.agg({"pct_chg": lambda x: round(((x/100 + 1.0).prod() - 1.0),4)*100,
           "open": lambda x: x[x.first_valid_index()] if x.first_valid_index() else np.nan,
           "close": lambda x: x[x.last_valid_index()] if x.last_valid_index() else np.nan,
           "high": lambda x: x.max(),
           'low': lambda x: x.min()
           })
    df['ts_code'] = ts_code
    # print(df)
    one = sum(map(lambda x,y:x>=y,df['close'][-days:],df['open'][-days:]))>=days-1
    two = 25>sum(df['pct_chg'][-days:]) > 9
    three = all(map(lambda x: x<20,df['pct_chg'][-days:]))
    today_price = df['close'][-1]
    four = today_price<20
    # print(one,two,three,four)
    if one and two and three and four:
        return df['ts_code'][0]

def m3_steady_break_down_stg(df,days = 8):



    ma_m3 = df["ma_m3"][-days:]
    close = df["close"][-days:]
    one = ma_m3.std() < 0.05
    nums_close_gt_mam3 = sum(map(lambda x,y:x<y ,ma_m3,close))
    # print(ma_m3.std(),df['ts_code'][0])
    three = nums_close_gt_mam3 >= days-3
    two = df['low'][-1] < df['ma_m3'][-1]
    if one and three and two:
        return df['ts_code'][0]

def macd_zerozeroone_stg(df,timestamp,days = 6):
    "macd 一直是绿色0连续，然后"
    df1 = select_df_by_timestamp(df.copy(),timestamp,days)

    df2 = select_df_by_timestamp(df.copy(), timestamp, 16)

    macd_zore = sum(map(lambda x:-0.01<=x<=0,df1['macd']))> 3

    two = any(map(lambda x:-0.1>x  or x>0.1,df2['macd']))

    three = have_green_of_number(df1)>3
    if macd_zore and two and three:
        return df['ts_code'][0]
    return None

def continual_gre_up_stg(df,timestamp,days = 4):
    df = select_df_by_timestamp(df,timestamp,days)
    if df.empty:
        return None

    one = masquerade(df, [ breakdown_ma("ma_m3"),
                                common(increase_per=(4,10),red=False) ,
                                common(increase_per=(-1, 10), red=False),
                               ])
    if one:
        return df['ts_code'][0]
    return None


def three_line_up_stg(df,timestamp,days = 8):
    df = select_df_by_timestamp(df,timestamp,days)
    one = three_lines_up(df)
    # two = all(map(lambda x:x>0,df['macd'][-8:-1]))

    # 给他1%的靠近五日线就行
    three =  df['low'][-1]/1.01 <= df['ma_m1'][-1] <= df['high'][-1]

    if one and three:
        return df['ts_code'][0]
    return None

def reds_no_raise_stg(df,timestamp,days = 6):

    df_old = df.copy()
    df = select_df_by_timestamp(df,timestamp,days)

    if df.empty:
        return None

    end_all_gt_m2 = all(map(lambda x,y:x>=y ,df['close'],df['ma_m2']))
    length_reds = have_red_of_number(df)
    length_day = len(df)
    jump_open_nums = have_lot_of_jump(df)
    fake_reds = have_fake_reds(df)
    big_fake_reds = have_big_fake_red(df)
    no_big_increase = have_no_big_increase(df)

    flag_is_m1_gt_last_week =  is_m1_gt_last_week(df_old,timestamp)
    flag_is_first_top = is_first_top(df_old,timestamp)
    flag_is_m1_steady = is_m1_steady(df)

    # print(flag_is_m1_gt_last_week,flag_is_first_top,flag_is_m1_steady,fake_reds)
    # print( end_all_gt_m2 , jump_open_nums >=2 , fake_reds>=2 , no_big_increase  , big_fake_reds>=1,flag_is_m1_gt_last_week , flag_is_first_top)
    if length_reds >= length_day-1 and 0.95< df.iloc[-1]['close']/df['close'].max()<1.03 and end_all_gt_m2 and jump_open_nums >=2 \
            and fake_reds>=2 and no_big_increase  and big_fake_reds>=1 and flag_is_m1_gt_last_week and flag_is_first_top \
            and flag_is_m1_steady:

        return df.ts_code[0]
    return None

def fake_2reds_stg(df,timestamp):
    if df.empty:
        return None

    fake_2reds = masquerade(df, [common(red=True),
                                 common(increase_per=(-10, -0.1), red=True),
                                 common(increase_per=(-10, -0.1), red=True),
                                 ])

    close_small_gt_m3 = map(lambda x,y:0.02<(x-y)/y<0.12,df['close'][:8],df['ma_m3'][:8])

    if fake_2reds and close_small_gt_m3:
        return df.ts_code[0]
    return None


def macd_120_stg(df ,timestamp,days = 4,default = "macd_120"):

    df = select_df_by_timestamp(df,timestamp,days)
    macd = df[default]

    one = masquerade(df,[common(red=True)
                         ])

    macd_120 = df['macd']

    if len(macd)>2 and tail_is_increasing(macd,tail_length= 3 ) and -0.1< macd[-1] < 0.5  and tail_is_increasing(macd_120,tail_length= 3 ) and one:
        return df.ts_code[0]
    return None

def jump_open_stg(df,timestamp):
    df = select_df_by_timestamp(df,timestamp,days= 11)


    jump_limit = 9
    id_loc = -1

    df['id'] = range(len(df))
    # 如果没有高开大于9% 直接返回
    for order,i,k in zip(df['id'],df['jump_open'].tolist(),df['pct_chg'].tolist()):
        if i > jump_limit and k > jump_limit:
            id_loc = order

    if id_loc == -1:
        return None

    # print(id_loc)
    # 找到一字版的 前后 几天的涨跌幅
    front_behind_pct_chg = df['pct_chg'].tolist()[id_loc-1:id_loc] + df['pct_chg'].tolist()[id_loc+1:id_loc+3]

    #前后涨跌幅 都没有涨超过5% 就ok
    if all( [i<5 for i in front_behind_pct_chg]):
        return df.ts_code[0]
    return None



def net_moneyflow(df,timestamp):
    if df.empty:
        return None
    one = masquerade(df, [(net_moneyflow_r(0.6), common(red=False))
                                 ])

    two = df['vol'][-1] > df['vol'][:10].mean()*1.5

    if one and two:
        return df.ts_code[0]


def down_to_m3(df,timestamp,days = 6):

    df =select_df_by_timestamp(df,timestamp,days)

    if df.empty:
        return None

    """undone"""


def macd_stg(df ,timestamp,days = 10,default = "macd"):

    df = select_df_by_timestamp(df,timestamp,days)
    macd = df[default]
    # few_days_ago_four_line_follow = masquerade(df,
    #                                            [*[four_line_follow()]*2,
    #                                             *[common()]*5
    #                                            ])

    # 大于20日线
    four = sum(map(lambda x:x<0,macd)) <7
    one  =  df['close'][-1]> df['ma_m3'][-1] and df['ma_m1'][-1]< df['ma_m2'][-1]

    if len(macd)>2 and tail_is_increasing(macd,tail_length= 3 ) and -0.5< macd[-1] < -0.01 and one and four:
        return df.ts_code[0]
    return None

def long_head_stg(df,timestamp,days = 12):
    df = select_df_by_timestamp(df,timestamp,days)
    if df.empty:
        return None
    one_shoot = masquerade(df, [(long_head(), compare_ma(), compare_ma("close", 'ma_m1', "lt"))
                                ])

    one = all(map(lambda x,y:0.02<(x-y)/y<0.12,df['close'][:8],df['ma_m3'][:8]))



    if one_shoot and one:

        return df.ts_code[0]




def kdj_stg(df,timestamp,days= 12):
    df = select_df_by_timestamp(df,timestamp,days)
    if df.empty:
        return None

    kdj_j = df['kdj_j'].tolist()
    if any(map(lambda x:x<=0.5,kdj_j)):
        return None


    kdj_j_recent = kdj_j[-2:]
    kdj_j_recent_lt_10 = any(map(lambda x:x<10,kdj_j_recent))
    kdj_j_touch_100 = any(map(lambda x:x>=100,kdj_j))
    kdj_j_5d_mean_gt_20 = np.mean(kdj_j[-5:]) > 30

    limited_down = all(map(lambda x:x>-6, df['pct_chg']))

    today_gt_m3 = df['close'][-1] > df['ma_m3'][-1]

    if limited_down and kdj_j_recent_lt_10 and kdj_j_5d_mean_gt_20 and kdj_j_touch_100 and today_gt_m3:
        return df['ts_code'][0]
    return None

