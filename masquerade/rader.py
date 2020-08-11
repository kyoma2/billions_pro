
import pandas as pd




def __loader(self, ts_code, timestamp,days):
    df = self.select_table(ts_code)
    df = self.increase_adder(df)
    df = self.jump_open_adder(df)
    p, f = self.days_range(timestamp, days)
    df = df[p:f]
    return df

def __satisfier(row_index, func, df):
    row_of_df = df.iloc[row_index]
    # print(row_of_df)
    # print(row_of_df)
    return func(row_of_df)

def __two_are_close(indicator1,indicator2,tolerance = 1):
    return abs((indicator1/indicator2)- 1)*100 < tolerance






def masquerade(df,action_tup_li):
    # print(df)
    """ action_tup: [前天,昨天，今天]"""
    df_length = len(df)

    if df_length == 0 or len(action_tup_li) == 0 or len(action_tup_li) > df_length:
        return None

    df = df.iloc[-len(action_tup_li):].copy()


    res = []
    for k, action_tup in enumerate(action_tup_li):
        if not isinstance(action_tup,(tuple,list)):
            action_tup = [action_tup]
        # 对每一对 action_tup 中的 action 必须全部符合,这次模拟才为True
        res.append(all(__satisfier(k, action, df) for action in action_tup))

    if all(res):
        return df.ts_code[0]

    else:
        return None


def crossing_star(increase_per=(0, 0),color = "any",leglen = 3, bodylen = 0, shape = "any" ):
    def wraper(row):
        row = row.copy()
        if (color == "any" and row['close'] <= row['open']) or color == "gre":
            temp = row['close']; row.loc['close'] = row['open']; row.loc['open'] = temp

        one = 1 <= row['close'] / row['open'] < 1.06
        two = 1.001 <= row['high'] / row['close']
        three = 1.001 <= row['open'] / row['low']
        four = increase_per[0] <= row['pct_chg'] <= increase_per[1] if increase_per != (0, 0) else True

        body_size = row['close'] - row['open'] if row['close'] - row['open']> 0.001 else  row['open'] -row['close']+0.003
        up_size   = row['high'] - row['close']
        down_size = row['open'] - row['low']

        six = body_size/row['close']*100 > bodylen
        five = (up_size + down_size)/body_size >= leglen


        seven = False
        if shape == "any":
            seven = True
        elif shape == "up":
            seven = up_size > down_size*2.5
        elif shape == "down":
            seven = up_size*2.5 < down_size


        return one and two and three and five and four and six and seven
    return wraper

def breakdown_ma(on = ('ma_m2',"ma_m3")):
    if not isinstance(on,(list,tuple)):
        on = [on]

    def wraper(row):
        row = row.copy()
        max_end_open = max( row['close'],row['open'])
        one = any([ max_end_open >= row[ma] >= row['low'] for ma in on ])

        # 20日线  大于10日线的 94% 但是小于10日线 ,齐次 5日线的 要大于 10日线的 98%

        """ 10日线*0.92 <20日线 < 10日线*0.99  ； 5日线 > 10日线*0.99 ；20日线*0.92 < 30日线 < 20日线*0.99  """
        two = row['ma_m2']*0.92 <row['ma_m3'] < row['ma_m2']*0.99 and row['ma_m3']*0.92 < row['ma_m4'] < row['ma_m3']*0.99 and \
              row['ma_m1'] > row['ma_m2']*0.99 and row['ma_m2'] > row['ma_m3']


        # 20日线和收盘 很接近

        three = __two_are_close(row['close'],row['ma_m3'], tolerance= 3)
        res = one and two and three
        return res
    return wraper



def closes(indictor1= "low",indictor2 = "ma_m1",tolerance = 1):
    def wraper(row):
        row = row.copy()
        return __two_are_close(row[indictor1], row[indictor2], tolerance=tolerance)
    return wraper

def tail_break_ma(on = ('ma_m2',"ma_m3")):
    if not isinstance(on, (list, tuple)):
        on = [on]

    def wraper(row):
        row = row.copy()
        # max_end_open = max(row['close'], row['open'])
        # one = any([max_end_open >= row[ma] >= row['low'] for ma in on])
        tail_size = min(row['open'],row['close']) - row['low']
        body_size = max(row['open'],row['close']) -  min(row['open'],row['close'])

        # 尾巴大概要长于2个点,假设平开

        one = tail_size > body_size* 1.2 and tail_size > row['open'] * 0.1* 0.2

        # 尾巴击穿均线m2 或者 m3
        two = any([ min(row['open'], row['close']) >= row[ma] >= row['low']*0.99 for ma in on])

        res = one and two
        return res

    return wraper



def four_line_follow():

    def wraper(row):

        row = row.copy()
        if not row['ma_m1'] > row['ma_m2'] > row['ma_m3'] > row['ma_m4'] :
            return None


        two = row['ma_m2']*0.995 > row['ma_m3']
        three = row['ma_m3']*0.99 > row['ma_m4']
        res = two and three

        return res
    return wraper




def long_head(color = "red"):
    if color not in ['red','gre','green']:
        raise Exception("long_head param color must red gre green")

    def wraper(row):
        # tail_size = min(row['open'],row['close']) - row['low']
        body_size = max(row['open'],row['close']) -  min(row['open'],row['close'])

        head_size = row['high'] - max(row['open'],row['close'])
        red_or_gre = row['close'] < row['open'] if color !="red" else True

        one = head_size > row['open'] * 0.1 * 0.4
        two = body_size < row['open'] * 0.1 * 0.3
        return one and two and red_or_gre
    return wraper



def common(increase_per = (0,0),jump = (0,0),red = "any"):
    def wraper(row):
        row = row.copy()
        one = increase_per[0] <= row['pct_chg'] <= increase_per[1] if increase_per != (0,0) else True
        two = jump[0] <= row['jump_open'] <= jump[1] if jump != (0,0) else True
        three = (row['open'] < row['close']) == red  if  red != "any" else True
        # print(one,two,three)
        return one and two and three
    return wraper

def compare_ma(fld = "close",fld_s = "ma_m3", how = "gt"):
    def wraper(row):
        row = row.copy()
        dic = {"gt":row[fld] > row[fld_s],"lt":row[fld] < row[fld_s],"eq":row[fld] == row[fld_s]}
        return dic[how]
    return wraper


def net_moneyflow_r(percent = 0):
    def wraper(row):
        # print(row)
        if abs(row['income']-0) < 1:
            return False
        one = percent < row['net_income']/row['income'] if percent != 0 else True
        return one

    return wraper