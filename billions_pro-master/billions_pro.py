
from tushare.stock.indictor import kdj
import tushare as ts
from thsAppAdder.unname import ths_api
from util.tools import *
from strategy.stghunter import *
from masquerade.rader import *
from conn.invate import *
from logger import logman
import time,conn
import traceback
# from good_emp import good_emp
from functools import partial


class billions_pro:
    def __init__(self):
        """:param self.result {'function_name':"ts_code"}"""

        self.result = {}
        self.isload_cache = 0
        self.cur_timestp = datetime.datetime.now().strftime("%Y-%m-%d")
        self.cache_all_df = {}
        self.loger = logman.logger("billions")
        self.ttl_vol = load_ttl_vol().rename(columns = {"sk_name":"ts_code"})

    def billians_work(self,model = "quick",timestampt= "",res2xlsx = False,**kwargs):
        # self.cur_timestp = datetime.datetime.now().strftime("%Y-%m-%d")
        if timestampt:
            self.cur_timestp = timestampt

        l = time.time()
        for k, i in enumerate(get_all_skname()):
            try:
                self.steam_line(i,model= model,**kwargs)
                processbar("\r正在扫描",k,3600) if k % 100 == 0 else None
            except Exception as e:

                # get_df_data = self.get_date_model(model=model)
                # df = get_df_data(i, self.cur_timestp)
                print(k,i)
                print(traceback.print_exc())


        if res2xlsx:
            self.res2df()
        print("用时",format(time.time() - l,'.2f' ))


    def previous_day(self):
        df = get_ts_data("600012.SH")
        return df.index[-2].strftime("%Y-%m-%d")


    def combine_res(self,tuple_res):
        """apply in func <billions_work>,combine the each stock result
        :param tuple_res :list of tuple eg ("600012.SH",'reds_no_raise') or (None,'reds_no_raise')
        :return :dic eg{'300585.SZ': ['break_m3_strategy', 'tail_break_ma']}"""
        for tuple_one in tuple_res:
            if tuple_one[0]:
                key = tuple(tuple_one)[0] ; val = tuple(tuple_one)[1]
                self.result.setdefault(key,[])
                self.result[key] = self.result[key] + [val]

        return self.result

    def show_res(self,model = "no"):
        """打印结果"""
        print(self.result.keys())
        for i,k in zip(self.result.keys(),self.result.values()):
            print(i,k,check_score(self.get_date_model(model=model)(i),self.cur_timestp,10)[0])
        self.result = {}

    def synchro_ths(self,p_name,which="all",**kwargs):
        """update to your ths app to self stock"""

        li = self.result.keys()

        def sort_asc_func(x):
            if "reds_no_raise" in self.result[x]:
                return 10
            if "break_ma" in self.result[x]:
                return 9
            if "tail_break_ma" in self.result[x]:
                return 8
            if "kdj" in self.result[x]:
                return 5
            else:
                return 1

        """做一个排序输出优先 reds_no_raise"""
        li = sorted(li,key= sort_asc_func)

        if which != "all":
            li = []
            for i,j in self.result.keys(),self.result.values():
                if which in j:
                    li.append(i)

        ths = ths_api()
        ths.synchro_ths(p_name,li,**kwargs)

    def de_bug(self,i,model = "quick",timestamp = ""):
        if timestamp:
            self.cur_timestp = timestamp
        self.steam_line(i,model = model)

        print(self.result)

    def get_date_model(self,model):
        """prepared : pro.daily 很多天的数据 ,计算所有股票指标信息耗时
          quick : pro.daily 很多天的数据 ,只计算一个指标信息
        else : pro.get_k_data 只获取单个股票"""

        if model == "prepared":
            get_sk_data = days_allprep_data
        elif model == "one":
            get_sk_data = days_oneprep_data
        else:
            get_sk_data = get_ts_data
        return get_sk_data


    def res2df(self, model = "prepared"):
        """ write excel to save result and score """
        if self.result == {}:
            print("the day ", self.cur_timestp," have no result" )
            return

        get_df_data = self.get_date_model(model=model)

        columns = []
        for i in self.result.values():
            columns.extend(i)
        columns = set(columns)

        index = []
        for i in self.result.keys():
            index.append(i)

        df = pd.DataFrame(index = index,columns =columns,data=np.nan)
        df['score'] = 0

        df = df.copy()
        for sk,columns_list in zip(self.result.keys(),self.result.values()):
            for column in columns_list:
                df.loc[sk,column] = 1
                score = check_score(get_df_data(sk),self.cur_timestp,10)
                df.loc[sk,"bestscore"] = score[-1]
                df.loc[sk,"10daysscore"] = score[0]


        df.to_excel("check_report\{}.xlsx".format(self.cur_timestp))

        # 结果导出excel表
        score_df = pd.DataFrame(
            [[i,len(df[df[i] == 1]) ,df[df[i] == 1]['bestscore'].mean()] if "score" not in i else [np.nan, np.nan] for i in df.columns],
            columns=['indicator',"amount", 'score']).dropna().sort_values(by=['amount'], ascending=True).sort_values(by=['score'], ascending=False)
        # score_df = score[score]

        # cols = [i for i in df.columns if "score" not in i]
        # two_indictor_df = df[df[cols].sum(axis=1)>=2]

        print(self.cur_timestp,score_df)

        return df
    #
    # def good_emp(self):
    #     return good_emp(self)


    def lots_timestp_check(self,timestamp = "",days = 100,model = "prepared",**kwargs):
        """check recent days of score """
        timestp_list = get_trade_days(days,timestamp)
        timestp_list.reverse()
        timestp_list = [timestp_list[-1]] + timestp_list
        for timestp in timestp_list:
            self.cur_timestp = "-".join([timestp[0:4],timestp[4:6],timestp[6:8]])
            print(self.cur_timestp)
            self.billians_work(model=model,res2xlsx=True,**kwargs)
            self.result = {}


    def add_tor(self,df):
        if df.empty:
            return df

        index = df.index
        df = pd.merge(df,self.ttl_vol, how='left',on ="ts_code")
        df.index = index
        df['volume_ratio'] = df['vol'] / df['ttl_vol']*100
        df.drop(labels='ttl_vol',axis=1)
        return df

    def steam_line(self,ts_code, model = "quick",**kwargs):
        # print(ts_code)
        get_df_data = self.get_date_model(model=model)
        df = get_df_data(ts_code, self.cur_timestp)
        # print(df)
        if df.empty:
            return
        # df = pd.merge(df,self.ttl_vol,on=['ts_code'],how='left')
        # df = pd.DataFrame()
        df = self.add_tor(df)
        self.df = df
        if today_price(df) > 30 :
            return None

        # if today_price(df) >30 or today_amount(df) < 3000 or today_volratio(df) <1:
        #
        # print(df)


        class a:
                pass
        class strategy_setter:
            #
            # kongzhongjiayou = kongzhongjiayou_strategy(df)
            # gre_leg = masquerade(df,[
            #                          crossing_star(bodylen=0.5,leglen=2,color="gre",increase_per=(-10,-0.5)),
            #
            #
            # ])
            # ma3_chase_ma2 = ma3_chase_ma2(df,self.cur_timestp)
            # #
            # quick_up_down_greleg = quick_up_down_greleg_strategy(df)
            # quick_up_down = quick_up_down_strategy(df)
            # tail_break_ma = masquerade(df,[ *[four_line_follow()]*3
            #                 ,tail_break_ma(on="ma_m3")
            #                     ])
            #
            # two_upper_shadow = masquerade(df,[
            #                 crossing_star(bodylen=0.2, leglen=2, color="red", increase_per=(-3, 3),shape="up"),
            #                 crossing_star(bodylen=0.2, leglen=2, color="red", increase_per=(-3, 3),shape="up"),
            #                     ])
            # break_m3 = break_m3_strategy(df, self.cur_timestp)
            # macd = macd_strategy(df,self.cur_timestp)
            #
            # kdj = kdj_strategy(df,self.cur_timestp)
            # money_flow = net_moneyflow(df,self.cur_timestp)
            # three_lines_up = three_line_up_strategy(df,self.cur_timestp)
            # macd_zerozeroone = macd_zerozeroone_strategy(df,self.cur_timestp)
            # #
            # long_header = masquerade(df,[four_line_follow(),
            #                              *[common()]*3,
            #                              (long_head(),closes())
            #                         ])
            reds_no_raise = reds_no_raise_stg(df, self.cur_timestp)


        self.combine_res([(strategy_setter.__dict__[i],i) if i  not in a.__dict__  else (None,"") for i in strategy_setter.__dict__])


if __name__ == '__main__':
    bp = billions_pro()
    # df= bp.get_date_model("")("600012.SH")
    # print(df.columns)
    # bp.de_bug("000713.SZ")
    bp.de_bug("002548.SZ")
    # bp.de_bug("300575.SZ")
    # bp.de_bug("002644.SZ",timestamp="2020-04-17")
    # bp.billians_work(model="prepared",res2xlsx=True)
    # bp.billians_work(model="prepared",res2xlsx=True,timestampt="2020-04-20")
    # bp.lots_timestp_check(days=3)

    """
    第一条最重要的一条，缩量
    第二条 耐心
    """


        # print(i)
    bp.lots_timestp_check(days=20)
    # def getLastWeekDay():
    #     datetime.datetime.now() - datetime.timedelta(days=1)
    #     return lastWorkDay




    # bp.cur_timestp = "2019-07-17"

    # bp.cur_timestp = "2020-03-27"
    # bp.billians_work(model="prepared",res2xlsx=True)
    # bp.cur_timestp = "2020-03-27"

    # bp.synchro_ths("lcl")
    # print()
    # bp.de_bug("601872.SH")
    # bp.de_bug("600012.SH")
    # bp.cur_timestp = "2020-01-07"
    # bp.de_bug("300634.SZ")

    # bp.billians_work(model="prepared",res2xlsx=True)

    # bp.billians_work(model="prepared",timestampt=bp.previous_day(),res2xlsx=True)

    # bp.cur_timestp
    # bp.de_bug("600517.SH",timestamp="2019-03-14")
    # bp.good_emp()
    # bp.de_bug("300585.SZ",timestamp="2019-07-22")
    # bp.de_bug("300346.SZ",timestamp="2020-01-13")
    # bp.synchro_ths("lcl")
    # bp.billians_work(model="prepared",timestampt=bp.previous_day(),res2xlsx=True)
    # bp.billians_work(model="prepared",res2xlsx=True)
    # bp.lots_timestp_check(model="prepared",days=100)


    # bp.billians_work()

    # bp.synchro_ths("lcl")

    # bp.good_emp()

    # bp.good_emp()

