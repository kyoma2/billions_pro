from joblib import Parallel, delayed
import tushare as ts
import logging
import os
from util.formula import KDJ,ma,MACD,MA
from util.tools import *
import datetime,time
from concurrent.futures import ThreadPoolExecutor


pro = ts.pro_api(token="09c4fa2840be2fd486a8a5ebea249e49273d4d9f9872fb5202f639cc")

recent_df_cache = {}
recent_df_updated = {}
get_ts_df_cache = {}
tday_timestp = datetime.datetime.now().strftime("%Y%m%d")


def get_df_three_times(func):
    """尝试拉取3次"""
    def wraper(*args,**kwargs):
        res = None
        for i in range(3):
            if i != 0:
                print(i)
            try:
                res = func(*args,**kwargs)
            except Exception as e :
                logging.info(e)
                time.sleep(1)
                pass
            if isinstance(res,pd.DataFrame) :
                break
        return res
    return wraper

# def check_df_then_update(df,field):
#     if recent_df_updated.get(field,0):
#         re



def get_trade_days(n,timestamp = ""):
    """得到最近连续几天的开市日期
    param n means how many days datetime str you get ,when big A is workday
    :return list of timestamp """
    today_timestp = tday_timestp
    if timestamp:
        today_timestp = timestamp
    #
    pro_bar = get_df_three_times(ts.pro_bar)

    sy1 = pro_bar(ts_code='{}'.format("601988.SH"), start_date='20180801', end_date=today_timestp, api=pro)['trade_date'][:n].tolist()
    sy2 = pro_bar(ts_code='{}'.format("601288.SH"), start_date='20180801', end_date=today_timestp, api=pro)['trade_date'][:n].tolist()
    sy1.extend(sy2)
    sy1 = list(set(sy1))
    sy1.sort()
    return sy1


def get_all_recent(days = 120,work_n= 15,money_flow = False,read_hdf = True, path = "no_local"):
    """
    :parameter days int  default 120days
    :parameter work_n number of threading_process
    :return dateframe all days and all stocks of basic info
    """

    """HDF做为缓存文件,每次启动前检查、更新HDF,date.h5 这是基础数据,检查方式根据最近的日期"""

    if not os.path.exists("..\hdf_storage"):
        os.makedirs("..\hdf_storage")
    if not os.path.isfile("..\hdf_storage\data.h5"):
        _get_all_recent(days_or_days_list= 2 ,money_flow = money_flow).to_hdf("..\hdf_storage\data.h5",key="df",mode = "w",format = "fixed")
    old_df = pd.DataFrame(pd.read_hdf("..\hdf_storage\data.h5"))
    if path == "local":
        old_df = pd.DataFrame(pd.read_hdf("..\..\hdf_storage\data.h5"))

    if read_hdf == False:
        old_df = _get_all_recent(days_or_days_list=days, work_n=work_n, money_flow=money_flow)
        return old_df

    li_timestamp = get_trade_days(days)
    days_not_in_old_df_li = []
    li_old_date = old_df['date'].unique()
    for i in li_timestamp:
        if i not in li_old_date:
            days_not_in_old_df_li.append(i)


    if days_not_in_old_df_li == []:
        return old_df




    new_df = _get_all_recent(days_or_days_list = days_not_in_old_df_li,work_n=work_n,money_flow = money_flow)

    # 因为基本信息和资金data 没有同步,所以资金信息没有更新的话，就不更新hdf.data.h5
    if new_df.empty:
        return old_df

    r_df = pd.concat([old_df, new_df], axis=0, sort=True)
    r_df = r_df.drop_duplicates(['date','ts_code'])
    r_df = r_df.fillna(0)
    # 当数据为空时 会有performance warning
    r_df.to_hdf("..\hdf_storage\data.h5",key="df",mode = "w",format = "fixed")

    # if not money_flow == True:
    #     r_df = r_df.drop(labels=['money_flow'],axis= 1)
    return r_df


def _get_all_recent(days_or_days_list,work_n= 15,money_flow =False):

    """下载几天中所有的股票信息
    :param days : int or timestamp_list  ;length of days to download
    :param work_n: n of multiprocess to download,if work_n is zero ,means dont use multiprocess
    :returns list dataframes of all stock in a times"""
    print("fetch recent full stock data need to reload days of", days_or_days_list)
    """得到最近几天的所有股票信息"""

    df = pd.DataFrame()
    # moneyflow_df = pd.DataFrame()
    pro_daily = get_df_three_times(pro.daily)

    if isinstance(days_or_days_list,(int,float)):
        day_list = get_trade_days(days_or_days_list)
    elif isinstance(days_or_days_list,(list,tuple)):
        day_list = days_or_days_list
    else:
        raise Exception("day_list input error")

    l = time.time()
    if work_n==0:
        dfs_to_concat  = []
        for timestamp in day_list:
            part = pro_daily(trade_date=timestamp)
            dfs_to_concat.append(part)
        df = pd.concat(dfs_to_concat,axis=0,sort=True)
    else:
        # 基本信息
        dfs = _threading_load_recent(pro_daily,work_n = work_n,trade_date = day_list)
        df = pd.concat(dfs,sort=True,axis=0)
        print("base time to fetch_date is ", format(time.time() - l,'.2f' ))
        # ##资金流入流出情况

        l = time.time()
        if money_flow == True:
            money_flow_dfs = _threading_load_recent(moneyflow_onedays_df,work_n=3,trade_date = day_list)
            moneyflow_df = pd.concat(money_flow_dfs,axis=0,sort=True)
            if moneyflow_df.empty:
                print("money_flow data of requests is failed, nothing changed about data.h5")
                return pd.DataFrame()
            print("moneyflow time to fetch_date is ",  format(time.time() - l,'.2f' ))
            df = pd.merge(df,moneyflow_df,how='left',on=['ts_code','trade_date'])

    # print(111)
    df = __set_datetime_index(df)
    return df


def _threading_load_recent(func,work_n = 1,**kwargs):
    """
    :param work_n is number of threading_process
    :param kwargs {field = args} args is list
    eg. 1.func(filed = 1) 2.func(filed = 2) 3.func(filed = 3)
    in this case equal to _threading_load_recent(func,work_n = 2,filed = [1,2,3])
    :return list of all func returns
    """

    with ThreadPoolExecutor(work_n) as executor:
        # print(kwargs)
        id_list = list(range(len(list(kwargs.values())[0])))
        """id is order len of kwargs.values"""
        result = []
        for i in id_list:
            kw = {}
            for key in kwargs.keys():
                kw[key] = kwargs[key][i]

            """kw = {'a': 1, 'b': 3}
                 {'a': 2, 'b': 5}
                 {'a': 3, 'b': 6}"""

            result.append(executor.submit(func,**kw))
        result = [res.result() for res in result]
    return result


class processbar:
    "print processbar "
    def __new__(cls, *args, **kwargs):
        if not hasattr(processbar,"_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, print_str, i, total):
        self._bar_print_flag = 0
        end_symbol = ""
        if hasattr(self,'last_print_str')  and self.last_print_str != print_str:
            print('\n')
            self.last_print_str = print_str
            self._bar_print_flag = 1
            end_symbol = "\n"


        if i == total:
            end_symbol = "\n"
        if i <= total:
            print("\r" + " -v- " + print_str + ": " + str(round(i / total * 100, 2)) + "%  |  ", end=end_symbol)
        elif i-100 > total and self._bar_print_flag:
            print("实际进度条 偏快了")
            self._bar_print_flag = 0





def days_oneprep_data(ts_code,time_stamp ="",days = 121):

    """利用最近几天的所有股票数据然后合并,对每个股票都做处理后 才能返回 return get各个股票信息"""
    if time_stamp:
        days = (datetime.datetime.now() - datetime.datetime.strptime(time_stamp,"%Y-%m-%d")).days + days

    if not recent_df_cache.get(ts_code):
        # [recent_df_cache.setdefault(name,pd.DataFrame) for name in get_all_skname()]
        alldf = get_all_recent(days=days,money_flow=False)

        sk_df = alldf[alldf['ts_code'] == ts_code]
        sk_df = indictor_adder(sk_df)
        sk_df = jump_open_adder(sk_df)
        recent_df_cache[ts_code] = sk_df

    df = recent_df_cache[ts_code]
    if df.empty:
        return df
    if time_stamp:
        # pandas的时间索引不科学，不能逆序，所以要先按日期排序
        df = select_table_by_timestamp(df,time_stamp)
    return df



def _allprep_data_storage_backup(days = 120, update = 1):
    """从原有的data_prepared中读取，于新的date比较，如果日期一样就不用了更新参数了，否则要更新参数，然后保存"""

    if not os.path.exists("..\hdf_storage"):
        os.makedirs("..\hdf_storage")
    if not os.path.isfile("..\hdf_storage\data_prepared.h5"):
        _get_all_recent(days_or_days_list=1).to_hdf("..\hdf_storage\data_prepared.h5",key="df",mode = "w",format = "fixed")

    old_df = pd.DataFrame(pd.read_hdf("..\hdf_storage\data_prepared.h5"))

    old_date_li = old_df['date'].unique()
    new_date_li = get_all_recent(days)['date'].unique()
    update_flag = 0
    for i in new_date_li:
        if i not in old_date_li:
            update_flag = update



    if update_flag == 0 :
        r_df = old_df
    else:

        alldf = get_all_recent(days)
        alldf = alldf.sort_values(by="date")
        l = time.time()

        dfs = []
        for k, ts_code in enumerate(alldf['ts_code'].unique()):
            sk_df = alldf[alldf['ts_code'] == ts_code]


            sk_df = __mix_set(sk_df)
            # sk_df = indictor_adder(sk_df)
            # sk_df = jump_open_adder(sk_df)
            dfs.append(sk_df)

            if k % 100 == 0:
                processbar("indicator prepared", k, 3600)

        r_df = pd.concat(dfs,sort=True,axis=0)
        print("indicator finished time", time.time() - l)
        r_df.to_hdf("..\hdf_storage\data_prepared.h5",key="df",mode = "w",format = "fixed")
    return r_df

def _allprep_data_storage(days = 120, update = 1,parrallel = True):
    """从原有的data_prepared中读取，于新的date比较，如果日期一样就不用了更新参数了，否则要更新参数，然后保存"""

    if not os.path.exists("..\hdf_storage"):
        os.makedirs("..\hdf_storage")
    if not os.path.isfile("..\hdf_storage\data_prepared.h5"):
        _get_all_recent(days_or_days_list=1).to_hdf("..\hdf_storage\data_prepared.h5",key="df",mode = "w",format = "fixed")

    old_df = pd.DataFrame(pd.read_hdf("..\hdf_storage\data_prepared.h5"))

    old_date_li = old_df['date'].unique()
    new_date_li = get_all_recent(days)['date'].unique()
    update_flag = 0
    for i in new_date_li:
        if i not in old_date_li:
            update_flag = update



    if update_flag == 0 :
        r_df = old_df

    else:

        alldf = get_all_recent(days)
        alldf = alldf.sort_values(by="date")
        l = time.time()
        dfs = []

        if parrallel:
            dfs = _add_indicator_parrallel(alldf,worker_n=6)
        elif not parrallel:
            dfs = []
            for k, ts_code in enumerate(alldf['ts_code'].unique()):
                sk_df = alldf[alldf['ts_code'] == ts_code]

                sk_df = __mix_set(sk_df)
                # sk_df = indictor_adder(sk_df)
                # sk_df = jump_open_adder(sk_df)
                dfs.append(sk_df)

                if k % 100 == 0:
                    processbar("indicator prepared", k, 3600)


        r_df = pd.concat(dfs,sort=True,axis=0)
        print("并行后的股票数据",len(r_df['ts_code'].unique()))
        print("indicator  finished time", time.time() - l)
        r_df.to_hdf("..\hdf_storage\data_prepared.h5",key="df",mode = "w",format = "fixed")
    return r_df

def _add_indicator_parrallel(alldf,worker_n = 6):
    """对股票总表 添加新的指标进行计算 返回成 [df1,df2] 单个股票的列表"""
    def indictor_prepared_parallel_func(k, sk_df):
        if k % 100 == 0:
            processbar("indicator parallel prepared", k, 800)
        sk_df = __mix_set(sk_df)
        return sk_df

    def df_slice_parallel_func(alldf, slice_position, max_slice,parrel_slice="read_the_docs"):
        """ 本来要对每个股票df,遍历生成 delayed（新增指标）, 现在并行 生成 delayed对象"""
        """parallel_slice_ must be settled before run default 0:1200 1200:2400 2400:end"""


        partition_len = int(len(alldf['ts_code'].unique())/max_slice)+1
        print("df slice partition to insert cache",slice_position * partition_len,(slice_position + 1) * partition_len)
        ts_codes = alldf['ts_code'].unique()[slice_position * partition_len:(slice_position + 1) * partition_len]


        delayed_jobs = []
        for k, ts_code in enumerate(ts_codes):
            processbar("df_slice parallel prepared", k, partition_len)
            sk_df = alldf[alldf['ts_code'] == ts_code]
            delayed_jobs.append(delayed(indictor_prepared_parallel_func)(k, sk_df))
        return delayed_jobs

    jobs = []
    jobs_three_part = Parallel(worker_n, max_nbytes=None)(delayed(df_slice_parallel_func)(alldf, i, worker_n) for i in range(worker_n))

    for i in jobs_three_part:
        jobs.extend(i)
    from numpy.random import shuffle
    shuffle(jobs)
    # print(jobs[:2])

    dfs = Parallel(worker_n , max_nbytes=None)(jobs)


    return dfs


def days_allprep_data(ts_code,time_stamp ="",days = 121,parallel = True,**kwargs):

    """do_all_prepared_to_cache first  才能返回 return get各个股票信息"""
    if time_stamp:
        days = (datetime.datetime.now() - datetime.datetime.strptime(time_stamp,"%Y-%m-%d")).days + days

    if not recent_df_cache:
        [recent_df_cache.setdefault(nm,pd.DataFrame) for nm in get_all_skname()]
        alldf = _allprep_data_storage(days=days,**kwargs)

        l = time.time()

        if parallel:
            _insert_cache_parallel(alldf)
        elif not parallel:
            for k,sk in enumerate(alldf['ts_code'].unique()):
                sk_df = alldf[alldf['ts_code'] == sk]
                recent_df_cache[sk] = sk_df
                if k%100 == 0:
                    processbar("insert cache",k,3600)



        print("insert cache:",format(time.time() - l,'.2f' ))

    if ts_code in recent_df_cache.keys():
        df = recent_df_cache[ts_code]
    else:
        df = pd.DataFrame()

    if df.empty:
        return df

    if time_stamp:
        # pandas的时间索引不科学，不能逆序，所以要先按日期排序
        df = select_table_by_timestamp(df,time_stamp)
    return df


def _insert_cache_parallel(alldf,worker_n = 6):
    """多进程 遍历总股票表 然后分别{代码：股票df} 插入缓存字典recent_df_cache"""
    def df_slice_parallel_func(alldf, slice_position,max_slice, parrel_slice="read_the_docs"):

        """ 本来要对每个股票df,遍历生成 delayed（新增指标）, 现在并行 生成 delayed对象"""
        """parallel_slice_ must be settled before run default 0:1200 1200:2400 2400:end"""
        partition_len = int(len(alldf['ts_code'].unique())/max_slice)+1
        print("df slice partition to insert cache",slice_position * partition_len,(slice_position + 1) * partition_len,'\n')
        ts_codes = alldf['ts_code'].unique()[slice_position * partition_len:(slice_position + 1) * partition_len]

        df_tups = []
        for k, ts_code in enumerate(ts_codes):
            processbar("df_slice parallel to insert cache", k, partition_len)
            sk_df = alldf[alldf['ts_code'] == ts_code]
            df_tups.append((ts_code, sk_df))

        return df_tups

    dfs = []
    """dfs = [(ts_codes,df)]"""
    dfs_three_part = Parallel(worker_n, max_nbytes=None)(delayed(df_slice_parallel_func)(alldf, i,worker_n) for i in range(worker_n))

    for i in dfs_three_part:
        dfs.extend(i)

    for k, tup in enumerate(dfs):
        ts_code, sk_df = tup
        # print(ts_code)
        recent_df_cache[ts_code] = sk_df
        if k % 100 == 0:
            processbar("insert cache", k, 3600)


def get_ts_data(ts_code,time_stamp="",money_flow = False):
    """得到单个股票的信息"""
    pro_bar = get_df_three_times(ts.pro_bar)
    df = pro_bar(ts_code='{}'.format(ts_code), start_date='20170801', end_date= tday_timestp, api=pro,factors = ["vr"])
    df = df.sort_values(by = "trade_date")
    # print(df.columns,get_ts_data.__name__)

    if money_flow==True:
        money_df = moneyflow_onedays_df(ts_code = ts_code)
        money_df = money_df.drop(labels=['ts_code'],axis=1)
        df = pd.merge(df,money_df,how='left',on=['trade_date'])


    df = __mix_set(df)
    if time_stamp:
        df = select_table_by_timestamp(df,time_stamp)
    return df

def get_ts_prepared_data(ts_code,time_stamp =""):
    l = time.time()
    """一支接一支获得所有股票的信息，存入cache"""
    if not get_ts_df_cache:
        all_skame = get_all_skname()
        [get_ts_df_cache.setdefault(nm,pd.DataFrame) for nm in all_skame]

        dfs = _threading_load_recent(get_ts_data,all_skame)
        for df in dfs:
            get_ts_df_cache[df.ts_code[-1]] = df
        print("sss",time.time()- l)

    df =  get_ts_df_cache[ts_code]
    if df.empty:
        return df
    if time_stamp:
        # pandas的时间索引不科学，不能逆序，所以要先按日期排序
        df = select_table_by_timestamp(df,time_stamp)
    return df



def select_table_by_timestamp(df,time_stamp):
    df = df.sort_values(by="date")
    df = df[:time_stamp]
    return df


def moneyflow_onedays_df(**kwargs):
    """单个天数 获得资金数据,
    :param :
    trade_date = "20190527"
    tscode = "600012.SH"
    """

    pro_moneyf = get_df_three_times(pro.moneyflow)
    df = pro_moneyf(**kwargs,fields=['buy_lg_amount',"buy_lg_amount",'sell_elg_amount','sell_lg_amount','ts_code','trade_date'],timeout = 20)
    if df.empty:
        return df

    df['income'] = df.apply(lambda x:x['buy_lg_amount']+x["buy_lg_amount"],axis=1)
    df['net_income'] = df.apply(lambda x:x['income']- x['sell_elg_amount'] - x['sell_lg_amount'],axis=1)

    return df[['income','net_income','ts_code','trade_date']]



def __mix_set(df):
    try:
        df = __set_datetime_index(df)
        df = indictor_adder(df)
        df = jump_open_adder(df)
    except Exception as e:
        print(df,e)
    return df

def indictor_adder(df):
    df = df.copy()
    df["ma_m1"] = MA(df,5)
    df["ma_m2"] = MA(df,10)
    df["ma_m3"] = MA(df,20)
    df["ma_m4"] = MA(df,30)
    df['ma_m5'] = MA(df,180)

    df['macd']  = MACD(df,12,26,9)[-1]
    df['kdj_k'],df['kdj_d'],df['kdj_j'] = KDJ(df,9,3,3)
    df['macd_120'] = MACD_120(df)
    # df = df.rename(columns = {"ts_code":'ts_code'})

    return df

def __set_datetime_index(df):
    df = df.rename(columns={"trade_date": "date"})
    df = df.sort_values(by= "date")
    date_stamp = pd.to_datetime(df['date'],format='%Y%m%d')
    df = df.set_index(date_stamp , drop=True)
    df.index.name = "a"
    return df


def MACD_120(df):
    df2 = df.copy()
    df2['df2_mark'] = 1
    df = pd.concat([df,df2],sort=True)
    df['df2_mark'] = df['df2_mark'].fillna(0)
    df = df.sort_values(by= ['date','df2_mark'])
    df['close'] = df.apply(lambda x:(x['open'] + x['close'])/2 if x['df2_mark']==0 else x['close'],axis= 1)
    df["macd_120"] = MACD(df,12,26,9)[-1]
    df = df[df['df2_mark']==0]
    df = df.drop(labels=['df2_mark'],axis=1)
    return df['macd_120'].fillna(0)


def load_ttl_vol():
    if not os.path.isfile("hdf_storage\\ttl_vol.xlsx"):
        print("take long to fetch all ttl_vol_at first time")
        dic = {}
        dic['sk_name'] = []
        dic['ttl_vol'] = []
        for i in get_all_skname()[:]:
            time.sleep(0.1)
            try:
                # print(i)
                pro_bar = get_df_three_times(ts.pro_bar)
                df = pro_bar(ts_code='{}'.format(i), start_date='20190801', end_date=tday_timestp, api=pro,
                             factors=["tor"])
                df = df.sort_values(by="trade_date")
                dic['ttl_vol'].append(int(df.iloc[-1,]['vol'] / df.iloc[-1,]['turnover_rate'] * 100))
                dic['sk_name'].append(i)
            except:
                print(i)
        df = pd.DataFrame(dic)
        df.to_excel("hdf_storage\\ttl_vol.xlsx",index=False)
    df = pd.read_excel("hdf_storage\\ttl_vol.xlsx")
    return df



def get_all_skname():
    return  get_df_three_times(pro.query)('stock_basic', exchange='', list_status='L', fields='ts_code')['ts_code'].tolist()

if __name__ == '__main__':
    df = moneyflow_onedays_df(trade_date = "20200722",tscode = "600012.SH")
    print(df)