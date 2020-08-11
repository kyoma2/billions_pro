# -*- encoding:utf-8 -*-
from conn.invate import get_trade_days
from selenium.webdriver import Chrome
from selenium import webdriver
import datetime
from bs4 import BeautifulSoup
import re

class ths_queapi:


    def __init__(self):
        self.cur_timestp = datetime.datetime.now().strftime("%Y-%m-%d")
        self.common = ",非新股,非st,股价小于30"
        self.trade_days = self.get_trade_days()
        self.td,self.ysd,self.two_dl,*rest = self.trade_days

    def get_trade_days(self):
        trade_days = get_trade_days(120,self.cur_timestp)
        trade_days.reverse()

        return [str(i[4:6]).lstrip("0") + "月"+ str(i[6:8]).lstrip("0") + "日" for i in trade_days]

    def set_review_date(self,timestp = None):
        """timestp = "几月几日" """
        if timestp == None:
            return self.trade_days


        trade_days = self.trade_days
        timestp_index = trade_days.index(timestp)

        return trade_days[timestp_index:]



    def tail_fall_but_money_in(self,timestamp,verbose=False):
        """每个小时区间跌幅但是资金流入,这种情况用于判断最后几天的洗盘"""
        td,ysd,two_dl,d_3,d_4,d_5,d_6,d_7,d_8,*rest = self.set_review_date(timestamp)
        res1 = f"{td}14点到15点跌幅大于1.5，" \
               f"({td}15点dde大单净额-{td}14点dde大单净额)>0," \
               f"({td}15点dde大单净额-{td}14点30分dde大单净额)>0," \
               f"{td}15点dde大单净额>0,{td}涨幅小于3," \
               f"{d_6}至{td}每天涨跌幅小于8," \
               f"{d_7}至{td}的区间涨跌幅>5%且{d_7}至{td}的区间涨跌幅<20%," \
               f"{td}涨幅小于3," \


        res1= res1 + self.common
        if verbose:
            print(res1)
        return res1

    def mid_fall_but_money_in(self,timestamp):
        """每个小时区间跌幅但是资金流入,这种情况用于判断最后几天的洗盘"""
        td,ysd,two_dl,d_3,d_4,d_5,d_6,d_7,d_8,*rest = self.set_review_date(timestamp)
        res1 = "{td}13点到14点跌幅大于1.5，" \
               "({td}14点dde大单净额-{td}13点dde大单净额)>0," \
               "{td}15点dde大单净额>0,{td}涨幅小于3," \
               "{d_7}至{td}的区间涨跌幅>5%且{d_7}至{td}的区间涨跌幅<20%," \
               "{td}涨幅小于3"

        res1= res1 + self.common
        # print(res1.format(td = td,d_6= d_6,d_7=d_7))


        return res1.format(td = td,d_6= d_6,d_7=d_7)

    def big_suck_not_leave(self,timestamp,verbose=False):
        """资金流入，成交量暴增，不能涨停，次日的成交量放缓，资金继续少量流入"""
        td,ysd,two_dl,*rest = self.set_review_date(timestamp)
        res1 = f"({ysd}dde大单净额/dde大单买入金额)>0.2," \
               f"({ysd}换手率/{two_dl}换手率)>2," \
               f"({td}换手率/{ysd}换手率) <1," \
               f"{td}dde大单净额>0," \
               f"{ysd}涨幅大于3小于10,"

        res1 = res1 + self.common
        if verbose:
            print(res1)
        return res1

    def jump_fall_suck(self,timestamp=None,verbose = False):
        """资金流入，成交量暴增，不能涨停，次日的成交量放缓，资金继续少量流入"""

        td,ysd,two_dl,*rest = self.set_review_date(timestamp)
        res1 = f"({ysd}dde大单净额/dde大单买入金额)>0.1," \
               f"{ysd}dde大单净额>300万," \
               f"{ysd}低开>1.5个点," \
               f"{ysd}跌幅<8," \
               f"{two_dl}涨跌幅>-3," \
               f"({ysd}换手率/{two_dl}换手率)>1.5," \
               f"{td}dde大单净额>0" \

        res1 = res1+ self.common
        # print(res1.format(ysd = ysd,two_dl = two_dl,td = td))
        if verbose:
            print(res1)
        return res1

    def sss(self,timestamp = None,verbose =False):
        # print(self.set_review_date(timestamp))
        td,ysd,two_dl,d_3,d_4,d_5,d_6,d_7,d_8,d_9,d_10,*rest = self.set_review_date(timestamp)
        res1 =  f"{ysd}换手率/{two_dl}换手率大于1.1，" \
                f"{ysd}最高价/{two_dl}收盘价大于1.09，" \
                f"{ysd}最高价/{ysd}开盘价大于1.03，" \
                f"0.98<{ysd}开盘价/{ysd}收盘价<1.02,{ysd}涨幅小于5，" \
                f"{td}高开大于0"


        res1 = res1+ self.common
        if verbose:
            print(res1)
        return res1

    def up_break_stable_line(self,timestamp = None,verbose = False):
        td,ysd,two_dl,d_3,d_4,d_5,d_6,d_7,d_8,d_9,d_10,*rest = self.set_review_date(timestamp)
        res1 = f"{d_5}至{ysd}涨幅小于6," \
            f"{ysd}最高价/近20日最高价>0.95," \
            f"{td}最低价/{ysd}收盘价< 0.96," \
            f"{td}开盘价/{td}最低价大于1.03," \
            f"{td}涨幅大于0," \
            f"{td}dde大单净额大于0,"
        if verbose:
            print(res1)
        return res1

    def lower_shadow_line(self,timestamp = None,verbose =False):
        td,ysd,two_dl,d_3,d_4,d_5,d_6,d_7,d_8,d_9,d_10,*rest = self.set_review_date(timestamp)
        """找上影线的"""
        res1 = f"1.02<{ysd}收盘价/开盘价<1.06," \
               f"{ysd}开盘价/{ysd}最低价>1.03, " \
               f"{ysd}最低价大于20日线小于{ysd}5日线，" \
               f"{ysd}最低价大于20日线小于{ysd}10日线，" \
               f"{ysd}最高价/{ysd}收盘价<1.02," \
               f"{ysd}涨跌幅小于4," \
               f"{ysd}收盘价大于5日线,"

        if verbose:
            print(res1)
        return res1

    def five_reds_under_m20(self,timestamp = None,verbose =False):
        td,ysd,two_dl,d_3,d_4,d_5,d_6,d_7,d_8,d_9,d_10,*rest = self.set_review_date(timestamp)

        res1 = f"{td}小于20日线，{d_4}至{d_5}有大于4个阳线，{d_4}至{d_5}涨幅小于6"
        if verbose:
            print(res1)
        return res1

    def onefake_of_threereds(self,timestamp = None,verbose =False):
        td,ysd,two_dl,d_3,d_4,d_5,d_6,d_7,d_8,d_9,d_10,*rest = self.set_review_date(timestamp)

        res1 = f"{td}换手率大于{ysd}换手率," \
               f"{td}换手率/{d_10}至{td}换手率 <0.1," \
               f"{two_dl}至{td}有大于2个阳线，" \
               f"{two_dl}至{td}涨幅小于5，" \
               f"{two_dl}至{td}有大于0个假阳线，" \
               f"{d_3}是阴线," \
               f"{td}dde大单净额/dde大单买入金额大于0.05"
        if verbose:
            print(res1)
        return res1

    def onefake_red_after_twored(self,timestamp = None,verbose =False):
        td, ysd, two_dl, d_3, d_4, d_5, d_6, d_7, d_8, d_9, d_10, *rest = self.set_review_date(timestamp)
        res1 = f"{two_dl}至{td}有大于2个阳线，" \
            f"{td}跌幅大于1，" \
            f"{two_dl}至{td}涨幅小于6，" \
            f"{td}收盘价小于{ysd}开盘价，"

        if verbose:
            print(res1)
        return res1



    def requests_selenium(self,query):
        """chromedrive """
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = Chrome(options=options)
        driver.get(
            "http://www.iwencai.com/stockpick/search?typed=0&preParams=&ts=1&f=1&qs=result_original&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w={query}"
            .format(query=query))
        try:
            web_data = str(driver.page_source).encode("gbk",'ignore').decode("gbk")
            soup = BeautifulSoup(web_data,'lxml')
            all_res_li = soup.find_all('a',attrs={"href":re.compile("stockpick/search\?tid=stockpick&qs=stockpick_diag.*")})
            tscode = [i.get("href")[-6:] for i in all_res_li]
        except Exception as e:
            raise e
        finally:
            driver.quit()
        return tscode


    def ss(self,td,verbose = 0):

        stg = [
                # self.tail_fall_but_money_in,
                # self.onefake_of_threereds,
                # self.jump_fall_suck,
                # self.lower_shadow_line,
                # self.five_reds_under_m20,
                self.onefake_red_after_twored


               ]



        res = {}
        for i in stg:
            outcome = self.requests_selenium(i(td,verbose))
            if outcome==[]:
                continue
            res[i.__name__] = outcome


        # to_pd_Dataframe
        import pandas as pd
        index = []
        for i in  res.values():
            index.extend(i)
        columns = [i for i in  res.keys()]


        df = pd.DataFrame(index = index,columns =columns,data= 0)
        df['score'] =""
        for k,v in zip(res.keys(),res.values()):
            for value in v:
                df.loc[df.index == value,k] = 1
        for ts_code in df.index:
            if str(ts_code).startswith("6"):
                ts_code += ".SH"
            else:
                ts_code += ".SZ"
            from conn.invate import get_ts_data
            from util.tools import check_score
            import time
            a = time.strptime("2020年"+td, "%Y年%m月%d日")
            timestamp = time.strftime("%Y-%m-%d", a)
            score = check_score(get_ts_data(ts_code), timestamp, 10)
            df.loc[df.index == ts_code[:6], "score"]= score[1]

        df['link'] = "http://stockpage.10jqka.com.cn/" + df.index
        df.to_excel("..\check_report\\ths{}.xlsx".format(td))
        # print(df)

        print("-"*10,td,"-"*10,'\n')
        for col in df.columns:
            part_df = df.loc[df[col]==1,'score']
            if not part_df.empty:
                print(col,f":{(20-len(col))*' '} num {str(part_df.count())} score {str(part_df.mean())}")




    def lots_of_work(self,timestamp,days = 100,verbose = 0):
        if timestamp not in self.trade_days:
            self.trade_days = [timestamp] + self.trade_days
        trade_days = self.set_review_date(timestamp)[:days]
        for tmstamp in trade_days:
            # print(tmstamp)
            self.ss(tmstamp,verbose=verbose)



a = ths_queapi()

a.lots_of_work("8月10日",verbose=1)
# a.lots_of_work("7月25日")
# a.ss("6月11日")
# driver = Chrome()
# driver.get("http://www.iwencai.com/stockpick/search?typed=0&preParams=&ts=1&f=1&qs=result_original&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=")
