# coding:utf-8
import requests,re
from random import randint

# "Host": "upass.10jqka.com.cn",
# "Origin": "http://upass.10jqka.com.cn",
# "Upgrade-Insecure-Requests": "1",
# "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
# "Accept-Encoding": "gzip, deflate",
# "Accept-Language": "zh,zh-CN;q=0.9,zh-TW;q=0.8",
# "Cache-Control": "max-age=0",
# "Connection": "keep-alive",
# "Content-Length": "566",
# "Content-Type": "application/x-www-form-urlencoded",

# web_data = requests.get("http://stock.10jqka.com.cn/self.php?stockcode=001872&op=del&&jsonp=jQuery1110062020114_1555852571804&_=1555852571821",headers = cookie)

#
# print(web_data.text)
# print(u"\u80a1\u7968\u5df2\u5b58\u5728")
#
#




# web = requests.get(op["getselfstock"].format( rnd_num = randint(10000000,99999999)),headers = cookie)
# print(web.text)



class ths_api:

    """ zqh :15361555096: zhou666
        zxm :38831550  :241875
        lcl :chu
    """

    my_cookie = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
        "Cookie": "cid=8ffbe6857a0faefb7d01e53e456a39a11577149599; ComputerID=8ffbe6857a0faefb7d01e53e456a39a11577149599; guideState=1; other_uid=Ths_iwencai_Xuangu_5b05e2bfd3f8a1301f4bbee45fc40a1f; PHPSESSID=e8183d95bae41340ba0bac9dd7dc141c; user=MDrSttfT1Nq2rMPfOjpOb25lOjUwMDoxNzU3Mjc1MDU6NywxMTExMTExMTExMSw0MDs0NCwxMSw0MDs2LDEsNDA7NSwxLDQwOzEsMTAxLDQwOzIsMSw0MDszLDEsNDA7NSwxLDQwOzgsMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDEsNDA6Mjc6OjoxNjU3Mjc1MDU6MTU4NTIwMTAwNzo6OjEzNzIzMDkyNjA6NjA0ODAwOjA6MTg4MTAyM2ZmOTdhMDkyYjExYzNmZDhlNjMyOTA1ZTlhOmRlZmF1bHRfNDow; userid=165727505; u_name=%D2%B6%D7%D3%D4%DA%B6%AC%C3%DF; escapename=%25u53f6%25u5b50%25u5728%25u51ac%25u7720; ticket=71e49f76a4527b9a128440659e1bf91e; v=Aq39caxqj3DmlmucYPWtL0WavEIkCuFIaz9Fse-y6ZmzjMe0t1rxrPuOVYd8"}

    zhou_cookie = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
        "Cookie": "historystock=600012; spversion=20130314; log=; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1555852427,1555852428,1555930765; Hm_lvt_da7579fd91e2c6fa5aeb9d1620a9b333=1555856980,1555857232,1555857275,1555930767; Hm_lpvt_da7579fd91e2c6fa5aeb9d1620a9b333=1555930767; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1555930767; v=AgJBLRws6mBx3Pa84m_PUkSKUwNn0weSuNL6EUwILf8ge6w1NGNW_YhnSiIf; user=MDpteF80ODIxMTQ0NzI6Ok5vbmU6NTAwOjQ5MjExNDQ3Mjo3LDExMTExMTExMTExLDQwOzQ0LDExLDQwOzYsMSw0MDs1LDEsNDA7MSwxLDQwOzIsMSw0MDszLDEsNDA7NSwxLDQwOzgsMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDEsNDA6MTY6Ojo0ODIxMTQ0NzI6MTU1NTkzMDg1Mjo6OjE1NTM4NDczMDA6NjA0ODAwOjA6MThhZGE1NzE2N2FhM2ExMjg2OWNkMmRmMDFkYjU0YzIzOmRlZmF1bHRfMjow; userid=482114472; u_name=mx_482114472; escapename=mx_482114472; ticket=450818daa4d4a335d5e9208765ee3e21"}

    op = {
        "delete": "http://stock.10jqka.com.cn/self.php?stockcode={stockcode}&op=del&&jsonp=jQuery1110026471582{rnd_num}_1555854844255&_=1555854844294",
        "add": "http://stock.10jqka.com.cn/self.php?stockcode={stockcode}&op=add&&jsonp=jQuery1110062020154{rnd_num}_1555852571802&_=1555852571821",
        "getselfstock": "http://pop.10jqka.com.cn/getselfstockinfo.php?jsonp=callbackinfo&_=15558{rnd_num}&callback=callbackinfo"}

    cookies = {"lcl":my_cookie,"zqh":zhou_cookie}

    def __init__(self):
        self.cookie = ''

    def set_cookie(self,name):
        self.cookie = self.cookies[name]


    def __operater(self,sk_name,how="add"):
        web = requests.get(self.op[how].format(stockcode = sk_name,rnd_num = randint(10000000,99999999)), headers = self.cookie)
        # print(web.text)
        res = re.findall('\.*"',web.text)
        # print(web.text)
        return "u"+ str(res)

    def add(self,sk_name):
        self.__operater(sk_name)

    def delt(self,sk_name):
        self.__operater(sk_name,how='delete')

    def multi_adder(self,stock_list):
        for sk_code in stock_list:
            self.add(sk_code)

    def getNowSelfStock(self):
        web_data = requests.get(self.op["getselfstock"].format(rnd_num=randint(10000000, 99999999)), headers= self.cookie)
        return re.findall('\d\d\d\d\d\d',web_data.text)

    def multi_delter(self,keep_alive = 10):
        for sk_code in self.getNowSelfStock()[keep_alive:]:
            self.delt(sk_code)

    def synchro_ths(self,p_name,st_code_list,delete = True,keep_alive = 3):
        self.set_cookie(p_name)
        if delete == True:
            self.multi_delter(keep_alive= keep_alive)

        #最多99个 不然同花顺封号

        stock_list = [alpha_digit[:6] for alpha_digit in st_code_list][:40]
        print("开始同步同花顺",stock_list)
        # self.multi_adder(stock_list)
        # 换一个问财的更快
        self.add_wencai("|".join(stock_list))


    def add_wencai(self,strstockli):
        # data = {"codes": "000596.SZ|000609.SZ|600012.SH"}
        data = {"codes": "{}".format(strstockli)}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36",
                   "Cookie": "cid=8ffbe6857a0faefb7d01e53e456a39a11577149599; ComputerID=8ffbe6857a0faefb7d01e53e456a39a11577149599; guideState=1; other_uid=Ths_iwencai_Xuangu_5b05e2bfd3f8a1301f4bbee45fc40a1f; PHPSESSID=e8183d95bae41340ba0bac9dd7dc141c; user=MDrSttfT1Nq2rMPfOjpOb25lOjUwMDoxNzU3Mjc1MDU6NywxMTExMTExMTExMSw0MDs0NCwxMSw0MDs2LDEsNDA7NSwxLDQwOzEsMTAxLDQwOzIsMSw0MDszLDEsNDA7NSwxLDQwOzgsMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDEsNDA6Mjc6OjoxNjU3Mjc1MDU6MTU4NTIwMTAwNzo6OjEzNzIzMDkyNjA6NjA0ODAwOjA6MTg4MTAyM2ZmOTdhMDkyYjExYzNmZDhlNjMyOTA1ZTlhOmRlZmF1bHRfNDow; userid=165727505; u_name=%D2%B6%D7%D3%D4%DA%B6%AC%C3%DF; escapename=%25u53f6%25u5b50%25u5728%25u51ac%25u7720; ticket=71e49f76a4527b9a128440659e1bf91e; v=Aq39caxqj3DmlmucYPWtL0WavEIkCuFIaz9Fse-y6ZmzjMe0t1rxrPuOVYd8"}

        webdata= requests.post("http://www.iwencai.com/stockpick/addselfstock",data=data,headers = headers)

        print(webdata.text)





if __name__ == '__main__':
    ths = ths_api()
    ths.set_cookie("lcl")
    a = """000667.SZ
001965.SZ
002051.SZ
002120.SZ
002233.SZ
002263.SZ
300234.SZ
300334.SZ
300665.SZ
600634.SH
600720.SH
600725.SH
600778.SH
600801.SH
600841.SH
603385.SH
""".split()
    ths.multi_delter()
    ths.add_wencai("|".join(a))

    # ths.add_wencai("")
    # print(ths.getNowSelfStock())

    # ths.multi_adder(['000005', '000006', '000007', '000010', '000017'])