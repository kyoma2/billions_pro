import requests
import json
import re





def check(sk_code,page):
    d = {"view":{"nolazy":1,"parseArr":{"_v":"new","dateRange":["20190523","20190523"]}}}
    f = json.dumps(d)

    para = """pid={pic}&codes={sk_code}&codeType=stock&page={page}&""".format(pic = "8731",sk_code = "002164",page =1)+"info=" +f
    para = "pid=8731&codes=002164&codeType=stock&info=%7B%22view%22%3A%7B%22nolazy%22%3A1%2C%22parseArr%22%3A%7B%22_v%22%3A%22new%22%2C%22dateRange%22%3A%5B%2220190523%22%2C%2220190523%22%5D%2C%22staying%22%3A%5B%5D%2C%22queryCompare%22%3A%5B%5D%2C%22comparesOfIndex%22%3A%5B%5D%7D%2C%22asyncParams%22%3A%7B%22tid%22%3A8547%7D%7D%7D"
    a = requests.get("http://www.iwencai.com/diag/block-detail?{}".format(para),headers = cookie).headers
    print(para)
    print(a.text)
    # a= json.loads(a.text)

# check("","")



def find_pid():

    a = requests.get("http://www.iwencai.com/stockpick/search?typed=0&preParams=&ts=1&f=1&qs=result_original&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%E5%AE%81%E6%B3%A2%E4%B8%9C%E5%8A%9B+%E5%85%AC%E5%91%8A",headers = cookie)
    b = re.findall('pid=\d{2,6}',a.text)

    print(b)


"""默认cookie 我删了一部分"""
headers = {"Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Cookie": "cid=5988517f5dcebf8e0056cd2140560e2e1548387991; ComputerID=5988517f5dcebf8e0056cd2140560e2e1548387991; other_uid=Ths_iwencai_Xuangu_uuy8aosl7sa5y3e57dzr3aizpbqxdd6z; other_uname=5t7xcezm5d; guideState=1; user=MDrSttfT1Nq2rMPfOjpOb25lOjUwMDoxNzU3Mjc1MDU6NywxMTExMTExMTExMSw0MDs0NCwxMSw0MDs2LDEsNDA7NSwxLDQwOzEsMSw0MDsyLDEsNDA7MywxLDQwOzUsMSw0MDs4LDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAxLDQwOjI3Ojo6MTY1NzI3NTA1OjE1NTg1MDI4Nzg6OjoxMzcyMzA5MjYwOjYwNDgwMDowOjE4NDgwNWE5NjVlZGNlNWMxMzFmY2IwNGZkZmQwYzAzNzpkZWZhdWx0XzI6MA%3D%3D; userid=165727505; u_name=%D2%B6%D7%D3%D4%DA%B6%AC%C3%DF; escapename=%25u53f6%25u5b50%25u5728%25u51ac%25u7720; ticket=5f91073e42c94ecbdf4c8d0c692181e3; PHPSESSID=c3c26dd205f84bb49df51d15505ccb4b; ",
        # "hexin-v": "AlAABmnZODVBQOQxsAg4wEj5IZWhGTRjVv2IZ0ohHKt-hfqD8ikE86YNWPCZ",
        "Host": "www.iwencai.com",
        "Referer": "http://www.iwencai.com/stockpick/search?typed=0&preParams=&ts=1&f=1&qs=result_original&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%E5%AE%81%E6%B3%A2%E4%B8%9C%E5%8A%9B+%E5%85%AC%E5%91%8A",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"}



"""我把headers['Cookie'] 中的V 值删掉了"""
"""如果新增的v 是网站chrome中保存下来新的 就可以正常访问，不然结果报错"""

headers['Cookie'] += "v=Al4O7OtfjmUzDtqB4qcOSqoXr_-jHyKZtOPWfQjnyqGcK_SpcK9yqYRzJovb"
s = requests.Session()
a = s.get("http://www.iwencai.com/stockpick/search?typed=0&preParams=&ts=1&f=1&qs=result_original&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%E5%AE%81%E6%B3%A2%E4%B8%9C%E5%8A%9B+%E5%85%AC%E5%91%8A",headers = headers)
print(a.cookies)

