import requests
import json
import re




web_data = requests.get("http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=4010&po=1&np=1&ut=b2884a393a59ad64002292a3e90d46a5&fltt=2&invt=2&fid0=f4001&fid=f62&fs=m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2&stat=1&fields=f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124&rt=53234936&cb=jQuery18305371687909894549_1597048106309&_=1597048106583")
a = re.findall("{.*}",web_data.text)[0]

json_data = json.loads(a)['data']['diff']

import pandas as pd
df = pd.DataFrame(json_data)
df = df[['f12','f14','f62']]
df.to_excel("../check_report/dfcf_zj.xlsx")
print(df)
# print(web_data.text)