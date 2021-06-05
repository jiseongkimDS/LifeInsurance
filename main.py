import requests
import pandas as pd
import json
import numpy as np
from functools import reduce

#Variable Definition
rownum, pgnum, ym = '196', '1', '201612'

# References
url = 'http://apis.data.go.kr/1160100/service/GetLifeInsuCompInfoService/getLifeInsuCompGeneInfo'
queryParams = '?' + 'ServiceKey=' + 'SzfcS4DsKYQpTde0nf0wDoNDomO7hBvjakHgeNqSGwh0USlMlVeywpcOhL2mA5MwxGryRC238PEOnIdUJSsgQA%3D%3D' + \
              '&numOfRows=' + rownum + \
              '&pageNo=' + pgnum + \
              '&resultType=' + 'json' + \
              '&basYm=' + ym + \
              '&title=' + '생보_일반현황_임직원 및 설계사 현황'
url = url + queryParams

#Request & Json_Parser
result = requests.get(url)
json_object = json.loads(result.content)
print(pd.json_normalize(json_object))
df=pd.json_normalize(json_object['response']['body']['tableList'][0]['items']['item'])
print(df)

# Column Selection $ Relocation
df = df[['basYm', 'crno', 'fncoNm', 'fncoCd', 'xcsmPlnpnDcdNm','xcsmPlnpnCnt']]

# Row Selection (Remove Subsum of xcsmPlnpnDcdNm)
mask = df['fncoCd'].str.contains('S')
df = df[~mask]

#Column Rename
df.columns = ['기준연도', '법인등록번호', '기업명', '고유번호', '직급', '해당직급 직원수']

#DataFrame Split & Join
dfs = np.array_split(df, df.shape[0]/24)
df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['기준연도', '법인등록번호', '기업명', '고유번호'],how='left'), dfs)

# Row Sum
df_bef_sum = df_merged[['해당직급 직원수_x','해당직급 직원수_y','해당직급 직원수']]
df_bef_sum = df_bef_sum.apply(pd.to_numeric)
df_merged['총직원수'] = df_bef_sum.apply(lambda x:np.sum(x), axis=1)
print(df_merged)

#To_CSV
df_merged.to_csv('practice.csv',encoding='euc-kr')