import requests
import pandas as pd
import json
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd
from functools import reduce

#Variable Definition
rownum, pgnum, ym = '196', '1', '201203'

#Time Difference
now = pd.datetime.now()
base = pd.datetime.strptime(ym,'%Y%m')
diff = rd(now, base)
monthdiff = diff.years*12 + diff.months

#List of DataFrame by months
ResultMonths = []

for i in range(0, monthdiff+1, 3):
    #Time Repetition
    ymtemp = base + pd.DateOffset(months=i)
    ymtemp = dt.strftime(ymtemp,'%Y%m')

    # References
    url = 'http://apis.data.go.kr/1160100/service/GetLifeInsuCompInfoService/getLifeInsuCompGeneInfo'
    queryParams = '?' + 'ServiceKey=' + 'SzfcS4DsKYQpTde0nf0wDoNDomO7hBvjakHgeNqSGwh0USlMlVeywpcOhL2mA5MwxGryRC238PEOnIdUJSsgQA%3D%3D' + \
                  '&numOfRows=' + rownum + \
                  '&pageNo=' + pgnum + \
                  '&resultType=' + 'json' + \
                  '&basYm=' + str(ymtemp) + \
                  '&title=' + '생보_일반현황_임직원 및 설계사 현황'
    url = url + queryParams

    try:
        #Request & Json_Parser
        result = requests.get(url)
        json_object = json.loads(result.content)
        df=pd.json_normalize(json_object['response']['body']['tableList'][0]['items']['item'])

        # Row Selection (Remove Subsum of xcsmPlnpnDcdNm)
        mask = df['fncoCd'].str.contains('S')
        df = df[~mask]

        #Total Num of Employees
        df['xcsmPlnpnCnt'] = df['xcsmPlnpnCnt'].apply(pd.to_numeric)
        subsum = pd.DataFrame()
        subsum[ymtemp] = df.groupby('fncoNm').sum()['xcsmPlnpnCnt']

        #Append
        ResultMonths.append(subsum)

    except:
        continue

#Join
result = reduce(lambda  left,right: pd.merge(left,right,on=['fncoNm'],how='outer'), ResultMonths)

#To_CSV
result.to_csv('LifeInsurance_TimeSeries.csv',encoding='euc-kr')