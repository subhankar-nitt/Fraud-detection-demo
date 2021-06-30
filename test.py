import pandas as pd
import numpy as np
#testing last 24hours count of transaction code
def load_data():
    data = [['2019-01-01 00:00:18'  ,1, 20,     1,  33.9659,    -80.9355,   33.986391,-81.200714],
             ['2019-01-01 00:00:28' ,1, 10.1,   1,  40.3207  , -110.436,    39.450498, -109.960431],
             ['2019-05-02 02:00:30' ,2, 5.5,    0,  40.6729  , -73.5365  ,  40.49581 , -74.196111],
             ['2019-05-02 12:01:28' ,1, 80.6,   0,  28.5697  , -80.8191  ,  28.812398, -80.883061],
             ['2019-06-02 06:59:18' ,1, 180.6,  1,  30.5354  , -95.4532  ,  31.152375, -95.272196],
             ['2019-06-02 08:01:29' ,1, 1080.6, 1,  28.5697  , -80.8191  ,  28.812398, -80.883061],
             ['2019-06-03 11:31:38' ,1, 12.2,   1,  35.8312  , -94.1187  ,  36.016774, -94.27251],
             ['2019-08-02 02:07:27' ,2, 72.1,   0,  40.6729  , -73.5365  ,  41.609817, -73.327921],
             ['2019-08-05 03:26:15' ,1, 80.6,   1,  41.6964  , -96.9858  ,  42.54643,  -97.00985],
             ['2019-09-10 04:07:04' ,2, 576.2,  0,  35.6665  , -97.4798  ,  36.145536, -97.524801]]
    df = pd.DataFrame(data, columns=['time', 'cc_num','amount','is_fraud','lat','long','merch_lat','merch_long'])
    df['Time'] = pd.to_datetime(df['time'])
    df.index = pd.to_datetime(df['time'])
    df = df.rename_axis(index={'time': 'time_index'})
    df = df.sort_index()
    df['counter']=1
    #df1.head()
    return df

def count_24_last(df):
    return df.groupby("cc_num")['counter'].rolling('24H').count().reset_index().fillna(0).to_dict()['counter']
#testing count of last 24 hours transactions
def test1():
    data = load_data()
    expected = {0: 1.0, 1: 2.0, 2: 1.0, 3: 1.0, 4: 2.0, 5: 1.0, 6: 1.0, 7: 1.0, 8: 1.0, 9: 1.0}
    result = count_24_last(data)
    assert expected == result

def amt_last_24(df):
    return df \
    .groupby(['cc_num'])['amount']\
    .rolling('24H')\
    .sum()\
    .reset_index()\
    .fillna(0).to_dict()['amount']
#testing sum in last 24 hours
def test2():
    data = load_data()
    expected = {0: 20.0, 1: 30.1, 2: 80.6, 3: 180.6, 4: 1261.1999999999998, 5: 12.2, 6: 80.6, 7: 5.5, 8: 72.1, 9: 576.2}
    result = amt_last_24(data)
    assert expected == result

def total_amt_30_days(df):
    df1= df \
        .groupby(['cc_num'])['amount'] \
        .rolling('30D') \
        .sum() \
        .shift(1) \
        .reset_index() \
        .fillna(0)
    df1.columns = ['cc_num', 'trans_date', 'total_amt_30d']
    return df1.to_dict()['total_amt_30d']

def test3():
    data = load_data()
    expected = {0: 0.0, 1: 20.0, 2: 30.1, 3: 80.6, 4: 180.6, 5: 1261.1999999999998, 6: 1273.3999999999999, 7: 80.6, 8: 5.5, 9: 72.1}
    result = total_amt_30_days(data)
    assert expected == result

def avg_amt_60_days(df):
    df1 = df.groupby(['cc_num'])['amount'].rolling('60D').mean().shift(1).reset_index().fillna(0)

    df1.columns = ['cc_num', 'trans_date', 'avg_amt_60d']
    return df1.to_dict()['avg_amt_60d']

def test4():
    data = load_data()
    expected = {0: 0.0, 1: 20.0, 2: 15.05, 3: 80.6, 4: 130.6, 5: 447.26666666666665, 6: 338.5, 7: 80.6, 8: 5.5, 9: 72.1}
    result = avg_amt_60_days(data)
    assert expected == result

def fraud_in_24h(df):
    return df \
    .groupby(['cc_num'])['is_fraud']\
    .rolling('24H')\
    .sum()\
    .reset_index()\
    .fillna(0).to_dict()['is_fraud']

def test5():
    data = load_data()
    expected = {0: 1, 1: 2, 2: 0, 3: 1, 4: 2, 5: 1, 6: 1, 7: 0, 8: 0, 9: 0}
    result = fraud_in_24h(data)
    assert expected == result

def trans_hour(data):
    return dict(zip(np.arange(0,len(data),1),data['Time'].dt.hour))

def test6():
    data = load_data()
    expected = {0: 0, 1: 0, 2: 2, 3: 12, 4: 6, 5: 8, 6: 11, 7: 2, 8: 3, 9: 4}
    result = trans_hour(data)
    assert expected == result

def getDistance(lat1,lat2,lon1,lon2):
  rad = 6371 #radious of the earth is 6371

  #converting to radians for different 
  lat1=np.radians(lat1)  
  lat2=np.radians(lat2)
  lon1=np.radians(lon1)
  lon2=np.radians(lon2)

  #difference between two places latitude and longitude
  dlat = lat2-lat1
  dlon = lon2-lon1

  #formula to claculate the distance between two place
  a = np.sin(dlat/2)**2+np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2

  c = np.sqrt(a)
  #distance / time = speed , returning the speed for the   
  return c*rad

def dist(data):
    d = dict()
    for i in range(len(data)):
        d[i] = getDistance(data.iloc[i]['lat'],data.iloc[i]['long'],data.iloc[i]['merch_lat'],data.iloc[i]['merch_long'])
    return d

def test7():
    data = load_data()
    expected = {0: 5711.432942546096, 1: 5286.730110466094, 2: 5899.3018597977425, 3: 5552.877439748958, 4: 5438.106144517965, 5: 5552.877439748958, 6: 5601.832025811776, 7: 5900.741478497138, 8: 5682.582263113524, 9: 5526.778507775557}
    result = dist(data)
    assert expected == result

    
def count_90d_last(df):
    return df.groupby("cc_num")['counter'].rolling('90D').count().reset_index().fillna(0).to_dict()['counter']

#testing count of last 90 days transactions
def test8():
    data = load_data()
    expected = {0: 1.0, 1: 2.0, 2: 1.0, 3: 2.0, 4: 3.0, 5: 4.0, 6: 4.0, 7: 1.0, 8: 1.0, 9: 2.0}
    result = count_90d_last(data)
    assert expected == result

