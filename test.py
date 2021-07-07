import pandas as pd
def load_data():
    data = [['2019-01-01 00:00:18' ,1,20,'entertainment'],
             ['2019-01-01 00:00:28' ,1,10,'entertainment'],
             ['2019-05-02 02:00:30' ,1,5.5,'entertainment'],
             ['2019-05-02 02:01:28' ,1,80.6,'travel'],
            ['2019-05-02 02:01:55', 1, 45,'travel'],
            ['2020-01-01 02:00:00', 2, 10,'home'],
            ['2020-01-02 01:59:59', 2, 30,'home'],
            ['2020-01-03 01:59:59',2, 55,'entertainment'],
            ['2020-02-03 02:00:00', 2, 100,'travel'],
            ['2020-03-03 01:59:59', 2, 10,'travel']]

    df = pd.DataFrame(data, columns=['time', 'cc_num','amount','category'])
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
    expected = {0:1.0,1:2.0,2:1.0,3:2.0,4:3.0, 5:1.0, 6: 2.0, 7:1.0, 8:1.0,9:1.0}
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
    expected = {0:20.0,1:30.0,2:5.5,3:86.1, 4:131.1,5:10.0, 6:40.0, 7:55.0,8:100.0, 9:10.0}
    result = amt_last_24(data)
    assert expected == result

def total_amt_30_days(df):
    df1= df \
        .groupby(['cc_num'])['amount'] \
        .rolling('30D') \
        .sum() \
        .reset_index() \
        .fillna(0)
    df1.columns = ['cc_num', 'trans_date', 'total_amt_30d']
    return df1.to_dict()['total_amt_30d']

def test3():
    data = load_data()
    expected = {0: 20.0, 1: 30.0, 2: 5.5, 3: 86.1,4:131.1, 5: 10.0, 6: 40.0, 7: 95.0, 8: 100.0, 9: 110.0}
    result = total_amt_30_days(data)
    assert expected == result

def count_trans_90D(df):
    df2= df \
            .groupby(['cc_num'])['counter'] \
            .rolling('90D') \
            .count() \
            .reset_index() \
            .fillna(0)
    df2.columns=['cc_num', 'trans_date', 'hist_trans_90d']
    return df2.to_dict()['hist_trans_90d']

def test4():
        data = load_data()
        expected = {0: 1.0, 1: 2.0, 2: 1.0, 3: 2.0, 4: 3.0, 5: 1.0, 6: 2.0, 7: 3.0, 8: 4.0, 9: 5.0}
        result = count_trans_90D(data)
        assert expected == result

def count_trans_category_30D(df):
    df3= df \
            .groupby(['cc_num','category'])['counter'] \
            .rolling('30D') \
            .count() \
            .reset_index() \
            .fillna(0)
    df3.columns=['cc_num','category', 'trans_date','category_type_count']
    return df3.to_dict()['category_type_count']

def test5():
        data = load_data()
        expected = {0: 1.0, 1: 2.0, 2: 1.0, 3: 1.0, 4: 2.0, 5: 1.0, 6: 1.0, 7: 2.0, 8: 1.0, 9: 2.0}
        result = count_trans_category_30D(data)
        assert expected == result

def avg_amt_trans_30D(df):
    df4= df \
            .groupby(['cc_num'])['amount'] \
            .rolling('30D') \
            .mean() \
            .reset_index() \
            .fillna(0)
    df4.columns=['cc_num', 'trans_date','hist_trans_avg_amt_30d']
    return df4.to_dict()['hist_trans_avg_amt_30d']

def test6():
        data = load_data()
        expected = {0: 20.0, 1: 15.0, 2: 5.5, 3: 43.05, 4:  43.699999999999996, 5: 10.0, 6: 20.0, 7: 31.666666666666668, 8: 100.0, 9: 55.0}
        result = avg_amt_trans_30D(data)
        assert expected == result

def amt_category_30D(df):
    df5= df \
            .groupby(['cc_num','category'])['amount'] \
            .rolling('30D') \
            .sum() \
            .reset_index() \
            .fillna(0)
    df5.columns=['cc_num','category', 'trans_date','hist_category_amt_30d']
    return df5.to_dict()['hist_category_amt_30d']

def test7():
        data = load_data()
        expected = {0: 20.0, 1: 30.0, 2: 5.5, 3: 80.6, 4: 125.6, 5: 55.0, 6: 10.0, 7: 40.0, 8: 100.0, 9: 110.0}
        result = amt_category_30D(data)
        assert expected == result

