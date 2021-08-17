# import sys
# sys.path.append(sys.path[-1]+"/bt/alpha")

import datetime as dt
import dig_function
from DB_Reader import *
from PreProcessing import *
from Strategy import strat
import matplotlib.pyplot as plt
################################################################
# fetch data
################################################################
#testTicker = ['000001.SZ', '600000.SH']
# {'沪深300': 3145, '中证500': 4978, '中证800':4982}
testIndex = 4982
startDate = '2009-01-01'
endDate = '2019-01-01'
#dt.datetime.now().strftime('%Y-%m-%d')
# dt.datetime.now().strftime('%Y-%m-%d')
switch = 1
if switch:
    db = db_Secu(startDate, endDate, IndexCode=testIndex)
    db.get_Data()
    db.save(filename='中证800.bin')

db = db_Secu(filename='中证800.bin')
db.get_specialtrade()

################################################################
# data prep
################################################################
data = prep(db)
data.prep_data()
data.get_nonst_mat()
################################################################
# strat
################################################################




st = strat(data,freq='month')
testfactor_name='PE'
testfactor=data.get_mat(testfactor_name)

closes = data.closes.loc[st.period_ends]
Benchmark = data.benchmark.reset_index('Ticker',drop=True)
Benchmark= Benchmark.loc[st.period_ends]
Benchmark_retu= Benchmark['ClosePrice'].pct_change()

inPool_dummy = data.inPool_dummy.loc[st.period_ends]
volume = data.get_mat('TurnoverVolume').loc[st.period_ends]
trade_avail = buildFilter(volume, positive=True)

st.setFactor(testfactor,testfactor_name)
st.setFilter(inPool_dummy,'inPool')
st.setFilter(trade_avail,'trade available')
#
st.run_factor(testfactor_name,layerMat=None)
#
plotdata=st.run_factor_plot(testfactor_name)
IC=st.resultList[testfactor_name]['IC']
rankIC=st.resultList[testfactor_name][ 'RankIC' ]
all_data=pd.merge(plotdata,Benchmark_retu,how='inner',left_index=True,right_index=True)
all_data=(all_data+1).cumprod()
all_data.columns=['1','2','3','4','5','benchmark']

excess=plotdata.T-Benchmark_retu
excess=excess.T
excess=excess.rolling(12).sum()
 
plot_data(all_data,excess,IC,testfactor_name)   
year_return(plotdata,Benchmark_retu,testfactor_name)







#
#
#
#st = strat(data, freq='month')
#
#closes = st.closes
#
#sector = data.get_mat('FirstIndustryCode').loc[st.period_ends]
#
#
#reversal = closes.pct_change()
#reversal_rel = reversal- sector_fun(reversal, sector)
#
#inPool_dummy = data.inPool_dummy.loc[st.period_ends]
#volume = data.get_mat('TurnoverVolume').loc[st.period_ends]
#trade_avail = buildFilter(volume, positive=True)
#
#st.setFactor(reversal, 'reversal')
#st.setFactor(reversal_rel, 'relative reversal')
#st.setFilter(inPool_dummy, 'inPool')
#st.setFilter(trade_avail, 'trade available')
#
#st.run_factor('reversal', layerMat=None)
#st.run_factor('relative reversal', layerMat=None)
#st.run_factor_plot('reversal')
#st.run_factor_plot('relative reversal')
#
#
#
#
#
#
#
#
#
#
## obj2.data
## obj2.mat_indexPool()
## obj2.to_mat('TurnoverVolume')
#
## tablename = 'lc_indexbasicinfo'
## columns = '*'
## option = ''
## df0  = obj2.read_sql(tablename, columns, option)
## InnerCode = list(df0['IndexCode'].unique())
##
##
## tablename = 'secumain'
## InnerCode = ','.join(list(map(str, InnerCode)))
## option = "WHERE InnerCode in (%s)" % (InnerCode)
##
## df1 = obj2.read_sql(tablename, columns, option)
## df1.to_csv('index.csv',encoding="utf_8_sig")
