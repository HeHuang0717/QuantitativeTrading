import pandas as pd
import pymysql
# from sqlalchemy import create_engine
import pickle
import warnings
warnings.filterwarnings("ignore")

config = {'host':'192.168.18.16',
          'port':3307,
          'user':'zhgu',
          'password':'zhgu',
          'db':'jydb',
          'charset':'utf8'
          }
con = 'mysql+pymysql://%s:%s@%s:%d/%s' % \
      (config['user'], config['password'], config['host'], config['port'], config['db'])


class db_Secu:
    """
    给定股票代码列表或指数代码，提取历史上某一段时间的价格数据和行业数据
    指数代码{'沪深300':3145,'中证500':4978}
    股票代码格式：600000.SZ
    构建完成后：
    估值指标如PE,PB可通过 .add_valInd()添加
    """
    def __init__(self,
                 startDate = None, endDate= None, secuTicker=None, indexCode=None,
                 filename=None):
        self.startDate = None               # 起始日期
        self.endDate = None                 # 结束日期
        self.dates = None                   # 交易日期
        
        self.secuMain = None                # 股票信息
        self.secuTicker = None              # 股票代码
        
        self.data = None                    # 股票池日行情数据(不含benchmark)
        self.price_data = None              # 股票价格数据
        self.sector_data = None             # 行业类别数据
        self.valInd_data = None             # 估值指标
        self.indexPrice_data = None           # 指数行情
        self.indexPool = None               # 指数成分股信息纳入剔除

        if filename:
            self.load(filename)
            self.engine = pymysql.connect(host=config['host'], port = config['port'],
                                   user=config['user'],password=config['password'],
                                   db=config['db'],charset=config['charset'])
            # self.engine = create_engine(con)
        else:
            assert startDate and endDate
            assert secuTicker or indexCode
            self.engine = pymysql.connect(host=config['host'], port = config['port'],
                                   user=config['user'],password=config['password'],
                                   db=config['db'],charset=config['charset'])
            # self.engine = create_engine(con)
            self.initialise(startDate, endDate, secuTicker, indexCode)

    def save(self, filename):
        # self.engine.close()
        self.engine = None
        file = open(filename, mode='wb')
        pickle.dump(self, file)
        file.close()

    def load(self, filename):
        file = open(filename, mode='rb')
        self.__dict__ = pickle.load(file, encoding='bytes').__dict__
        file.close()

    def initialise(self, startDate, endDate, secuTicker=None, indexCode=None):
        self.startDate = startDate
        self.endDate = endDate

        #Stock information
        if indexCode:
            self.get_IndexOHLCV(indexCode)
            InnerCode = self.get_indexPool(indexCode)
            self.secuMain = self.get_secuMain(InnerCode=InnerCode)
            self.secuTicker = list(self.secuMain['Ticker'])
        else:
            self.secuTicker = secuTicker
            self.secuMain = self.get_secuMain(secuTicker=secuTicker)

    def read_sql(self, tablename, columns, option):
        sql = "SELECT %s FROM %s %s" % (columns, tablename, option)
        return pd.read_sql(sql, con=self.engine)

    def get_secuMain(self, secuTicker=None, InnerCode=None, columns='*'):
        tablename = 'secuMain'

        # Given InnerCode
        if InnerCode:
            InnerCode = ','.join(list(map(str, InnerCode)))
            option = "WHERE InnerCode in (%s)" % (InnerCode)
            df = self.read_sql(tablename, columns, option)
            df['StrCode'] = df['SecuCode'].apply(str)
            df.loc[df['SecuMarket'] == 83, 'Market'] = 'SH'
            df.loc[df['SecuMarket'] == 90, 'Market'] = 'SZ'
            df['Ticker'] = df[['StrCode', 'Market']].apply(lambda x: '.'.join(x), axis=1)
            df.drop(['StrCode', 'Market'], axis=1, inplace=True)
            return df
        else:
            # Given Ticker
            SecuCodes = pd.DataFrame({'Ticker': secuTicker})
            SecuCodes[['SecuCode', 'SecuMarket']] = pd.DataFrame(list(map(lambda x: x.split(sep='.'), secuTicker)))
            SecuCodes.loc[SecuCodes['SecuMarket'] == 'SH', 'SecuMarket'] = 83
            SecuCodes.loc[SecuCodes['SecuMarket'] == 'SZ', 'SecuMarket'] = 90

            SecuStr = []
            for i in SecuCodes['SecuMarket'].unique():
                strTmp = ','.join(list(SecuCodes.loc[SecuCodes['SecuMarket'] == i, 'SecuCode']))
                SecuStr.append('(SecuMarket = %d AND SecuCode in (%s))' % (i, strTmp))
            SecuStr = ' OR '.join(SecuStr)
            option = 'WHERE ' + SecuStr

            df = self.read_sql(tablename, columns, option)
            df['SecuMarket'] = df['SecuMarket'].astype(object)
            df = SecuCodes.merge(df, on=['SecuCode', 'SecuMarket'])
            return df

    def get_Data(self):
        self.price_data = self.get_OHLCV()      # Price data
        self.sector_data = self.get_SWSector()  # Sector data

        #Merge data
        tmp = pd.merge(self.price_data[['Ticker', 'TradingDay']], self.sector_data, left_on=['Ticker', 'TradingDay'],
                      right_on=['Ticker', 'StartDate'], how='outer')
        tmp.loc[tmp['TradingDay'].isna(), 'TradingDay'] = tmp.loc[tmp['TradingDay'].isna(), 'StartDate']
        tmp = tmp.sort_values(['Ticker', 'TradingDay'])
        tmp['Ticker1'] = tmp['Ticker']
        tmp = tmp.groupby('Ticker1').fillna(method='pad')
        tmp = tmp.loc[self.price_data.index]
        df = self.price_data.merge(tmp, on=['Ticker', 'TradingDay'])
        self.data = df
        self.dates = self.data['TradingDay'].unique()
        return

    def get_SWSector(self):
        tablename = 'lc_exgindustry'
        columns = 'CompanyCode,Standard,FirstIndustryCode,SecondIndustryCode,ThirdIndustryCode,FourthIndustryCode'

        CompanyCodes = ','.join(list(self.secuMain['CompanyCode'].astype('str')))

        df09 = pd.DataFrame()
        if self.startDate < '2014-01-01':
            option = "WHERE CompanyCode IN (%s) AND Standard = 9 AND InfoPublDate < '2014-01-01'" % (CompanyCodes)
            df09 = self.read_sql(tablename,columns,option)
            df09['StartDate'] = pd.to_datetime(self.startDate)
            df09['EndDate'] = pd.to_datetime('2014-01-01') - pd.Timedelta(days=1)

        df24 = pd.DataFrame()
        if self.endDate >= '2014-01-01':
            option = "WHERE CompanyCode IN (%s) AND Standard = 24" % (CompanyCodes)
            df24 = self.read_sql(tablename,columns,option)
            df24['StartDate'] = pd.to_datetime('2014-01-01')
            df24['EndDate'] = pd.to_datetime(self.endDate)

        df = pd.concat([df09, df24])
        df = self.secuMain[['Ticker', 'CompanyCode']].merge(df, on='CompanyCode')
        df.sort_values(by=['CompanyCode','StartDate'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def get_OHLCV(self, useAdjusted='TRUE'):
        ### GET raw price data ###
        tablename = 'qt_dailyquote'
        columns = 'InnerCode, TradingDay, OpenPrice, HighPrice, LowPrice, ClosePrice, TurnoverVolume, TurnoverValue'
        InnerCodes = ','.join(list(self.secuMain['InnerCode'].astype('str')))
        option = "WHERE InnerCode in (%s) AND TradingDay >= '%s' AND TradingDay <='%s'" % (InnerCodes, self.startDate, self.endDate)
        raw_price = self.read_sql(tablename, columns, option)
        raw_price.sort_values(by=['InnerCode', 'TradingDay'], inplace=True)
        raw_price.reset_index(drop=True, inplace=True)

        ### GET adjusting factor ###
        tablename = 'qt_adjustingfactor'
        columns = 'InnerCode, ExDiviDate,RatioAdjustingFactor'
        option = "WHERE InnerCode in (%s)" % InnerCodes
        adjfacor_df = self.read_sql(tablename,columns,option)

        #Fill NA
        adjfacor_df.rename({'ExDiviDate': 'TradingDay'},axis=1,inplace=True)
        df = raw_price.merge(adjfacor_df, left_on=['InnerCode','TradingDay'], right_on=['InnerCode','TradingDay'], how='outer')
        df.sort_values(by=['InnerCode', 'TradingDay'], inplace=True)
        df['RatioAdjustingFactor'] = df.groupby('InnerCode')['RatioAdjustingFactor'].fillna(method='pad')
        df['RatioAdjustingFactor'].fillna(1, inplace=True)
        df = df.loc[raw_price.index]
        df['RatioAdjustingFactor'] = df.groupby('InnerCode')['RatioAdjustingFactor'].apply(lambda x: x / x.iloc[-1])

        if useAdjusted == 'TRUE':
            df.OpenPrice = df.OpenPrice * df.RatioAdjustingFactor
            df.HighPrice = df.HighPrice * df.RatioAdjustingFactor
            df.LowPrice = df.LowPrice * df.RatioAdjustingFactor
            df.ClosePrice = df.ClosePrice * df.RatioAdjustingFactor

        df = df.merge(self.secuMain[['Ticker', 'InnerCode']], on='InnerCode')
        return df

    def get_indexPool(self, indexCode):

        tablename = 'lc_indexcomponent'
        columns = 'SecuInnerCode, InDate, OutDate'
        option = "WHERE IndexInnerCode = %s" % (indexCode)

        indexPool = self.read_sql(tablename, columns, option)
        indexPool.fillna(pd.Timestamp.now(), inplace=True)
        indexPool = indexPool.loc[(indexPool['InDate'] <= self.endDate) & (indexPool['OutDate'] > self.startDate)]
        self.indexPool = indexPool
        return list(indexPool['SecuInnerCode'].unique())

    def mat_indexPool(self):
        df = self.indexPool
        df.rename(index=str, columns={"SecuInnerCode": "InnerCode"}, inplace=True)
        df = df.merge(self.secuMain[['InnerCode', 'Ticker']], how='left', on='InnerCode')
        df = df.melt(id_vars=['Ticker'], value_vars=['InDate', 'OutDate'], var_name='status', value_name='Date')
        # 按照data排序
        df.sort_values(by='Date', inplace=True)
        # 去掉nan的行，以1，0替代'InDate'，'OutDate'
        # df = df.dropna()
        df.set_index('Date', inplace=True)
        df.replace('InDate', 1, inplace=True)
        df.replace('OutDate', 0, inplace=True)
        #    df长转宽
        df = df.pivot_table(index=['Date'], columns=["Ticker"], values=["status"])
        df.fillna(method='ffill', axis=0, inplace=True)
        df.fillna(0, inplace=True)
        df.columns = df.columns.droplevel(level=0)
        df.index = df.index.date
        return df

    def to_mat(self, variable="ClosePrice"):
        df = self.data.pivot_table(index=['TradingDay'], columns=["Ticker"], values=[variable])
        df.columns = df.columns.droplevel(level=0)
        df.index = df.index.date
        return df

    def get_IndexOHLCV(self,indexCode):
        tablename = 'qt_indexquote'
        columns = 'InnerCode, TradingDay, OpenPrice, HighPrice, LowPrice, ClosePrice, TurnoverVolume, TurnoverValue'
        option = "WHERE InnerCode in (%s) AND TradingDay >= '%s' AND TradingDay <='%s'" % (indexCode, self.startDate, self.endDate)
        self.indexPrice_data = self.read_sql(tablename, columns, option)

    def get_ValuationIndices(self):
        tablename = 'lc_dindicesforvaluation'
        columns = 'InnerCode,TradingDay,PE,PB,PCF,PS,DividendRatio,PSTTM,TotalMV,NegotiableMV,' \
                  'PELYR,PCFTTM,PCFS,PCFSTTM,DividendRatioLYR,EnterpriseValueW,EnterpriseValueN'
        InnerCodes = ','.join(list(self.secuMain['InnerCode'].astype('str')))
        option = "WHERE InnerCode in (%s) AND TradingDay >= '%s' AND TradingDay <='%s'" % (InnerCodes, self.startDate, self.endDate)
        df = self.read_sql(tablename,columns,option)
        return df

    def add_valInd(self):
        #Valuation Indices data
        self.valInd_data = self.get_ValuationIndices()
        self.data = self.data.merge(self.valInd_data, on=['InnerCode', 'TradingDay'])
        return