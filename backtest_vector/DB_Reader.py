import os
import pickle
import warnings

import pandas as pd
import pymysql

warnings.filterwarnings("ignore")

config = {'host': '192.168.18.16',
          'port': 3307,
          'user': 'yxfan',
          'password': 'yxfan',
          'db': 'jydb',
          'charset': 'utf8'
          }
con = 'mysql+pymysql://%s:%s@%s:%d/%s' % \
      (config['user'], config['password'], config['host'], config['port'], config['db'])


class db_Secu:
    # 给定股票代码列表或指数代码，提取历史上某一段时间的价格数据和行业数据
    # 指数代码{'沪深300':3145,'中证500':4978}
    # 股票代码格式：600000.SZ
    # 构建完成后：
    # 估值指标如PE,PB可通过 .add_valInd()添加
    def __init__(self, startDate=None, endDate=None, SecuTicker=None, IndexCode=None,
                 filename=None):
        self.startDate = startDate  # 起始日期
        self.endDate = endDate  # 结束日期
        self.IndexCode = IndexCode
        self.SecuTicker = SecuTicker  # 股票代码
        self.secumain = None  # 股票信息(不含benchmark)
        self.data = None  # 全部数据(不含benchmark)
        self.price_data = None  # 股票行情数据
        self.sector_data = None  # 行业类别数据
        self.valueIndice_data = None  # 估值指标
        self.index_price = None  # 指数行情
        self.index_pool = None  # 指数成分股信息纳入剔除

        if filename:
            if os.path.exists(filename):
                print("File exists. Load data from local.")
                self.load(filename)
                self.engine = pymysql.connect(host=config['host'], port=config['port'],
                                              user=config['user'], password=config['password'],
                                              db=config['db'], charset=config['charset'])
                return

        print("File does not exist. Read data from database.")
        assert startDate and endDate
        assert SecuTicker or IndexCode
        self.engine = pymysql.connect(host=config['host'], port=config['port'],
                                      user=config['user'], password=config['password'],
                                      db=config['db'], charset=config['charset'])
        self.initialise(SecuTicker, IndexCode)

    def save(self, filename):
        self.engine = None
        file = open(filename, mode='wb')
        pickle.dump(self, file)
        file.close()

    def load(self, filename):
        file = open(filename, mode='rb')
        self.__dict__ = pickle.load(file, encoding='bytes').__dict__
        file.close()

    def initialise(self, secuTicker=None, indexCode=None):
        # Stock information
        if self.IndexCode:
            self.index_price = self.get_IndexOHLCV(indexCode)
            InnerCode = self.get_IndexPool(indexCode)
            self.secumain = self.get_SecuMain(InnerCode=InnerCode)
            self.SecuTicker = list(self.secumain['Ticker'])
        else:
            self.secumain = self.get_SecuMain(SecuTicker=secuTicker)

    def get_Data(self):
        # Price data
        self.price_data = self.get_OHLCV()
        # Sector data
        self.sector_data = self.get_SWSector()
        # Merge data
        self.merge_data()
        self.add_valInd()

    def read_sql(self, tablename, columns, option):
        sql = "SELECT %s FROM %s %s" % (columns, tablename, option)
        return pd.read_sql(sql, con=self.engine)

    def get_SecuMain(self, SecuTicker=None, InnerCode=None, columns='*'):
        tablename = 'secumain'

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
            SecuCodes = pd.DataFrame({'Ticker': SecuTicker})
            SecuCodes[['SecuCode', 'SecuMarket']] = pd.DataFrame(list(map(lambda x: x.split(sep='.'), SecuTicker)))
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

    def get_SWSector(self, columns='*'):
        tablename = 'lc_exgindustry'
        columns = 'CompanyCode,Standard,FirstIndustryCode,SecondIndustryCode,ThirdIndustryCode,FourthIndustryCode'

        CompanyCodes = ','.join(list(self.secumain['CompanyCode'].astype('str')))

        df09 = pd.DataFrame()
        if self.startDate < '2014-01-01':
            option = "WHERE CompanyCode IN (%s) AND Standard = 9 AND InfoPublDate < '2014-01-01'" % (CompanyCodes)
            df09 = self.read_sql(tablename, columns, option)
            df09['StartDate'] = pd.to_datetime(self.startDate)
            df09['EndDate'] = pd.to_datetime('2014-01-01') - pd.Timedelta(days=1)

        df24 = pd.DataFrame()
        if self.endDate >= '2014-01-01':
            option = "WHERE CompanyCode IN (%s) AND Standard = 24" % (CompanyCodes)
            df24 = self.read_sql(tablename, columns, option)
            df24['StartDate'] = pd.to_datetime('2014-01-01')
            df24['EndDate'] = pd.to_datetime(self.endDate)

        df = pd.concat([df09, df24])
        df = self.secumain[['Ticker', 'CompanyCode']].merge(df, on='CompanyCode')
        df.sort_values(by=['CompanyCode', 'StartDate'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def get_OHLCV(self, columns='*', useAdjusted='TRUE'):
        ### GET raw price data ###
        tablename = 'qt_dailyquote'
        columns = 'InnerCode, TradingDay, OpenPrice, HighPrice, LowPrice, ClosePrice, TurnoverVolume, TurnoverValue'
        InnerCodes = ','.join(list(self.secumain['InnerCode'].astype('str')))
        option = "WHERE InnerCode in (%s) AND TradingDay >= '%s' AND TradingDay <='%s'" % (
            InnerCodes, self.startDate, self.endDate)
        raw_price = self.read_sql(tablename, columns, option)
        raw_price.sort_values(by=['InnerCode', 'TradingDay'], inplace=True)
        raw_price.reset_index(drop=True, inplace=True)

        ### GET adjusting factor ###
        tablename = 'qt_adjustingfactor'
        columns = 'InnerCode, ExDiviDate,RatioAdjustingFactor'
        option = "WHERE InnerCode in (%s)" % (InnerCodes)
        adjfacor_df = self.read_sql(tablename, columns, option)

        # Fill NA
        adjfacor_df.rename({'ExDiviDate': 'TradingDay'}, axis=1, inplace=True)
        df = raw_price.merge(adjfacor_df, left_on=['InnerCode', 'TradingDay'], right_on=['InnerCode', 'TradingDay'],
                             how='outer')
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

        df = df.merge(self.secumain[['Ticker', 'InnerCode']], on='InnerCode')
        return df

    def get_IndexPool(self, IndexCode):

        tablename = 'lc_indexcomponent'
        columns = 'SecuInnerCode, InDate, OutDate'
        option = "WHERE IndexInnerCode = %s" % (IndexCode)

        IndexPool = self.read_sql(tablename, columns, option)
        IndexPool = IndexPool.fillna(pd.Timestamp.now())
        IndexPool = IndexPool.loc[(IndexPool['InDate'] <= self.endDate) & (IndexPool['OutDate'] > self.startDate)]
        self.index_pool = IndexPool
        return list(IndexPool['SecuInnerCode'].unique())

    def get_specialtrade(self):
        tablename = 'lc_specialtrade'
        columns = 'InnerCode,SecurityAbbr,SpecialTradeType,SpecialTradeTime'
        InnerCodes = ','.join(list(self.secumain['InnerCode'].astype('str')))
        option = "WHERE InnerCode in (%s)" % (InnerCodes)
        self.specialtrade = self.read_sql(tablename, columns, option)

    def get_IndexOHLCV(self, IndexCode):
        tablename = 'qt_indexquote'
        columns = 'InnerCode, TradingDay, OpenPrice, HighPrice, LowPrice, ClosePrice, TurnoverVolume, TurnoverValue'
        option = "WHERE InnerCode in (%s) AND TradingDay >= '%s' AND TradingDay <='%s'" % (
            IndexCode, self.startDate, self.endDate)
        index_price = self.read_sql(tablename, columns, option)
        return index_price

    def get_ValuationIndices(self):
        tablename = 'lc_dindicesforvaluation'
        columns = 'InnerCode,TradingDay,PE,PB,PCF,PS,DividendRatio,PSTTM,TotalMV,NegotiableMV,' \
                  'PELYR,PCFTTM,PCFS,PCFSTTM,DividendRatioLYR,EnterpriseValueW,EnterpriseValueN'
        InnerCodes = ','.join(list(self.secumain['InnerCode'].astype('str')))
        option = "WHERE InnerCode in (%s) AND TradingDay >= '%s' AND TradingDay <='%s'" % (
            InnerCodes, self.startDate, self.endDate)
        df = self.read_sql(tablename, columns, option)
        return df

    def add_valInd(self):
        # Valuation Indices data
        self.valueIndice_data = self.get_ValuationIndices()
        self.data = self.data.merge(self.valueIndice_data, on=['InnerCode', 'TradingDay'])
        return

    def merge_data(self):
        tmp = pd.merge(self.price_data[['Ticker', 'TradingDay']], self.sector_data, left_on=['Ticker', 'TradingDay'],
                       right_on=['Ticker', 'StartDate'], how='outer')
        tmp.loc[tmp['TradingDay'].isna(), 'TradingDay'] = tmp.loc[tmp['TradingDay'].isna(), 'StartDate']
        tmp = tmp.sort_values(['Ticker', 'TradingDay'])
        tmp['Ticker1'] = tmp['Ticker']
        tmp = tmp.groupby('Ticker1').fillna(method='pad')
        tmp = tmp.loc[self.price_data.index]
        self.data = self.price_data.merge(tmp, on=['Ticker', 'TradingDay'])
        return
