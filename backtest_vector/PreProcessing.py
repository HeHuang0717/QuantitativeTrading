import numpy as np
import pandas as pd


class prep:
    # 数据预处理
    #
    def __init__(self, jydb, moneyrate=0.01 / 252):
        self.db = jydb
        self.moneyrate = moneyrate
        self.SecuTicker = jydb.SecuTicker
        self.Dates = jydb.price_data['TradingDay'].unique()
        self.data = jydb.data.set_index(['TradingDay', 'Ticker']).sort_index()
        self.closes = None
        self.cash = None
        self.benchmark = None
        self.inPool_dummy = None

    def prep_data(self):
        self.clean_price()
        self.closes = self.get_mat('ClosePrice')
        self.cash = self.get_cash()
        self.benchmark = self.get_benchmark(self.db.index_price)
        self.inPool_dummy = self.get_inPool_dummy(self.db.index_pool)
        return

    def clean_price(self):
        df = self.data
        df.loc[(df['ClosePrice']<=0) | (df['ClosePrice']==np.inf)] = np.nan
        df['ClosePrice'] = df.groupby('Ticker')['ClosePrice'].fillna(method='pad')
        df.loc[df['OpenPrice'] == 0, 'OpenPrice'] = df['ClosePrice']
        df.loc[df['HighPrice'] == 0, 'HighPrice'] = df['ClosePrice']
        df.loc[df['LowPrice'] == 0, 'LowPrice'] = df['ClosePrice']

    def set_moneyrate(self, moneyrate):
        self.moneyrate = moneyrate
        self.cash = self.get_cash()

    def get_cash(self):
        df_index = pd.MultiIndex.from_product([['Cash'], self.Dates], names=('Ticker', 'TradingDay'))
        df = pd.DataFrame(index=df_index)
        df['ClosePrice'] = (1 + np.array([self.moneyrate] * len(df_index))).cumprod()
        # df['Volume'] = 1e10
        return df

    def get_benchmark(self, index_price):
        if index_price is not None:
            df = index_price.copy()
            df['Ticker'] = 'Benchmark'
            df.drop('InnerCode', axis=1, inplace=True)
            df.set_index(['Ticker', 'TradingDay'], inplace=True)
            return df
        else:
            return None

    def get_inPool_dummy(self, IndexPool):
        df = IndexPool.melt(id_vars=['SecuInnerCode'], value_vars=['InDate', 'OutDate'], var_name='status',
                            value_name='Date')
        df = df.replace('InDate', 1)
        df = df.replace('OutDate', 0)
        df.rename({'SecuInnerCode': 'InnerCode', 'Date': 'TradingDay'}, axis=1, inplace=True)

        tmp = self.data[['InnerCode']]
        tmp.reset_index(inplace=True)

        dummy = tmp.merge(df, on=['InnerCode', 'TradingDay'], how='outer')
        dummy.sort_values(by=['InnerCode', 'TradingDay'], inplace=True)
        dummy['status'] = dummy.groupby('InnerCode')['status'].fillna(method='pad')
        dummy = dummy.loc[tmp.index]
        dummy.drop('InnerCode', axis=1, inplace=True)
        dummy.set_index(['Ticker', 'TradingDay'], inplace=True)

        dummy = dummy.unstack().T
        dummy.fillna(0, inplace=True)
        dummy.reset_index(0, drop=True, inplace=True)
        dummy.replace(0, np.nan, inplace=True)
        return dummy

    def get_nonst_mat(self):
        '''
        非(ST|PT|退)为1， otherwise 为 nan
        '''
        df = self.db.specialtrade.copy()
        df.SpecialTradeType = 0
        df.SpecialTradeType[df.SecurityAbbr.str.contains("ST|PT|退") == 0] = 1
        df.rename({'SpecialTradeTime': 'TradingDay', 'SpecialTradeType': 'st'}, axis=1, inplace=True)
        tmp = self.data[['InnerCode']]
        tmp.reset_index(inplace=True)

        dummy = tmp.merge(df, on=['InnerCode', 'TradingDay'], how='outer')
        dummy.sort_values(by=['InnerCode', 'TradingDay'], inplace=True)
        dummy['st'] = dummy.groupby('InnerCode')['st'].fillna(method='pad')
        dummy = dummy.loc[tmp.index]
        dummy.drop('InnerCode', axis=1, inplace=True)
        dummy.set_index(['Ticker', 'TradingDay'], inplace=True)

        dummy = dummy.unstack().T
        dummy.fillna(1, inplace=True)
        dummy.reset_index(0, drop=True, inplace=True)
        dummy.replace(0, np.nan, inplace=True)
        return dummy

    def get_nonipo_mat(self):
        '''
        非次新股（上市6个月）为1， otherwise 为 nan
        '''

        pass


    def get_mat(self, colname):
        df = self.data[colname]
        df = df.unstack()
        return df


# Some Functions
def buildFilter(factor_mat, positive=False, method=None):
    # method={'type':,'num':,'dir':}
    # type includes ['n','p','q']
    # dir includes ['up','low']
    mat = pd.DataFrame(1, columns=factor_mat.columns, index=factor_mat.index)
    mat[factor_mat.isna()] = np.nan

    if positive:
        mat[factor_mat <= 0] = np.nan

    if method is not None:
        if method['dir'] == 'low':
            factor_mat = -factor_mat

        matrank = factor_mat.rank(axis=1)

        if method['type'] == 'n':
            n_top = method['num']
        elif method['type'] == 'q':
            nFinite = factor_mat.count(axis=1)
            n_top = nFinite * method['num']
        elif method['type'] == 'p':
            p_mat = factor_mat.apply(lambda x: x.sort_values(ascending=False).cumsum(), axis=1).T / factor_mat.sum()
            n_top = p_mat[p_mat < method['num']].count()

        mat.T[matrank.T > n_top] = np.nan
    return mat


def sector_fun(factorMat, sectorMat, fun='mean'):
    df = factorMat.stack().to_frame(name='factor')
    df['sector'] = sectorMat.stack()
    df = df.reset_index().set_index(['TradingDay', 'sector'])
    sectormean = df.groupby(['TradingDay', 'sector'])['factor'].mean()
    sectormean = sectormean.reset_index().set_index(['TradingDay', 'sector'])
    df['sectormean'] = sectormean
    df = df.reset_index().set_index(['TradingDay', 'Ticker'])
    return df['sectormean'].unstack()
