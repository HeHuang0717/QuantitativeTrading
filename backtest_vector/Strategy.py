from functools import reduce

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from math import ceil


class strat:
    def __init__(self, data, freq='week'):
        self.data = data
        self.commission = {'cps': 0, 'fixed': 0, 'percentage': 0}
        self.freq = freq
        self.period_ends = self.get_periodEnds()
        self.cash = data.cash.loc(axis=0)[:, self.period_ends]
        self.bmrk = data.benchmark.loc(axis=0)[:, self.period_ends]
        self.closes = data.closes.loc[self.period_ends]
        self.filterList = {}
        self.factorList = {}
        self.resultList = {}

    def setCommission(self, cps=None, fixed=None, percentage=None):
        self.commission['cps'] = cps
        self.commission['fixed'] = fixed
        self.commission['percentage'] = percentage

    def setFreq(self, freq):
        self.freq = freq
        self.period_ends = self.get_periodEnds()
        self.cash = self.data.cash.loc(axis=0)[:, self.period_ends]
        self.bmrk = self.data.benchmark.loc(axis=0)[:, self.period_ends]
        self.closes = self.data.closes.loc[self.period_ends]

    def setFilter(self, mat, name):
        assert list(mat.index) == list(self.period_ends)
        self.filterList[name] = mat

    def setFactor(self, mat, name):
        assert list(mat.index) == list(self.period_ends)
        self.factorList[name] = mat

    def get_periodEnds(self):
        if isinstance(self.freq, int):
            period_ends = self.data.Dates[range(0, len(self.data.Dates), self.freq)]
        elif self.freq == 'week':
            series = pd.Series(self.data.Dates)
            period_ends = np.array(series[(series - series.shift(1)) != pd.Timedelta(days=1)])
        elif self.freq == 'month':
            series = pd.Series(self.data.Dates)
            period_ends = np.array(series[series.dt.month != series.shift(-1).dt.month])
        else:  # elif self.freq == 'day':
            period_ends = self.data.Dates
        return period_ends

    def run_factor(self, fname, layerMat=None, quantile=5):
        filters = reduce(lambda x, y: x * y, [self.filterList[i] for i in self.filterList])

        factor = self.factorList[fname]

        df = factor * filters
        df = df.stack().to_frame(name=fname)

        if layerMat is None:
            df['quantile'] = df.groupby('TradingDay')[fname].apply(lambda x, q: (x.rank() / x.count() * q).apply(ceil),
                                                                   quantile)
        else:
            df['layer'] = layerMat.stack()
            df['quantile'] = df.groupby(['TradingDay', 'layer'])[fname].apply(
                lambda x, q: (x.rank() / x.count() * q).apply(ceil), quantile)

        df['close'] = self.closes.stack()
        df['next_ret'] = df['close'].groupby('Ticker').pct_change().shift(-1)

        factor_quantiles = df.groupby(['TradingDay', 'quantile'])[fname].mean().unstack()
        winning_prob = df.groupby(['TradingDay', 'quantile'])['next_ret'].apply(
            lambda x: (x > 0).mean()).unstack().shift()
        ret_quantiles = df.groupby(['TradingDay', 'quantile'])['next_ret'].mean().unstack().shift()
        IC = df[[fname, 'next_ret']].groupby('TradingDay').apply(lambda x: x.corr(method='pearson').iloc[0, 1])
        RankIC = df[[fname, 'next_ret']].groupby('TradingDay').apply(lambda x: x.corr(method='spearman').iloc[0, 1])

        result = {'ret_quantiles': ret_quantiles,
                  'winning_prob': winning_prob,
                  'factor_quantiles': factor_quantiles,
                  'IC': IC,
                  'RankIC': RankIC}

        self.resultList[fname] = result

    def run_factor_plot(self, fname):
        ret = self.resultList[fname]['ret_quantiles']
        (ret + 1).cumprod().plot()
        plt.show()
