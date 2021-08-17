# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 09:49:41 2019

@author: Shangyi
"""
import matplotlib.pyplot as plt
def plot_data(all_data,excess,IC,testfactor_name):
    plot=all_data.plot(legend=True,title="%s factor net value"%testfactor_name).legend(bbox_to_anchor=(1.0, 0.5))
    fig = plot.get_figure()
    fig.savefig("%s factor net value.png"%testfactor_name,dpi=800, bbox_inches = 'tight')
    
    plot=excess.plot(legend=True,title="%s factor excess income"%testfactor_name)
    fig = plot.get_figure()
    fig.savefig("%s factor excess income.png"%testfactor_name,dpi=800, bbox_inches = 'tight')
    
    IC2=IC.index.strftime('%y-%m-%d')
    fig = plt.figure(figsize=(10,3)) 
    IC.index = list(IC2)
    plot=IC.plot(kind="bar",title="IC Sequence")
    fig.savefig("%s IC Sequence.png"%testfactor_name,dpi=800, bbox_inches = 'tight')
def year_return(data,benchmark,testfactor_name):
    data_in=data

    year_return=(1+data_in.mean()).apply(lambda x:pow(x,12))-1   

    year_volatility=data_in.std()*np.sqrt(12)    
    sharpe=year_return/year_volatility   
    in_value=(data_in+1).cumprod()   
    max_drawdown=((in_value.cummax()-in_value)/in_value.cummax()).max()   
    excess_return=(data.T-benchmark).T    
    excess_year_return=(1+excess_return.mean()).apply(lambda x:pow(x,12))-1 
    excess_return_volatility=excess_return.std()*np.sqrt(12) 
    benchmark_year_return=pow((1+benchmark.T.mean()),12)-1
    InformationRate=(year_return-benchmark_year_return)/excess_return_volatility
    win_rate = (excess_return[excess_return>0]).count()/(excess_return[excess_return>0].shape[0])  
    excess_year_return_drawdown = (((excess_return+1).cumprod().cummax()-(excess_return+1).cumprod())/(excess_return+1).cumprod().cummax()).max()   
    Performance_Analysis=pd.DataFrame([year_return,year_volatility,sharpe,max_drawdown,excess_year_return,excess_return_volatility,InformationRate, win_rate,excess_year_return_drawdown]).T
#    Performance_Analysis.columns=['year_return','year_volatility','sharpe','max_drawdown','excess_year_return','excess_return_volatility','InformationRate', 'win_rate','excess_return_drawdown']
    Performance_Analysis.columns=['annualized return', 'annualized volatility', 'sharp ratio', 'maximum drawdown', 'annualized excess return rate', 'annualized excess return rate volatility', 'information ratio', ' win rate ', 'maximum drawdown of excess returns']
#    Performance_Analysis.columns=['年化收益率', '年化波动率', '夏普比率', '最大回撤','年化超额收益率','超额收益年化波动率', '信息比率','相对基准月胜率','超额收益最大回撤']  
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.bar(range(1,len(year_return)+1), year_return,label='Annualized Return')  
    ax2 = ax1.twinx() 
    ax2.plot(range(1,len(sharpe)+1), sharpe, 'grey',label='Sharpe Rate')   
    ax2.plot(range(1,len(InformationRate)+1), InformationRate, 'y',label='Information Rate')  
    plt.legend()
    plt.title('%s  Annualized Return'%testfactor_name)
    Performance_Analysis.to_csv('%s绩效.csv'%testfactor_name)
    fig.savefig("%s IC Sequence.png"%testfactor_name,dpi=800, bbox_inches = 'tight')