import os
import pandas as pd
import global_setting.global_dic as glv
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
source=glv.get('mode')
config_path=glv.get('config_path')
class data_prepare:
    def __init__(self,start_date,end_date):
        self.start_date=start_date
        self.end_date=end_date
    def index_name_mapping(self,index_name):
        if index_name=='沪深300':
            return '000300.SH'
        elif index_name=='中证2000':
            return '932000.CSI'
        elif index_name=='中证500':
            return '000905.SH' 
        elif index_name=='中证1000':
            return '000852.SH'
        elif index_name=='中证800':
            return '932000.CSI'
        elif index_name=='中证A500':
            return '000510.CSI'
        elif index_name=='上证50':
            return '000016.SH'
        elif index_name=='国证2000':
            return '399303.SZ'
        else:
            raise ValueError('index_name must be 沪深300 or 中证2000 or 中证500 or 中证1000 or 中证800 or 中证A500 or 上证50 or 国证2000')
    def index_name_mapping2(self,index_code):
        if index_code=='000300.SH':
            return '沪深300'
        elif index_code=='932000.CSI':
            return '中证2000'
        elif index_code=='000905.SH':
            return '中证500'
        elif index_code=='000852.SH':
            return '中证1000'
        elif index_code=='000510.CSI':
            return '中证A500'
        elif index_code=='000510.SH':
            return '中证A500'
        elif index_code=='000016.SH':
            return '上证50'
        elif index_code=='399303.SZ':
            return '国证2000'
        else:
            raise ValueError('index_code must be 沪深300 or 中证2000 or 中证500 or 中证1000 or 中证800 or 中证A500 or 上证50 or 国证2000')
    #shibor
    def raw_shibor(self,period):
        inputpath = glv.get('raw_Shibor')
        if source=='sql':
            inputpath=str(inputpath)+f" Where name='Shibor_{period}' And type='close' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1=gt.data_getting(inputpath,config_path)
        df1=df1[['valuation_date','value']]
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        signal_name='Shibor_'+period
        df1.columns=['valuation_date',signal_name]
        if signal_name not in df1.columns:
            raise ValueError('period must be 2W or 9W')
        df1=df1[['valuation_date',signal_name]]
        return df1
    #国债
    def raw_bond(self,period):
        inputpath = glv.get('raw_Bond')
        if source == 'sql':
            inputpath = str(inputpath) + f" Where name='CGB_{period}' And type='close' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1 = df1[['valuation_date', 'value']]
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        signal_name='CGB_'+period
        df1.columns = ['valuation_date', signal_name]
        if signal_name not in df1.columns:
            raise ValueError('period must be 3Y or 10Y')
        df1=df1[['valuation_date',signal_name]]
        return df1
    #国开债
    def raw_ZZGK(self,period):
        inputpath_ZZGK = glv.get('raw_ZZGK')
        if source == 'sql':
            inputpath = str(inputpath_ZZGK) + f" Where name='CDBB_{period}' And type='close' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1 = df1[['valuation_date', 'value']]
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        signal_name='CDBB_'+period
        df1.columns = ['valuation_date', signal_name]
        if signal_name not in df1.columns:
            raise ValueError('period must be 3M or9M or 1Y or 5Y or 10Y')
        df1=df1[['valuation_date',signal_name]]
        return df1
    #中债中短
    def raw_ZZZD(self,period):
        inputpath_ZZZD= glv.get('raw_ZZZD')
        if source == 'sql':
            inputpath = str(inputpath_ZZZD) + f" Where name='CMTN_{period}' And type='close' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1 = df1[['valuation_date', 'value']]
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        signal_name='CMTN_'+period
        df1.columns = ['valuation_date', signal_name]
        if signal_name not in df1.columns:
            raise ValueError('period must be 9M or 3M or 5Y ')
        df1=df1[['valuation_date',signal_name]]
        return df1
    #M1 and M2
    def raw_M1M2(self,signal_name):
        inputpath = glv.get('raw_M1M2')
        if source == 'sql':
            inputpath = str(inputpath ) + f" Where name='{signal_name}' And type='close' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1 = df1[['valuation_date', 'value']]
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df1.columns = ['valuation_date', signal_name]
        if signal_name not in df1.columns:
            raise ValueError('type must be M1 or M2')
        df1=df1[['valuation_date',signal_name]]
        return df1
    #美国方面
    #美元指数
    def raw_usdx(self):
        inputpath1 = glv.get('raw_USDX')
        if source == 'sql':
            inputpath = str(inputpath1) + f" Where name='USDX' And type='close' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1 = df1[['valuation_date', 'value']]
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df1.columns=['valuation_date','USDX']
        return df1
    #风险因子方面：
    def raw_index_earningsyield(self):
        inputpath=glv.get('raw_indexFactor')
        if source == 'sql':
            inputpath = str(inputpath) + f" Where type='earningsyield' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1=df1[['valuation_date','organization','value']]
        df=gt.sql_to_timeseries(df1)
        df['valuation_date']=pd.to_datetime(df['valuation_date'])
        df['valuation_date']=df['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df=df[['valuation_date','hs300','gz2000','zz1000']]
        df_final=df.dropna()
        df_final['difference_earningsyield']=df_final['hs300']-df_final['gz2000']-df_final['zz1000']
        df_final=df_final[['valuation_date','difference_earningsyield']]
        return df_final
    def raw_index_growth(self):
        inputpath=glv.get('raw_indexFactor')
        if source == 'sql':
            inputpath = str(inputpath) + f" Where type='growth' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1 = df1[['valuation_date', 'organization', 'value']]
        df = gt.sql_to_timeseries(df1)
        df['valuation_date']=pd.to_datetime(df['valuation_date'])
        df['valuation_date']=df['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df=df[['valuation_date','hs300','gz2000','zz1000']]
        df_final=df.dropna()
        df_final['difference_Growth']=df_final['hs300']-df_final['gz2000']-df_final['zz1000']
        df_final=df_final[['valuation_date','difference_Growth']]
        return df_final
    def raw_CPI_withdraw(self):
        inputpath = glv.get('raw_CPI')
        if source == 'sql':
            inputpath = str(inputpath) + f" Where name='CPI' And type='close' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1 = df1[['valuation_date', 'value']]
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df1.columns=['valuation_date','CPI']
        return df1
    def raw_PPI_withdraw(self):
        inputpath = glv.get('raw_PPI')
        if source == 'sql':
            inputpath = str(inputpath) + f" Where name='PPI' And type='close' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1 = df1[['valuation_date', 'value']]
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df1.columns=['valuation_date','PPI']
        return df1
    def raw_PMI_withdraw(self):
        inputpath = glv.get('raw_PMI')
        if source == 'sql':
            inputpath = str(inputpath) + f" Where name='PMI' And type='close' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1 = df1[['valuation_date', 'value']]
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df1.columns=['valuation_date','PMI']
        return df1
    #资金因子方面:
    def raw_LHBProportion_withdraw(self):
        inputpath=glv.get('raw_LHBProportion')
        if source == 'sql':
            inputpath = str(inputpath) + f" Where valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1['valuation_date']=pd.to_datetime(df1['valuation_date'])
        df1['valuation_date']=df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df1=df1[['valuation_date','LHBProportion']]
        return df1
    def raw_NetLeverageBuying_withdraw(self):
        inputpath=glv.get('raw_NLBPDifference')
        if source == 'sql':
            inputpath = str(inputpath) + f" Where type='NetLeverageBuying' and organization='NetLeverageAMTProportion_difference' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1 = df1[['valuation_date', 'value']]
        df1['valuation_date']=pd.to_datetime(df1['valuation_date'])
        df1['valuation_date']=df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df1.columns=['valuation_date','NLBP_difference']
        df1['NLBP_difference'] = df1['NLBP_difference'].shift(1)
        df1.dropna(inplace=True)
        return df1
    def raw_LargeOrder_withdraw(self):
        inputpath=glv.get('raw_LargeOrder')
        if source == 'sql':
            inputpath = str(
                inputpath) + f" Where type='LargeOrderInflow' And valuation_date between '{self.start_date}' and '{self.end_date}'"
        df1 = gt.data_getting(inputpath, config_path)
        df1=df1[['valuation_date','organization','value']]
        df1=gt.sql_to_timeseries(df1)
        df1['valuation_date']=pd.to_datetime(df1['valuation_date'])
        df1['valuation_date']=df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        return df1
    #股票方面:
    def raw_stockClose_withdraw(self):
        df1=gt.stockData_withdraw(start_date=self.start_date,end_date=self.end_date,columns=['close'])
        df1=gt.sql_to_timeseries(df1)
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        return df1
    #技术指标方面:
    #指数方面
    def raw_index_volume(self,index_name):
        index_code=self.index_name_mapping(index_name)
        df1=gt.indexData_withdraw(index_code,start_date=self.start_date,end_date=self.end_date,columns=['volume'])
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df1=df1[['valuation_date','volume']]
        df1.columns=['valuation_date',index_name]
        return df1
    def raw_index_turnover(self,index_name):
        index_code=self.index_name_mapping(index_name)
        df1 = gt.indexData_withdraw(index_code, start_date=self.start_date, end_date=self.end_date, columns=['turn_over'])
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df1 = df1[['valuation_date', 'turn_over']]
        df1.columns = ['valuation_date', index_name]
        return df1
    def raw_index_amt(self,index_name):
        index_code = self.index_name_mapping(index_name)
        df1 = gt.indexData_withdraw(index_code, start_date=self.start_date, end_date=self.end_date,
                                    columns=['amt'])
        df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df1 = df1[['valuation_date', 'turn_over']]
        df1.columns = ['valuation_date', index_name]
        return df1
    def index_return_withdraw(self):
        df1 = gt.indexData_withdraw(index_type=None, start_date=self.start_date, end_date=self.end_date,
                                    columns=['pct_chg'])
        print(df1)
        df1['code']=df1['code'].apply(lambda x: self.index_name_mapping2(x))
        df1=gt.sql_to_timeseries(df1)
        # df1['valuation_date'] = pd.to_datetime(df1['valuation_date'])
        # df1['valuation_date'] = df1['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        # df1 = df1[['valuation_date', 'turn_over']]
        # df1.columns = ['valuation_date', index_name]
        return df1
    def index_return_withdraw2(self):
        df_return=gt.timeSeries_index_return_withdraw()
        df_return=df_return[['valuation_date','沪深300','国证2000']]
        df_return[['沪深300','国证2000']]=df_return[['沪深300','国证2000']].astype(float)
        return df_return
    def BankMomentum_withdraw(self):
        inputpath=glv.get('raw_indexFinanceDifference')
        df=gt.readcsv(inputpath)
        df.set_index('valuation_date',inplace=True)
        df=(1+df).cumprod()
        df['difference']=df['finance_return']-df['gz2000_return']
        df.reset_index(inplace=True)
        df=df[['valuation_date','difference']]
        return df


    #target_index
    def target_index(self):
        df_return=self.index_return_withdraw()
        df_return=df_return[['valuation_date','沪深300','中证2000']]
        df_return.set_index('valuation_date',inplace=True)
        df_return=df_return.astype(float)
        df_return=(1+df_return).cumprod()
        df_return['target_index']=df_return['沪深300']/df_return['中证2000']
        df_return.reset_index(inplace=True)
        df_return=df_return[['valuation_date','target_index','沪深300','中证2000']]
        return df_return
    def target_index_candle(self):
        inputpath_close=glv.get('raw_indexClose')
        inputpath_high=glv.get('raw_indexHigh')
        inputpath_low=glv.get('raw_indexLow')
        df_close=gt.readcsv(inputpath_close)
        df_high = gt.readcsv(inputpath_high)
        df_low = gt.readcsv(inputpath_low)
        df_close=df_close[['valuation_date','000300.SH','399303.SZ']]
        df_high = df_high[['valuation_date','000300.SH','399303.SZ']]
        df_low = df_low[['valuation_date','000300.SH','399303.SZ']]
        df_close.columns=['valuation_date', '000300.SH_close', '399303.SZ_close']
        df_high.columns=['valuation_date','000300.SH_high','399303.SZ_high']
        df_low.columns = ['valuation_date', '000300.SH_low', '399303.SZ_low']
        df_hl=df_high.merge(df_low,on='valuation_date',how='left')
        df_final=df_close.merge(df_hl,on='valuation_date',how='left')
        df_final['close']=df_final['000300.SH_close']-df_final['399303.SZ_close']
        df_final['high']=df_final['000300.SH_low']-df_final['399303.SZ_high']
        df_final['low']=df_final['000300.SH_high']-df_final['399303.SZ_low']
        df_final=df_final[['valuation_date','high','close','low']]
        return df_final
    def target_index_candle2(self):
        inputpath_close=glv.get('raw_indexClose')
        inputpath_high=glv.get('raw_indexHigh')
        inputpath_low=glv.get('raw_indexLow')
        df_close=gt.readcsv(inputpath_close)
        df_high = gt.readcsv(inputpath_high)
        df_low = gt.readcsv(inputpath_low)
        df_close=df_close[['valuation_date','000300.SH','932000.CSI']]
        df_high = df_high[['valuation_date','000300.SH','932000.CSI']]
        df_low = df_low[['valuation_date','000300.SH','932000.CSI']]
        df_close.columns=['valuation_date', '000300.SH_close', '932000.CSI_close']
        df_high.columns=['valuation_date','000300.SH_high','932000.CSI_high']
        df_low.columns = ['valuation_date', '000300.SH_low', '932000.CSI_low']
        df_hl=df_high.merge(df_low,on='valuation_date',how='left')
        df_final=df_close.merge(df_hl,on='valuation_date',how='left')
        df_final['close']=df_final['000300.SH_close']-df_final['932000.CSI_close']
        df_final['high']=df_final['000300.SH_high']-df_final['932000.CSI_low']
        df_final['low']=df_final['000300.SH_low']-df_final['932000.CSI_high']
        df_final=df_final[['valuation_date','high','close','low']]
        return df_final
    def future_difference_withdraw(self):
        inputpath=glv.get('raw_futureDifference')
        df=gt.readcsv(inputpath)
        df=df[['valuation_date','difference_future']]
        return df
    def raw_vix_withdraw(self):
        inputpath=glv.get('raw_vix')
        df=gt.readcsv(inputpath)
        df=df[['valuation_date','hs300','zz1000']]
        df.fillna(method='ffill',inplace=True)
        return df

if __name__ == "__main__":
    dp = data_prepare('2025-01-01','2025-08-20')
    print(dp.index_return_withdraw())
    # df1 = dp.raw_CPI_withdraw()
    # df1.set_index('valuation_date', inplace=True)
    # #df1 = df1.shift(20)
    # df1.dropna(inplace=True)
    # df1.reset_index(inplace=True)
    # df2 = dp.target_index()
    # df = df1.merge(df2, on='valuation_date', how='left')
    # df['CPI'] = df['CPI'] / 3
    #
    # for i in [45,60,90,120]:
    #     df['MA_'+str(i)]=df['CPI'].rolling(i).mean()
    # df.dropna(inplace=True)
    # df.to_csv('D:\Signal/signal_original_test.csv', index=False, encoding='gbk')

