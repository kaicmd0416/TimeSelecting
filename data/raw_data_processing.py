import pandas as pd
import global_tools_func.global_tools as gt
import global_setting.global_dic as glv
from data.data_prepare import data_prepare
from data.data_processing import data_processing
import pandas_ta as ta
import os
class raw_data_processing:
    def __init__(self):
        dp=data_prepare()
        self.df_candle=dp.target_index_candle2()
        self.df_psa=self.TargetIndex_PSA()
        self.df_kdj=self.TargetIndex_KDJ()
        self.df_bbd=self.targetIndex_BOLLBAND()
        self.df_macd=self.targetIndex_MACD()
        self.df_rsi=self.targetIndex_RSI()
        self.df_ma=self.MA_processing()
        self.df_amt_hs300=dp.raw_index_amt('沪深300')
        self.df_amt_zz2000=dp.raw_index_amt('中证2000')
        self.df_volume=self.volume_processing()
        self.df_final=self.final_df_processing()
    def targetIndex_MACD(self):
        df=self.df_candle
        df.dropna(inplace=True)
        df=df[['valuation_date','close']]
        df['MACD'] = ta.macd(df['close'])['MACD_12_26_9']
        df['MACD_h'] = ta.macd(df['close'])['MACDh_12_26_9']
        df['MACD_s'] = ta.macd(df['close'])['MACDs_12_26_9']
        df.dropna(inplace=True)
        df.reset_index(inplace=True,drop=True)
        df.drop(columns='close',inplace=True)
        return df
    def targetIndex_RSI(self):
        df=self.df_candle
        df.dropna(inplace=True)
        df=df[['valuation_date','close']]
        df['RSI'] = ta.rsi(df['close'],14)
        df.dropna(inplace=True)
        df.reset_index(inplace=True,drop=True)
        df = df[['valuation_date', 'RSI']]
        return df
    def targetIndex_BOLLBAND(self):
        df=self.df_candle
        df.dropna(inplace=True)
        df = df[['valuation_date', 'close']]
        # 计算布林带指标
        # 使用正确的方式获取布林带指标
        bbands = ta.bbands(df['close'], length=20,std=1.5)
        if bbands is not None:
            df['upper'] = bbands['BBU_20_1.5']
            df['middle'] = bbands['BBM_20_1.5']
            df['lower'] = bbands['BBL_20_1.5']
        else:
            # 如果bbands返回None，设置默认值
            df['upper'] = df['close']
            df['middle'] = df['close']
            df['lower'] = df['close']
            print("Warning: Bollinger Bands calculation returned None. Using default values.")
        df=df[['valuation_date','upper','middle','lower']]
        df.columns = ['valuation_date', 'BBU_20_1.5', 'BBM_20_1.5', 'BBL_20_1.5']
        df.dropna(inplace=True)
        df.reset_index(inplace=True,drop=True)
        return df
    def TargetIndex_KDJ(self):
        df=self.df_candle
        df.dropna(inplace=True)
        df_kdj = ta.kdj(df['high'], df['low'], df['close'])
        # 将 KDJ 指标合并到原 DataFrame 中
        df = pd.concat([df, df_kdj], axis=1)
        df.dropna(inplace=True)
        df = df[['valuation_date', 'K_9_3', 'D_9_3', 'J_9_3']]
        return df
    def TargetIndex_PSA(self):
        df=self.df_candle
        df.dropna(inplace=True)
        # 计算抛物线指标
        psar = ta.psar(df['high'], df['low'])
        # 将抛物线指标合并到原 DataFrame 中
        df = pd.concat([df, psar], axis=1)
        df = df[['valuation_date', 'close', 'PSARl_0.02_0.2', 'PSARs_0.02_0.2']]
        df.loc[df['PSARl_0.02_0.2'].isna(), ['PSARl_0.02_0.2']] = df[df['PSARl_0.02_0.2'].isna()][
            'PSARs_0.02_0.2']
        df = df[['valuation_date', 'PSARl_0.02_0.2']]
        df.columns = ['valuation_date', 'psa']
        return df
    def MA_processing(self):
        df_ma=self.df_candle.copy()
        df_ma=df_ma[['valuation_date','close']]
        for i in [5,15,30,45,60,90,120]:
            df_ma['ma_'+str(i)]=df_ma['close'].rolling(i).mean()
        df_ma.drop(columns='close',inplace=True)
        return df_ma
    def volume_processing(self):
        df=self.df_amt_hs300.merge(self.df_amt_zz2000,on='valuation_date',how='left')
        df['volume_sum']=df['沪深300']+df['中证2000']
        df['volume_difference']=df['沪深300']-df['中证2000']
        df=df[['valuation_date','volume_sum','volume_difference']]
        return df
    def final_df_processing(self):
        df_final=self.df_candle.merge(self.df_psa,on='valuation_date',how='left')
        df_final = df_final.merge(self.df_kdj, on='valuation_date', how='left')
        df_final = df_final.merge(self.df_bbd, on='valuation_date', how='left')
        df_final = df_final.merge(self.df_macd, on='valuation_date', how='left')
        df_final = df_final.merge(self.df_rsi, on='valuation_date', how='left')
        df_final = df_final.merge(self.df_ma, on='valuation_date', how='left')
        df_final = df_final.merge(self.df_volume, on='valuation_date', how='left')
        df_final.dropna(inplace=True)
        df_final.to_csv('D:\OneDrive\Data_prepared_test\data_timeSeries\\raw_timeselecting_exposure.csv',index=False)
        return df_final
    def rawData_savingMain(self,start_date,end_date):
        outputpath=glv.get('raw_signal_output')
        gt.folder_creator2(outputpath)
        df_final=self.df_final.copy()
        working_days_list=gt.working_days_list(start_date,end_date)
        for available_date in working_days_list:
            print(available_date)
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'raw_signalData_'+str(available_date2)+'.csv')
            df_daily=df_final[df_final['valuation_date']==available_date]
            df_daily.to_csv(outputpath_daily,index=False)
if __name__ == "__main__":
    dfp=raw_data_processing()
    dfp.rawData_savingMain('2016-01-01','2025-05-15')



