###############################################################################################################################
#Code create in order to create KPIs with the counters of the tecnology UMTS
#Code created by Ing. Carlos Suarez
#
#Family of counters used in this code: 67109368 , 67109365 , 67109372 , 67109390 , 67109471 , 67109508 , 67109369 , 67109367
# 67109373 , 67109391 , 67109376 , 82834952 , 50331648 , 82863958
#this code is used to process the data that comes from the servers of Huawei Host03 and Host12 to create KPI used in
# the tecnology UMTS the code uses one for cicle for each family of counters in order to concatenate the data from each Host
###############################################################################################################################
from datetime import timedelta
import time
import pandas as pd
import os
import shutil
import glob
import sys
import tarfile
import timeit
import numpy as np

def cc(datax):

    datax['U_RRC_SETUP_SUCCESS_RATIO_DEN_NONE']=(datax['67179329']+datax['67179330']+datax['67179331']+datax['67179332']+datax['67179333']+datax['67179334']+datax['67179335']+datax['67179336']+datax['67179337']+datax['67179338']+datax['67179348']+datax['67179343']+datax['67179344']+datax['67179345']+datax['67179346']+datax['67179347'])
    datax['U_RRC_SETUP_SUCCESS_RATIO_NUM_NONE']=(datax['67179457']+datax['67179458']+datax['67179459']+datax['67179460']+datax['67179461']+datax['67179462']+datax['67179463']+datax['67179464']+datax['67179465']+datax['67179466']+datax['67179476']+datax['67179471']+datax['67179472']+datax['67179473']+datax['67179474'])
    datax['U_CS_RAB_SETUP_SUCCESS_RATIO_CELL_DEN_NUMBER']=(datax['67179825']+datax['67179826'])
    datax['U_CS_RAB_SETUP_SUCCESS_RATIO_CELL_NUM_NUMBER']=(datax['67179827']+datax['67179828'])

all_filenames= [i for i in glob.glob ("/var/files_CRUDOSUMTS/*.{}".format("csv"))]
files_list1=[]

for j in all_filenames:

    if '67109365' in j:
        df= pd.read_csv (j, header=0,dtype={'Result Time':str,'Granularity Period':str,'Object Name':str,'Reliability':str,"67179298":float,"67179299":float,"67179302":float,"67179329":float,"67179330":float,"67179331":float,"67179332":float,"67179333":float,"67179334":float,"67179335":float,"67179336":float,"67179337":float,"67179338":float,"67179339":float,"67179340":float,"67179341":float,"67179342":float,"67179343":float,"67179344":float,"67179345":float,"67179346":float,"67179347":float,"67179348":float,"67179457":float,"67179458":float,"67179459":float,"67179460":float,"67179461":float,"67179462":float,"67179463":float,"67179464":float,"67179465":float,"67179466":float,"67179467":float,"67179468":float,"67179469":float,"67179470":float,"67179471":float,"67179472":float,"67179473":float,"67179474":float,"67179475":float,"67179476":float,"67179633":float,"67179634":float,"67179649":float,"67179650":float,"67189400":float,"67189401":float,"67190586":float,"67190587":float,"67190588":float,"67190589":float,"67192607":float,"67196198":float,"67196199":float,"67196200":float,"67196201":float,"67199510":float,"73403798":float,"73421887":float,"73423486":float,"73423488":float,"73423490":float,"73423492":float,"73423494":float,"73423496":float,"73423498":float,"73423502":float,"73423504":float,"73423506":float,"73423508":float,"73423510":float,"73439969":float,"73441146":float,"73441147":float,"73441148":float,"73441149":float,"73441150":float,"73441151":float,"73441152":float,"73441153":float,"73441154":float,"73441155":float,"73441156":float,"73441157":float},na_values=['None','%','ms'])
        df=df.drop([0],axis=0)
        df= df.replace(to_replace="NIL", value="0",regex=True)
        df.columns = df.columns.str.upper().str.replace(' ', '_').str.replace('.', '_', regex=True)
        df['DIA'] = df['RESULT_TIME'].str.extract('(\d+[-]\d+[-]\d+)')
        df['HORA'] = df['RESULT_TIME'].str.extract('(\d+[:]\d+)')
        df['DIA'] = pd.to_datetime(df.DIA)
        df['DIA'] = df['DIA'].dt.strftime('%d/%m/%Y')
        new = df["OBJECT_NAME"].str.split("/", n = 1, expand = True)
        df["RNC"]= new[0]
        df['CELLID']=new[1]
        df['RNC']= df['RNC'].str[-3:]
        new2 = df["CELLID"].str.split(",", n = 1, expand = True)
        df['CELLID']=new2[1]
        df['CELLID'] = df['CELLID'].str.replace('CellID=', '',regex=True)
        df["CELLID"]=df["CELLID"].str.zfill(5)
        df['CELLID'] = df['CELLID'].str[-5:]
        df['NODEBID'] = df['CELLID'].str.extract('(\d{4})')
        df['TAGI']=df["RESULT_TIME"]+df["CELLID"]
        df['CELLID']= df["CELLID"].astype('int64')
        df["NODEBID"]= df["NODEBID"].astype("int64")
        df.drop(df[(df['NODEBID']==3899) & (df['RNC']=='CNT')].index,axis=0,inplace=True)
        df.drop(df[(df['NODEBID']==3341) & (df['RNC']=='LCH')].index,axis=0,inplace=True)
        df.drop(df[(df['NODEBID']==3472) & (df['RNC']=='VRA')].index,axis=0,inplace=True)
        df.drop(df[(df['NODEBID']==4027) & (df['RNC']=='VRA')].index,axis=0,inplace=True)
        files_list1.append(df)

final1= pd.concat(files_list1, ignore_index=True)
final1= final1.drop(['RESULT_TIME','GRANULARITY_PERIOD','OBJECT_NAME','RELIABILITY'],axis=1)
final1= final1.set_index("TAGI")

files_list2=[]
for j in all_filenames:
    if '67109368' in j:
        df= pd.read_csv (j, header=0,dtype={'Result Time':str,'Granularity Period':str,'Object Name':str,'Reliability':str,"67179825":float,"67179826":float,"67179827":float,"67179828":float,"67179858":float,"67179859":float,"67179860":float,"67179861":float,"67190457":float,"67190458":float,"67190460":float,"67190461":float,"67190462":float,"67190464":float,"67192120":float,"67192121":float,"67196202":float,"67196203":float,"67204853":float},na_values=['None','%'])
        df=df.drop([0],axis=0)
        df= df.replace(to_replace="NIL", value="0",regex=True)
        df.columns = df.columns.str.upper().str.replace(' ', '_').str.replace('.', '_', regex=True)
        df['DIA'] = df['RESULT_TIME'].str.extract('(\d+[-]\d+[-]\d+)')
        df['HORA'] = df['RESULT_TIME'].str.extract('(\d+[:]\d+)')
        df['DIA'] = pd.to_datetime(df.DIA)
        df['DIA'] = df['DIA'].dt.strftime('%d/%m/%Y')
        new = df["OBJECT_NAME"].str.split("/", n = 1, expand = True)
        df["RNC"]= new[0]
        df['CELLID']=new[1]
        df['RNC']= df['RNC'].str[-3:]
        new2 = df["CELLID"].str.split(",", n = 1, expand = True)
        df['CELLID']=new2[1]
        df['CELLID'] = df['CELLID'].str.replace('CellID=', '',regex=True)
        df["CELLID"]=df["CELLID"].str.zfill(5)
        df['CELLID'] = df['CELLID'].str[-5:]
        df['NODEBID'] = df['CELLID'].str.extract('(\d{4})')
        df['TAGI']=df["RESULT_TIME"]+df["CELLID"]
        df['CELLID']= df["CELLID"].astype('int64')
        df["NODEBID"]= df["NODEBID"].astype("int64")
        df.drop(df[(df['NODEBID']==3899) & (df['RNC']=='CNT')].index,axis=0,inplace=True)
        df.drop(df[(df['NODEBID']==3341) & (df['RNC']=='LCH')].index,axis=0,inplace=True)
        df.drop(df[(df['NODEBID']==3472) & (df['RNC']=='VRA')].index,axis=0,inplace=True)
        df.drop(df[(df['NODEBID']==4027) & (df['RNC']=='VRA')].index,axis=0,inplace=True)
        files_list2.append(df)

final2= pd.concat(files_list2, ignore_index=True)
final2=final2.drop(['RESULT_TIME','GRANULARITY_PERIOD','OBJECT_NAME','RELIABILITY','DIA','HORA','RNC','CELLID','NODEBID'],axis=1)
final2= final2.set_index("TAGI")
final=pd.merge(final1,final2,on='TAGI')


final=final.drop_duplicates()

cc(final)
final=final.drop(["67179298","67179299","67179302","67179329","67179330","67179331","67179332","67179333","67179334","67179335","67179336","67179337","67179338","67179339","67179340","67179341","67179342","67179343","67179344","67179345","67179346","67179347","67179348","67179457","67179458","67179459","67179460","67179461","67179462","67179463","67179464","67179465","67179466","67179467","67179468","67179469","67179470","67179471","67179472","67179473","67179474","67179475","67179476","67179633","67179634","67179649","67179650","67189400","67189401","67190586","67190587","67190588","67190589","67192607","67196198","67196199","67196200","67196201","67199510","73403798","73421887","73423486","73423488","73423490","73423492","73423494","73423496","73423498","73423502","73423504","73423506","73423508","73423510","73439969","73441146","73441147","73441148","73441149","73441150","73441151","73441152","73441153","73441154","73441155","73441156","73441157","67179825","67179826","67179827","67179828","67179858","67179859","67179860","67179861","67190457","67190458","67190460","67190461","67190462","67190464","67192120","67192121","67196202","67196203","67204853"],axis=1)
final.reset_index(inplace=True)
comp=pd.read_csv('./UMTS_COMP.csv',dtype={'NODEBID':str,'COD RF':str,'NODEBNAME':str,'RNC':str,'Estado':str,'Región RF':str,'Region Comercial':str,'Latitud':str,'Longitud':str})
comp=comp.drop(['RNC'],axis=1)

final=final.set_index('NODEBID')

if 3251 in final1.index:
        final1=final1.drop([3251],axis=0)
if 3218 in final1.index:
        final1=final1.drop([3218],axis=0)
if 4045 in final1.index:
        final1=final1.drop([4045],axis=0)
if 4212 in final1.index:
        final1=final1.drop([4212],axis=0)
if 5152 in final1.index:
        final1=final1.drop([5152],axis=0)

final.reset_index(inplace=True)
general=final
fecha=final
general=general.set_index('NODEBID')
general=general.drop(['TAGI','DIA','HORA','U_RRC_SETUP_SUCCESS_RATIO_DEN_NONE','U_RRC_SETUP_SUCCESS_RATIO_NUM_NONE','U_CS_RAB_SETUP_SUCCESS_RATIO_CELL_DEN_NUMBER','U_CS_RAB_SETUP_SUCCESS_RATIO_CELL_NUM_NUMBER'],axis=1)
general=general.drop_duplicates(subset='CELLID',keep='last')
general.reset_index(inplace=True)

#general=general.set_index('CELLID')

fecha=fecha.drop_duplicates(subset=['TAGI'],keep='last')
fecha=fecha.sort_values(by=['TAGI'],ascending=True)
final_kpi=fecha
fecha=fecha.drop(['TAGI','RNC','NODEBID','U_RRC_SETUP_SUCCESS_RATIO_DEN_NONE','U_RRC_SETUP_SUCCESS_RATIO_NUM_NONE','U_CS_RAB_SETUP_SUCCESS_RATIO_CELL_DEN_NUMBER','U_CS_RAB_SETUP_SUCCESS_RATIO_CELL_NUM_NUMBER'],axis=1)
fecha=fecha.set_index('CELLID')
final_kpi=final_kpi.drop(['TAGI','DIA','HORA','NODEBID','RNC'],axis=1)
final_kpi=final_kpi.set_index('CELLID')
final_kpi=final_kpi.replace([np.inf,-np.inf],0)

#general=general.drop(['DIA','HORA','U_RRC_SETUP_SUCCESS_RATIO_DEN_NONE','U_RRC_SETUP_SUCCESS_RATIO_NUM_NONE','U_CS_RAB_SETUP_SUCCESS_RATIO_CELL_DEN_NUMBER','U_CS_RAB_SETUP_SUCCESS_RATIO_CELL_NUM_NUMBER','U_PS_RAB_SETUP_SUCCESS_RATIOD_DEN_NUMBER','U_PS_RAB_SETUP_SUCCESS_RATIO_NUM_NUMBER','U_VS_HSDPA_MEANCHTHROUGHPUT_KBPS','U_VS_HSDPA_MEANCHTHROUGHPUT_TOTALBYTES_MB','U_VS_HSDPA_64QAM_UE_MEAN_CELL_NONE','U_VS_HSDPA_UE_MAX_CELL_NONE','U_VS_HSDPA_UE_MEAN_CELL_NONE','U_VS_HSUPA_MEANCHTHROUGHPUT_KBPS','U_VS_HSUPA_MEANCHTHROUGHPUT_TOTALBYTES_MB','U_VS_HSUPA_16QAM_UE_MEAN_CELL_NONE','U_VS_HSUPA_UE_MAX_CELL_NONE','U_VS_HSUPA_UE_MEAN_CELL_NONE','U_VS_HSUPA_UE_MEAN_TTI2MS','U_TRAFICO_CS_ERLANG_ERL','U_CS_RAB_CONGESTION_RATIO_NUM_NONE','U_CS_RAB_CONGESTION_RATIO_DEN_NONE','U_RRC_CONGESTION_RATIONUM_NUMBER','U_RRC_CONGESTION_RATIODEN_NUMBER','U_PS_RAB_CONGESTION_RATIO_DEN_NONE','U_PS_RAB_CONGESTION_RATIO_NUM_NONE','U_VS_CELL_UNAVAILTIME_SYS_S','IA_CS_UMTS_OPT_RF_NUM','IA_CS_UMTS_OPT_RF_DEN','IA_PS_UMTS_OPT_RF_DEN','IA_PS_UMTS_OPT_RF_NUM','G_CS_SERVICE_DROP_RATIO_DEN_NONE','G_CS_SERVICE_DROP_RATIO_NUM_NONE','G_PS_CALL_DROP_RATIO_DEN_NONE','G_PS_CALL_DROP_RATIO_NUM_NONE','NUMERO_DE_USUARIOS_PH_OPTRF_NONE'],axis=1)

#general.reset_index(inplace=True)

general['NODEBID']=general['NODEBID'].astype('str')
general=general.set_index('NODEBID')
comp=pd.merge(comp,general,on='NODEBID',how='right')
comp=comp.drop_duplicates()
comp=comp.set_index('CELLID')
columna_move=comp.pop('RNC')
comp.insert(3,'RNC',columna_move)
comp=comp.sort_values(by='CELLID',ascending=True)
#final=final.replace([np.inf,-np.inf],0)
#final=final.sort_values(by='CELLID', ascending=True)
fecha.to_csv('/var/files_CRUDOSUMTS/fecha.csv')
comp.to_csv('/var/files_CRUDOSUMTS/general.csv')
final_kpi.to_csv('/var/files_CRUDOSUMTS/finalkpis.csv')