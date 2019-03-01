#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : yuanjing
# @File    : featureselection.py
# @Time    : 2019/1/21 16:08

import pandas as pd
import numpy as np
import seaborn as sn
import matplotlib.pyplot as plt
import pdvega
import warnings
import xgboost as xgb
from xgboost.sklearn import  XGBClassifier
from sklearn.metrics import roc_auc_score
warnings.filterwarnings('ignore')

data = pd.read_csv('visualization/preprocessed_data.csv')
labelmat = data['classlabel']
datamat = data.drop(['classlabel'],axis=1)

datamat.drop(['creat','lostime','hr_min','rr_75','mbp_std','spo2_25','F'],axis=1, inplace=True)

"""
特征值排序：原有的方法
"""
# featurenames = datamat.keys()
# num_faetures = np.shape(datamat)[1]
#
# #用scikit-feature包计算，每个特征的得分
# from skfeature.function.similarity_based import fisher_score
# from skfeature.function.similarity_based import reliefF
# from skfeature.function.statistical_based import gini_index
# from sklearn import preprocessing
#
# datamat = np.array(datamat)
# labelmat = np.array(labelmat)
#
# Relief = reliefF.reliefF(datamat,labelmat)
# Fisher = fisher_score.fisher_score(datamat,labelmat)
# gini = gini_index.gini_index(datamat,labelmat)
# gini = -gini
# FSscore = np.column_stack((Relief,Fisher,gini))#合并三个分数
#
# min_max_scaler = preprocessing.MinMaxScaler()
# FSscore = min_max_scaler.fit_transform(FSscore)
# FinalScore = np.sum(FSscore,axis=1)
# FS = np.column_stack((FSscore,FinalScore))
# FS_nor = min_max_scaler.fit_transform(FS)#将最后一列联合得分归一化
# FS = pd.DataFrame(FS_nor, columns=["Relief", "Fisher","gini","FinalScore"],index=featurenames)
#
# sorteigen = FS.sort_values(by='FinalScore',ascending=False,axis=0)
# sorteigen.to_csv('FSsort_out.csv')
#
# print("test")

# """
# 将特征值依次代入，比较AUC结果
# """
# from sklearn.model_selection import cross_validate
# from sklearn.model_selection import StratifiedKFold
#
# sortinfo = pd.read_csv('FSsort.csv')
# sortname = sortinfo.ix[:,0]
# datasorted = data[sortname]
# names = datamat.keys()
# n = np.shape(datamat)[1]
#
# meanfit = []#用来存储逐渐增加特征值过程中，不同数目特征值对应的auc平均值
# stdfit = []#用来存储逐渐增加特征值的过程中，不同数目特征值对应的AUC标准差
# cv = StratifiedKFold(n_splits=10)
# other_params={
#               'learning_rate':0.1,
#               'n_estimators':200,
#               'max_depth':6,
#               'min_child_weight':1,
#               'gamma':0,
#               'reg_alpha':10,
#               'reg_lambda':100 }
# xgbClf = XGBClassifier(**other_params)
#
# for i in range(n):
#     print("第%s个参数："%(i+1))
#     index = names[0:i+1]
#     dataMat = datamat.loc[:,index]
#     dataMat = np.array(dataMat)
#     labelmat = labelmat
#
#     scores = []#用来存十折中每一折的AUC得分
#     mean_score = []#第i个特征值交叉验证后AUC平均值
#     std_score = []#第i个特征值交叉验证和BER的标准差
#     k = 0
#
#     aue_scores = cross_validate(xgbClf, dataMat, labelmat, scoring='balanced_accuracy',
#                                 cv=cv, return_train_score=True)
#     scores = aue_scores['test_score']
#     mean_score = np.mean(scores)
#     std_score = np.std(scores)
#
#     meanfit.append(mean_score)
#     stdfit.append(std_score)
#
# meanfit = np.array(meanfit)
# writemean = pd.DataFrame(meanfit)
# writemean.to_csv('xg_meanfit_ba.csv',encoding='utf-8', index=True)
#
# stdfit = np.array(stdfit)
# writestd = pd.DataFrame(stdfit)
# writestd.to_csv('xg_stdfit_ba.csv', encoding='utf-8', index=True)
#
#
# fig, ax1 = plt.subplots()
# line1 = ax1.plot(meanfit, "b-", label="AUC")
# ax1.set_xlabel("Number of features")
# ax1.set_ylabel("AUC", color="b")
# plt.show()
#
#
# print("test")

"""
画图选择特征值个数
"""

sortFS = pd.read_csv('FSsort.csv',names=['Features','Relief','Fisher','gini','FinalScore'])#特征值排序结果
names = sortFS['Features']#排序后特征值名称
stdresult = pd.read_csv('xg_stdfit_ba.csv',names=['index','std'])
meanresult = pd.read_csv('xg_meanfit_ba.csv',names=['index','mean'])

#十折交叉验证后后BER的平均值、标准差，FS.py程序运行出来的
stdvalue=stdresult[1:82]['std']
meanvalue=meanresult[1:82]['mean']#提取有效数值，第一行和第一列是编号
stdvalue=stdvalue*100#最后单位都用%表示
meanvalue=meanvalue*100

std_up=meanvalue+stdvalue#平均值加标准差
std_down=meanvalue-stdvalue#平均值减标准差

minindex=np.argmax(meanvalue)#BER最小值对应的索引值
minvalue=meanvalue[minindex]#最小BER值

up=std_up[minindex]
down=std_down[minindex]
a=(meanvalue[(meanvalue<up)&(meanvalue>down)].index)[0]
tmp=meanvalue[a]

#创建画布，开始绘图
fig = plt.figure()
ax = fig.add_subplot(1,1,1)

font1 = {'family':'Times New Roman',
         'weight':'bold',
         'size': 16}
plt.tick_params(labelsize=16)
plt.xlim(1,80)
plt.ylim(40,100)
x=np.linspace(1,81,81)#刻度1：1：80
line_mean = ax.plot(x,meanvalue, 'r-', label='BER_mean',linewidth = 2)#BER平均值变化曲线
line_down=ax.plot(x,std_down ,'b:', label='BER_down',linewidth = 2)#（BER平均值-标准差）变化曲线
line_up=ax.plot(x,std_up, 'b:', label='BER_up',linewidth = 2)#（BER平均值+标准差）变化曲线
ax.fill_between(x,std_up,std_down,color='gray',alpha=0.25)#填充上下标准差之间的范围
line_h=ax.hlines(up,1,80,'r',alpha=0.25,linewidth = 2)#画横线BER最小值+对应标准差处的
ax.plot(minindex,minvalue,color='r',marker='o',markersize = 10)#作marker，在BER最小值位置
ax.plot(a,tmp,'r^',markersize = 10)#作marker，在最小允许特征子集处
ax.set_xticks([1,10,20,30,40,50,60,69,70,80,81])#标出需要添加的横坐标

ax.set_xticklabels([1,10,20,30,40,50,60,' ',' ',' ',' '],size=16)
# ax.set_xticks([10.5, 39], minor=True)
# ax.set_xticklabels(['10 11', '38 40'], minor=True,size=16)

ax.set_xticks([69.5,80.5], minor=True)
ax.set_xticklabels(['69 70','80 81'], minor=True,size=16)

for line in ax.xaxis.get_minorticklines():
    line.set_visible(False)


line_v=plt.vlines(minindex,0,minvalue,'r',alpha=0.25,linewidth = 2)#画竖线，最小BER位置处
line_v1=plt.vlines(a,0,tmp,'r',alpha=0.25,linewidth = 2)#画竖线，最小特征值对应位置处
plt.legend(loc='upper right',fontsize = 16)#制定label的位置
# plt.title('BER for LR')
plt.xlabel("Number of features",font1)
plt.ylabel("Balance_accuracy(%)",font1)
plt.show()



print('test')

