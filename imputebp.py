#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : yuanjing
# @File    : preprocessing.py
# @Time    : 2018/12/14 11:06

import pandas as pd
import seaborn as sn
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Lasso
from sklearn.svm import SVR
from sklearn.metrics import r2_score
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer

data_dias = pd.read_csv('M:/AKIdata/diasbp.csv')
diasbp = data_dias[['diasbp', 'ni_dbp', 'age', 'gender', 'admission_type']]

dummy_feature_dias = diasbp[['gender','admission_type']]
dummies_dias = pd.get_dummies(dummy_feature_dias,prefix=['sex','type'])
diasbp.drop(['gender', 'admission_type'], axis=1, inplace=True)
datawithdummy_dias = dummies_dias.join(diasbp)

#extract the complete dataset
data_complete_dias = datawithdummy_dias.dropna()

#statistical information
coef = data_complete_dias.corr()
# plt.subplots(figsize=(6, 6))
# sn.heatmap(coef, annot=True, cmap="Blues")
# plt.show()
print(data_complete_dias.describe())

#impute model
x_dias = data_complete_dias[['age','diasbp']]
y_dias = data_complete_dias['ni_dbp']

rf = RandomForestRegressor(n_estimators=1000, n_jobs=-1)
rf.fit(x_dias,y_dias)

R2_rf_dias = rf.score(x_dias,y_dias)

data_complete_dias['ni_dbp'] = rf.predict(x_dias)

dias

print('pause')

########################################################
#impute sysbp
data_sys = pd.read_csv('M:/AKIdata/sysbp.csv')
sysbp = data_sys[['sysbp', 'ni_sbp', 'age', 'gender', 'admission_type']]

dummy_feature = sysbp[['gender','admission_type']]
dummies = pd.get_dummies(dummy_feature,prefix=['sex','type'])
diasbp.drop(['gender', 'admission_type'], axis=1, inplace=True)
datawithdummy = dummies.join(sysbp)

#extract the complete dataset
data_complete = datawithdummy.dropna()

#statistical information
coef = data_complete.corr()
# plt.subplots(figsize=(6, 6))
# sn.heatmap(coef, annot=True, cmap="Blues")
# plt.show()
print(data_complete.describe())

#impute model
x = data_complete[['age','diasbp']]
y = data_complete['ni_dbp']

rf = RandomForestRegressor(n_estimators=1000, n_jobs=-1)
rf.fit(x,y)

R2_rf = rf.score(x,y)

data_complete['ni_dbp'] = rf.predict(x)
print(data_complete.describe())
print('pause')
