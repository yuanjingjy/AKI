#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : yuanjing
# @File    : preprocessing.py
# @Time    : 2018/12/14 11:06
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder,StandardScaler,QuantileTransformer
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.svm import SVC
from sklearn.metrics import roc_auc_score,accuracy_score
from time import time
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.pipeline import make_pipeline

dataset=pd.read_csv('akidata.csv')
diasbp = pd.read_csv('M:/AKIdata/diasbp.csv')
del_col =['icustay_id','subject_id','hadm_id','intime','outtime','akistarttime','ethnicity','hospital_expire_flag','av_icustayid']
data = dataset.drop(del_col,axis=1)#delete irrelevent columns

labelmat = data['classlabel']#labelmat
datamat=data.drop('classlabel',axis=1)#datamat

x_train,x_test,y_train,y_test=train_test_split(datamat,labelmat,test_size=0.3,random_state=25)

keys = list(datamat.keys())#all eigen names
keys.remove('admission_type')
keys.remove('gender')#all eigen but admission_type and gender are numeric eigen

numeric_features=keys#impute the missing value with median for the numeric eigen
numeric_transformer = Pipeline(steps=[
    ('imputer',SimpleImputer(strategy='median')),
    ('scaler',QuantileTransformer(random_state=0,output_distribution='uniform'))
])

categorical_features=['admission_type','gender']#impute the missing value with 'missing' and Encode them for the categorical eigens
categorical_transformer = Pipeline(steps=[
    ('imputer',SimpleImputer(strategy='constant',fill_value='missing')),
    ('onehot',OneHotEncoder(handle_unknown='ignore'))
])

preprocessor = ColumnTransformer(#preprocessing numeric and categorical eigen with different methods respectively
    transformers=[
        ('num',numeric_transformer,numeric_features),
        ('cat',categorical_transformer,categorical_features)
    ]
)
param_range = [0.01, 0.1, 1.0]
param_grid = {'svm__C': param_range,
               'svm__kernel': ['linear']}

n_iter_search = 20
clf = SVC()
estimat = make_pipeline(SVC(),SGDClassifier(),MLPClassifier())
estimat.fit(x_train,y_train)
pipe = Pipeline(steps=[('preprocessor',preprocessor),#the final classifier,pipeline
                      ('model',estimat)])

grid = GridSearchCV(estimator=pipe, param_grid = param_grid, cv=10)

grid.fit(x_train,y_train)
############################test the final model#################################


clf = grid.best_estimator_
clf.fit(x_train,y_train)
score = clf.score(x_test,y_test)


print('test')