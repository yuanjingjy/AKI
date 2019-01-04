#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : yuanjing
# @File    : preprocessing.py
# @Time    : 2018/12/14 11:06
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder,QuantileTransformer,FunctionTransformer
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.svm import SVC
from sklearn.metrics import roc_auc_score,accuracy_score
from time import time
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.pipeline import make_pipeline
from sklearn.base import BaseEstimator,TransformerMixin

#定义预处理类
class preprocessing(object):
    def transform(self,data):
        # 处理年龄
        index = data[data['age'] > 200].index
        data['age'].loc[index] = 91.4

        # 去掉建模无关项
        del_col = ['icustay_id', 'akistarttime', 'ethnicity', 'hospital_expire_flag']
        data = data.drop(del_col, axis=1)  # delete irrelevent columns

        # 添加BMI的标签，插值得到的标签为1，否则为0
        data['bmi_label'] = 0
        index_bmi = data[data['height'].isnull() | data['weight'].isnull()].index
        data['bmi_label'].loc[index_bmi] = 1

        # # 提取所有的数值型特征的名称
        # keys = list(data.keys())
        # for key in ['admission_type', 'gender', 'vaso', 'vent', 'bmi_label']:
        #     keys.remove(key)
        #
        # # 定义对数值型变量和分类型变量的处理方法
        # numeric_features = keys  # impute the missing value with median for the numeric eigen
        # numeric_transformer = Pipeline(steps=[
        #     ('imputer', SimpleImputer(strategy='mean')),
        #     ('scaler', QuantileTransformer(random_state=0, output_distribution='uniform'))
        # ])
        #
        # categorical_features = ['admission_type', 'gender', 'vaso', 'vent',
        #                         'bmi_label']  # impute the missing value with 'missing' and Encode them for the categorical eigens
        # categorical_transformer = Pipeline(steps=[
        #     ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
        #     ('onehot', OneHotEncoder(handle_unknown='ignore'))
        # ])
        #
        # # 预处理模块，ColumnTransformer函数分别处理数值型特征和分类型特征
        # preprocessor_imp = ColumnTransformer(
        #     # preprocessing numeric and categorical eigen with different methods respectively
        #     transformers=[
        #         ('num', numeric_transformer, numeric_features),
        #         ('cat', categorical_transformer, categorical_features)
        #     ]
        # )
        #
        # # preprocessor_imp.fit(data)
        # data_processed = preprocessor_imp.transform(data)

        data_processed = data

        return data_processed

    def fit(self,X,y=None):
        return self

def main():
    # 加载训练集数据
    data = pd.read_csv('trainset.csv')

    #分离特征和标签
    labelmat = data['label']
    datamat = data.drop(['label'], axis=1)


    pipeline = Pipeline([('preprocess', preprocessing())])
    test = pipeline.transform(datamat)
    return test

if __name__ == '__main__':
    data = main()
    print(data)


