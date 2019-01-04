#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : yuanjing
# @File    : dividedata.py
# @Time    : 2019/1/4 15:38

import pandas as pd
from sklearn.model_selection import train_test_split

data = pd.read_csv('M:\AKIdata\yj_aki_finaleigen.csv')

# 提取特征和标签
labelmat = data['classlabel']  # labelmat
datamat = data.drop('classlabel', axis=1)  # datamat

x_train, x_test, y_train, y_test = train_test_split(datamat, labelmat, test_size=0.3, random_state=0)
x_train['label'] = y_train
x_test['label'] = y_test

x_train.to_csv('trainset.csv',index=0)
x_test.to_csv('testset.csv',index=0)
print("test")