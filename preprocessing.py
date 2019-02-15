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
        data.loc[data['age'] > 89, 'age'] = 91.4

        # 去掉建模无关项
        del_col = ['icustay_id', 'akistarttime', 'ethnicity', 'hospital_expire_flag']
        data = data.drop(del_col, axis=1)  # delete irrelevent columns

        # 添加BMI的标签，插值得到的标签为1，否则为0
        data['bmi_label'] = 0
        index_bmi = data[data['height'].isnull() | data['weight'].isnull()].index
        data.loc[index_bmi, 'bmi_label'] = 1

        #计算BMI
        height = data['height']
        weight = data['weight']
        bmi = weight / ((height / 100) * (height / 100))
        data.drop(['height', 'weight'], axis=1, inplace=True)
        data['BMI'] = bmi

        data_processed = data

        return data_processed

    def fit(self,X,y=None):
        return self

class outdata(object):
    """
    Description:
        利用改进zscore方法及Turkey方法识别异常值，两种方法都识别为异常值的认为是异常值
        改进zscore法：根据中位数及距离中位数的偏差来识别异常值
        Turkeys方法：定义IQR=上四分位数-下四分位数
                            上四分位数+3*IQR与下四分位数-3*IQR 范围外的定义为异常值
    Input:
        datain:待判断数据
    Output:
        dataout:将异常值位置置空后输出的矩阵
    """

    def transform(self,datain):
        # 利用改进zscore方法识别异常数据
        diff = datain - np.median(datain, axis=0)  # 原始数据减去中位数
        MAD = np.median(abs(diff), axis=0)  # 上述差值的中位数
        zscore = (0.6745 * diff) / (MAD + 0.0001)  # 计算zscore分数
        zscore = abs(zscore)  # zscore分数的绝对值
        dataout = datain.copy()
        mask_zscore = zscore > 3.5  # zscore分数大于3.5的认为是异常值，mask_score对应位置为1，其余位置为0

        # 利用Turkey方法识别异常值
        Q1, mid, Q3 = np.percentile(datain, (25, 50, 75), axis=0)  # 求上四分位数Q1、中位数mid、下四分位数
        IQR = Q3 - Q1
        out_up = Q3 + 3 * IQR
        out_down = Q1 - 3 * IQR
        mask_precup = np.maximum(datain, out_up) == datain  # 超过上限的异常值
        mask_precdown = np.maximum(datain, out_down) == out_down  # 超过下限的异常值
        mask_prec = np.logical_or(mask_precdown, mask_precup)  # 逻辑或，合并超过上限和下限的异常值

        maskinfo = np.logical_and(mask_prec, mask_zscore)  # 两种方法都识别为异常值的认为是异常值

        dataout[maskinfo] = np.nan  # 异常值位置置空
        return dataout

    def fit(self,X,y=None):
        return self

class preimp(object):
    def transform(self,data):
        # 提取所有的数值型特征的名称
        keys = list(data.keys())
        for key in ['admission_type', 'gender', 'vaso', 'vent', 'bmi_label','diuretic']:
            keys.remove(key)

        # 定义对数值型变量和分类型变量的处理方法
        numeric_features = keys
        numeric_transformer = Pipeline(steps=[
            ('imputer0', SimpleImputer(strategy='median'))
            ,('outdata', outdata())
            ,('imputer', SimpleImputer(strategy='mean'))
            ,('scaler', QuantileTransformer(random_state=0, output_distribution='uniform'))
        ])

        #定义分类型变量的预处理方法
        categorical_features = ['admission_type', 'gender', 'vaso', 'vent', 'bmi_label','diuretic']
        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
            ('onehot', OneHotEncoder(handle_unknown='ignore'))
        ])

        # 预处理模块，ColumnTransformer函数分别处理数值型特征和分类型特征
        preprocessor_imp = ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, numeric_features),
                ('cat', categorical_transformer, categorical_features)
            ]
        )

        preprocessor_imp.fit(data)
        data_processed = preprocessor_imp.transform(data)

        return data_processed

    def fit(self, X, y=None):
        return self

def main():
    # 加载训练集数据
    data = pd.read_csv('trainset.csv')

    #分离特征和标签
    labelmat = data['label']
    datamat = data.drop(['label'], axis=1)

    #最终的pipeline
    pipeline = Pipeline([('preprocess', preprocessing()),
                         ('test',preimp())
                        ])
    pipeline.fit(datamat)
    test = pipeline.transform(datamat)
    return test, labelmat

if __name__ == '__main__':
    data,label = main()
    eigens = ['creat', 'hr_max', 'hr_min', 'hr_avg', 'hr_std', 'hr_mid',
            'hr_25', 'hr_75', 'rr_max', 'rr_min', 'rr_avg', 'rr_std',
            'rr_mid', 'rr_25', 'rr_75', 'sbp_max', 'sbp_min', 'sbp_avg',
            'sbp_std', 'sbp_mid', 'sbp_25', 'sbp_75', 'dbp_max', 'dbp_min',
            'dbp_avg', 'dbp_std', 'dbp_mid', 'dbp_25', 'dbp_75', 'mbp_max',
            'mbp_min', 'mbp_avg', 'mbp_std', 'mbp_mid', 'mbp_25', 'mbp_75',
            'si_max', 'si_min', 'si_avg', 'si_std', 'si_mid', 'si_25', 'si_75',
            'spo2_max', 'spo2_min', 'spo2_avg', 'spo2_std', 'spo2_mid', 'spo2_25',
            'spo2_75', 'tem_max', 'tem_min', 'tem_avg', 'tem_std', 'tem_mid', 'tem_25',
            'tem_75', 'uo_max', 'uo_min', 'uo_avg', 'uo_std', 'uo_mid', 'uo_25', 'uo_75',
            'uosum', 'gcs_max', 'gcs_min', 'gcs_avg', 'gcs_std', 'gcs_mid', 'gcs_25', 'gcs_75',
            'age', 'lostime', 'inputsum','type_0','type_1','type_2',
            'F', 'M', 'vaso_0', 'vaso_1', 'vent_0', 'vent_1', 'bmi_0', 'bmi_1','diu_0', 'diu_1', 'BMI']
    dataframe = pd.DataFrame(data,columns = eigens)
    dataframe['classlabel'] = label
    dataframe.to_csv('visualization/preprocessed_data.csv', index=0)
    print(data)