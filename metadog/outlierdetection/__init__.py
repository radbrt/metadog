from prophet import Prophet
import pandas as pd
import numpy as np


class OutlierDetector:
    def __init__(self):
        pass


    def get_outliers_in_df(self, df):
        m = Prophet()
        m.fit(df)
        pred = m.predict(df)
        pred['observed'] = df['y']
        outliers = pred[ (pred['yhat_lower'] > pred['observed'])  | (pred['yhat_upper'] < pred['observed']) ]


        return outliers[['ds', 'observed', 'yhat_lower', 'yhat_upper']]


    def get_warnings(self):
        pass

