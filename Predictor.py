import keras
import numpy as np
import pandas as pd
import sklearn
from sklearn.preprocessing import MinMaxScaler
from dbmod import stockdbmodule
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
modelnames=['Habib Bank Limited','Pakistan State Oil Co']
class predictor:
    def __init__(self):
        self.scaler=MinMaxScaler(feature_range=(0,1))
        self.DBobj =stockdbmodule()
        self.models={}
        for x in modelnames:
            self.models[x]=keras.models.load_model('Models/{}.h5'.format(x))
        # its necessary that there should be 10 records of the company present in the database so that the scaler can work effectively
    def Forecast(self,company_name):
        if company_name in modelnames:
            #model=keras.models.load_model('Models/{}.h5'.format(company_name))
            past_OHL= self.DBobj.company_past_OHL(company_name)
            curr_ohl= self.DBobj.company_current_OHL(company_name)
            modelinputs=self.preparedata(past_OHL,curr_ohl)
            predclose=self.models[company_name].predict(modelinputs)
            self.scaler.fit_transform(self.DBobj.company_past_closure(company_name))
            expclosing=self.postproc(predclose)
            #print(company_name+" expected closing "+str(expclosing))
            self.DBobj.delete_forecast(company_name)
            self.DBobj.insert_forecast(expclosing,company_name)
        else:
            b='model for this company does not exist'

    def preparedata(self,past_OHL,curr_OHL):
        b=np.ravel(curr_OHL)
        past_OHL.append(b)
        c=past_OHL
        normalizedarray=self.scaler.fit_transform(c)
        predinput=np.reshape(np.array(normalizedarray),(int(normalizedarray.shape[0]),1,3))
        return predinput
    def postproc(self,predclose):
        expclose=np.reshape(np.array(predclose), (-1,1))

        closure=self.scaler.inverse_transform(expclose)
        close=np.ravel(closure)
        n=int(close.shape[0])-1
        currclose=close[n]
        return currclose