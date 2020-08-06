import keras
import numpy as np
import pandas as pd
import sklearn
from sklearn.preprocessing import MinMaxScaler
from dbmod import stockdbmodule
modelnames=['Habib bank Limited','Pakistan State Oil Co']
class Predictor:
    def __init__(self):
        self.scaler=MinMaxScaler(feature_range=(0,1))
        self.DBobj =stockdbmodule()

        # its necessary that there should be 10 records of the company present in the database so that the scaler can work effectively
    def Forecast(self,company_name):
        if company_name in modelnames:
            model=keras.models.load_model('/models/{}.h5'.format(company_name))
            past_OHL= self.DBobj.company_past_OHL(company_name)
            curr_ohl= self.DBobj.company_current_OHL(company_name)
            modelinputs=self.preparedata(past_OHL,curr_ohl)
            predclose=model.predict(modelinputs)
            expclosing=self.postproc(predclose)
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
        expclose=np.reshape(predclose, (-1,1))
        closure=self.scaler.inverse_transform(expclose)
        close=np.ravel(closure)
        n=close.len()-1
        currclose=close[n]
        return currclose