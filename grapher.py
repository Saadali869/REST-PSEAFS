import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dbmod import stockdbmodule
class Grapher:
    def __init__(self,company_name):
        self.dbobj=stockdbmodule()
        
        self.coordinates=self.dbobj.company_past_close(company_name)
        print(self.coordinates)
        self.dataframe=pd.DataFrame(self.coordinates,columns=['Date','Close'])
        self.fig,self.ax=plt.subplots()

    def plot(self):
        self.dataframe.plot(marker="o",color="b")
        plt.show()