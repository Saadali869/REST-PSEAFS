from webscrape import stockscraper
from dbmod import stockdbmodule
from Predictor import predictor

import time


def serversideprog():
    modelnames = ['Habib Bank Limited', 'Pakistan State Oil Co']
    forecastor = predictor()

    scrapeobj = stockscraper()
    dbobj = stockdbmodule()
    while True:
        if (stockdbmodule.flag):
            df_decrease, df_increase, df_same = scrapeobj.scrape()
            dbobj.action(df_decrease, df_increase, df_same)
            for x in modelnames:
                forecastor.Forecast(x)
            time.sleep(480)
        else:
            print('market closed \n')
            break
    dbobj.closure()
if __name__ == '__main__':
    serversideprog()