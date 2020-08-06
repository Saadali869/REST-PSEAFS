from bs4 import BeautifulSoup
import lxml
import requests
import time

def frmt(seq):
    a=[]
    for x in seq:
        try:
            a.append( float(x))

        except ValueError:
            a.append(x)

    return a      #this fun will return the formated list


class stockscraper:
    def __init__(self):
        self.webpage='https://www.psx.com.pk/market-summary/'

    def scrape(self):
        page=requests.get(self.webpage).text

        soup=BeautifulSoup(page, 'lxml')
        data_increase=soup.findAll("tr", "green-text-td")
        data_decrease=soup.findAll("tr","red-text-td")
        data_constant=soup.findAll("tr", class_="blue-text-td")
        df_increase=[]
        df_decrease=[]
        df_same=[]
        for item in data_increase:
            a=item.findAll("td")
            str = []
            for itm in a:
                if len(itm.findAll(text=True)) == 2:
                    str.append(itm.findAll(text=True)[1])
                else:
                    str.append(itm.findAll(text=True)[0].replace(",",""))
            res=frmt(str)

            df_increase.append(res)

        for item in data_decrease:
            a=item.findAll("td")
            str = []
            for itm in a:
                if len(itm.findAll(text=True)) == 2:
                    str.append(itm.findAll(text=True)[1])
                else:
                    str.append(itm.findAll(text=True)[0].replace(",",""))
            df_decrease.append(frmt(str))

        for item in data_constant:
            a=item.findAll("td")
            str = []
            for itm in a:
                if len(itm.findAll(text=True)) == 2:
                    str.append(itm.findAll(text=True)[1])
                else:
                    str.append(itm.findAll(text=True)[0].replace(",",""))
            df_same.append(frmt(str))


        return df_decrease,df_increase,df_same
#if finally formatted the stuff by using if else on the .findall function to remove the span element aka reformatting
