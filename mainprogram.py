from webscrape import stockscraper
from dbmod import stockdbmodule

#scrapeobj=stockscraper()
dbobj=stockdbmodule()
#df_decrease,df_increase,df_same=scrapeobj.scrape()
#dbobj.action(df_decrease,df_increase,df_same)
company_name=input("enter company name \n")
b=dbobj.select_company_history(company_name)
print(b)
dbobj.closure()
