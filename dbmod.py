from MySQLdb import Error
import MySQLdb
import datetime
import pytz
class stockdbmodule():
    flag=True
    def __init__(self):
        self.mydb=0
        self.df_decrease=[]
        self.df_increase=[]
        self.df_same=[]
        self.tz = pytz.timezone("Asia/Karachi")
        try:
            self.mydb = MySQLdb.connector.connect(host='localhost', port=3308, user='root', password='', database="pseafs")
        except MySQLdb.connector.Error as e:
            print("failed connection {}".format(e))
            exit(4)
    def action(self,DF_decrease,DF_increase,DF_same):
        self.df_increase=DF_increase
        self.df_decrease=DF_decrease
        self.df_same=DF_same


        timestring = "15:30"
        closuretime = datetime.datetime.strptime(timestring, "%H:%M")
        optimestr="10:00"
        openingtime = datetime.datetime.strptime(timestring, "%H:%M")
        if (datetime.datetime.now(self.tz).strftime("%a") =="Fri"):
            timestring = "13:00"
            closuretime = datetime.datetime.strptime(timestring, "%H:%M")
            if (datetime.datetime.now(self.tz).time() > closuretime.time())or(datetime.datetime.now(self.tz).time() < openingtime.time()):
                self.date_append()
                self.history_insert()
            else:
                stockdbmodule.flag=True
                self.scrapeinsert()
        else:
            if (datetime.datetime.now(self.tz).strftime("%a") !="Sat" )and (datetime.datetime.now(self.tz).strftime("%a") !="Sun"):
                if (datetime.datetime.now(self.tz).time() > closuretime.time())or(datetime.datetime.now(self.tz).time() < openingtime.time()):
                    self.date_append()
                    self.history_insert()
                else:
                    stockdbmodule.flag=True
                    self.scrapeinsert()
            else:
                stockdbmodule.flag=False
    def date_append(self):
        thisday=datetime.datetime.now(self.tz).date().strftime("%Y-%m-%d")
        for item in self.df_decrease:
            item.insert(0,thisday)
        for item in self.df_increase:
            item.insert(0, thisday)
        for item in self.df_same:
            item.insert(0,thisday)

    def history_insert(self):
        if stockdbmodule.flag:
            stockdbmodule.flag=False
            try:

                cursor = self.mydb.cursor()
                sql = "DELETE from market_history where market_history.dated <= SYSDATE()-10"
                cursor.execute(sql)
                self.mydb.commit()
                for varlist in self.df_decrease:

                    sql = "INSERT INTO market_history VALUES %r;" % (tuple(varlist),)
                    cursor.execute(sql)

                    self.mydb.commit()
                for varlist in self.df_increase:

                    query_string = "INSERT INTO market_history VALUES %r;" % (tuple(varlist),)
                    cursor.execute(query_string)

                    self.mydb.commit()
                for varlist in self.df_same:
                    #var_string = ', '.join('?' * len(varlist))
                    query_string = "INSERT INTO market_history VALUES %r;" % (tuple(varlist),)
                    cursor.execute(query_string)

                    self.mydb.commit()
                cursor.close()

            except MySQLdb.connector.Error as error:
                print("Failed to insert record into table {} program terminates".format(error))
                exit(4)
            except AttributeError as e1:
                print("failed because {}".format(e1))
                exit(4)
    def scrapeinsert(self):
        try:

            cursor = self.mydb.cursor()
            sql = "TRUNCATE TABLE current_market"
            cursor.execute(sql)
            self.mydb.commit()
            for varlist in self.df_decrease:
                #var_string = ', '.join('?' * len(varlist))
                #params = ['?' for item in varlist]
                sql = "INSERT INTO current_market VALUES %r;" % (tuple(varlist),)
                cursor.execute(sql)
                # MySQLdb_insert_query = """INSERT INTO Laptop (Id, Name, Price, Purchase_date)
                #                      VALUES
                #                     (10, 'Lenovo ThinkPad P71', 6459, '2019-08-14') """



                self.mydb.commit()
            for varlist in self.df_increase:
                #var_string = ', '.join('?' * len(varlist))
                query_string = "INSERT INTO current_market VALUES %r;" % (tuple(varlist),)
                cursor.execute(query_string)




                self.mydb.commit()
            for varlist in self.df_same:
                #var_string = ', '.join('?' * len(varlist))
                query_string = "INSERT INTO current_market VALUES %r;" % (tuple(varlist),)
                cursor.execute(query_string)




                self.mydb.commit()
            cursor.close()

        except MySQLdb.connector.Error as error:
            print("Failed to insert record into table {} program terminates".format(error))
            exit(4)
        except AttributeError as e1:
            print("failed because {}".format(e1))
            exit(4)
    def select_market_history(self):
        try:
            cursor=self.mydb.cursor()
            sql="Select dated,company_name,open,high,low,close from market_history"
            cursor.execute(sql)
            result=cursor.fetchall()
            a=[]
            for x in result:
                a.append(list(x))
            return a
        except MySQLdb.connector.Error as error:
            print("Failed to select records from table {} program terminates".format(error))
            exit(4)
        except AttributeError as e1:
            print("failed because {}".format(e1))
            exit(4)
    def select_company_history(self,company_name):
        try:
            cursor = self.mydb.cursor()
            sql="select dated,company_name,open,high,low,close from market_history where company_name like '%{}%'".format(company_name)
            cursor.execute(sql)
            result=cursor.fetchall()
            a=[]
            for x in result:
                a.append(list(x))
            return a
        except MySQLdb.connector.Error as error:
            print("Failed to select records from table {} program terminates".format(error))
            exit(4)
        except AttributeError as e1:
            print("failed because {}".format(e1))
            exit(4)
    def insert_forecast(self,forecast,company_name):
        try:
            cursor= self.mydb.cursor()
            sql="insert into forecast values ('{compname}',{pred})".format(compname=company_name,pred=forecast)
            cursor.execute(sql)
            self.mydb.commit()
        except MySQLdb.connector.Error as error:
            print("Failed to insert record into table {} program terminates".format(error))
            exit(4)
        except AttributeError as e1:
            print("failed because {}".format(e1))
            exit(4)
    def select_forecast(self,company_name):
        try:
            cursor = self.mydb.cursor()
        except MySQLdb.connector.Error as error:
            print("Failed to select records from table {} program terminates".format(error))
            exit(4)
        except AttributeError as e1:
            print("failed because {}".format(e1))
            exit(4)
    def select_current_market(self):
        try:
            cursor = self.mydb.cursor()
            sql="select company_name,open,high,low,current from current_market"
            cursor.execute(sql)
            result=cursor.fetchall()
            a=[]
            for x in result:
                a.append(list(x))
            return a
        except MySQLdb.connector.Error as error:
            print("Failed to select records from table {} program terminates".format(error))
            exit(4)
        except AttributeError as e1:
            print("failed because {}".format(e1))
            exit(4)
    def select_company_current(self,company_name):
        try:
            cursor = self.mydb.cursor()
            sql = "select company_name,open,high,low,close from current_market where company_name like '%{}%'".format(company_name)
            cursor.execute(sql)
            result = cursor.fetchall()
            a = []
            for x in result:
                a.append(list(x))
            return a
        except MySQLdb.connector.Error as error:
            print("Failed to select records from table {} program terminates".format(error))
            exit(4)
        except AttributeError as e1:
            print("failed because {}".format(e1))
            exit(4)
    def company_past_OHL(self,company):
        try:
            cursor = self.mydb.cursor()
            sql="select open,high,low from market_history where company_name like '%{}%' order by dated".format(company)
            cursor.execute(sql)
            result=cursor.fetchall()
            comp_pst_OHL=[]
            for x in result:
                comp_pst_OHL.append(list(x))
            return comp_pst_OHL
        except MySQLdb.connector.Error as error:
            print("Failed to select records from table {} program terminates".format(error))
            exit(4)
        except AttributeError as e1:
            print("failed because {}".format(e1))
            exit(4)
    def company_current_OHL(self,company_name):
        try:
            cursor = self.mydb.cursor()
            sql = "select open,high,low from current_market where company_name like '%{}%'".format(company_name)
            cursor.execute(sql)
            result = cursor.fetchall()
            a = []
            for x in result:
                a.append(list(x))
            return a
        except MySQLdb.connector.Error as error:
            print("Failed to select records from table {} program terminates".format(error))
            exit(4)
        except AttributeError as e1:
            print("failed because {}".format(e1))
            exit(4)
    def company_past_close(self,company_name):
        try:
            cursor = self.mydb.cursor()
            sql="select dated,close from market_history where company_name like '%{}%'".format(company_name)
            cursor.execute(sql)
            result=cursor.fetchall()
            a=[]
            for x in result:
                a.append(list(x))
            return a
        except MySQLdb.connector.Error as error:
            print("Failed to select records from table {} program terminates".format(error))
            exit(4)
        except AttributeError as e1:
            print("failed because {}".format(e1))
            exit(4)
    def delete_forecast(self,company_name):
        try:
            cursor= self.mydb.cursor()
            sql="Delete from forecast where company_name={}".format(company_name)
            cursor.execute(sql)
            self.mydb.commit()
        except MySQLdb.connector.Error as error:
            print("Failed to insert record into table {} program terminates".format(error))
            exit(4)
        except AttributeError as e1:
            print("failed because {}".format(e1))
            exit(4)
    def closure(self):
        if (self.mydb.is_connected()):
            self.mydb.close()
            print("MySQLdb connection is closed")
