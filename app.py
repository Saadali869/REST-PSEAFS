from flask import Flask
from dbmod import stockdbmodule
from flask import request
from flask import jsonify

dataobj=stockdbmodule()
app = Flask(__name__)
import threading

@app.route('/')
def main():
    
    return 'lmao'
@app.route('/currmarket',methods=['GET','POST'])
def req_currmarket():
    if request.method=='POST':
        identity = str(request.json["company_name"])
        iden=identity.replace("\'",'')
        result, colums = dataobj.select_company_current(iden)
        jsondata = []
        for item in result:
            jsondata.append(dict(zip(colums, item)))
        return   jsonify(jsondata)
    else:
        identity= request.args.get('id')
        result,colums=dataobj.select_company_current(identity)
        jsondata=[]
        for item in result:
            jsondata.append(dict(zip(colums,item)))
        return jsonify(jsondata)
@app.route('/history', methods=['GET','POST'])
def req_history():
    if request.method=='POST':
        identity = str(request.json["company_name"])
        iden=identity.replace("\'",'')
        result, colums = dataobj.select_company_history(iden)
        jsondata = []
        for item in result:
            jsondata.append(dict(zip(colums, item)))
        return   jsonify(jsondata)
    else:
        identity = request.args.get('id')
        result, colums = dataobj.select_company_history(identity)
        jsondata = []
        for item in result:
            jsondata.append(dict(zip(colums, item)))
        return jsonify(jsondata)
@app.route('/forecast', methods=['GET','POST'])
def req_forecast():
    if request.method=='POST':
        identity = str(request.json["company_name"])
        iden=identity.replace("\'",'')
        result, colums = dataobj.select_forecast(iden)
        jsondata = []
        for item in result:
            jsondata.append(dict(zip(colums, item)))
        return   jsonify(jsondata)
    else:
        identity = request.args.get('id')
        result, colums = dataobj.select_forecast(identity)
        jsondata = []
        for item in result:
            jsondata.append(dict(zip(colums, item)))
        return jsonify(jsondata)

if __name__ == '__main__':
    
    app.run(host='0.0.0.0')
