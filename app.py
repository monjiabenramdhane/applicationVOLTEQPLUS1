import sys,os
from flask import Flask,jsonify,request
from authCnx import LDAPCNX
from model import reglement
from flask import Flask,jsonify
from math import *
from flask_cors import CORS
import pandas as pd 
from datetime import datetime,timedelta,timezone
from flask_jwt_extended import create_access_token
from flask_jwt_extended import get_jwt
from flask_jwt_extended import get_jwt_identity
from flask_jwt_extended import jwt_required
from flask_jwt_extended import JWTManager
from flask_jwt_extended import set_access_cookies
from flask_jwt_extended import unset_jwt_cookies


app=Flask(__name__)
cors =CORS(app, resources={r"/*": {"origins": "*"}})
jwt = JWTManager(app)
app.config.from_pyfile('./config/config.py')
app.config["JWT_HEADER_TYPE"] = "JWT"
df=pd.DataFrame()
class reliquat: 
    def __init__(self, docD, status): 
        self.docD = docD
        self.status = status


@app.route("/")
def hello():
    return "Server is running..."

@app.route('/listOFs/<doc_num>',methods=['GET'])
def getListOF(doc_num):
    try:
        hf=reglement()
        detail=doc_num
        # of=hf.getDetailsDF(q)
        c=hf.getlistOFS(detail)
        return jsonify(listOFs=c)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        with open("./journal.log","a") as journalFile:
            current_time = datetime.now()
            journalFile.write(str(current_time)+':'+'qte\n')
            journalFile.write( str(fname)+ " "+ str(exc_tb.tb_lineno))
            journalFile.write(str(e)+'\n')
        return {"msg":"error"}
@app.route('/ajouterEnteteBL/<user>',methods=['POST','GET'])
def insertEntete(user):
    try:
        df=pd.DataFrame()
        c=pd.DataFrame()
        hf=reglement()
        data = request.json
        resultat=[]
        j=1
        for i in data:
            d=hf.getDocumentD(i['ofd_id'],i['doc_Num'],i['count'],j)
            j=j+1
            resultat.append(i)
            df=pd.concat([df, d])
        #res = df.to_sql("documentD",hf.engine,schema='dbo',if_exists='append',index=False)
    

        
        if len(df) > 0 :
            d=hf.inserstionBLtete(data[0]['doc_Num'])

            d['Doc_THT']=sum(df['DocD_THT'])
            d['Doc_TTPF']=sum(df['DocD_TPF'])
            d['Doc_TTVA']=sum(df['DocD_TVA'])
            d['Doc_TTTC']=sum(df['DocD_TTC'])
            d['Doc_RelReg']=sum(df['DocD_TTC'])
            d['Doc_TTVAC']=sum(df['DocD_TVA'])
            d['Doc_TTPFC']=sum(df['DocD_TPF'])
            d['Site_Code']='ATLVOLTEQ'
            d['User_Id_Validation']= user
            d['User_Id_Create']= user
            d['Doc_THTB']=sum(df['DocD_PHTB'])
            df1=d
            try:
                df1.to_sql("document",hf.engine,schema='dbo',if_exists='append',index=False)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                with open("./journal.log","a") as journalFile:
                    current_time = datetime.now()
                    journalFile.write(str(current_time)+':'+'ajouterBL\n')
                    journalFile.write(str(fname)+" "+str(exc_tb.tb_lineno))
                    journalFile.write(str(e)+'\n')
                return {"status": False, "msg": e}
            df.to_sql("documentD",hf.engine,schema='dbo',if_exists='append',index=False)
            cpt=hf.updateCpt()
            for obj in resultat:
                qte=hf.updateqte(obj['ofd_id'],obj['count'])
            return {"status":True,"BL_num":d['Doc_Num'][0]}   
        else :
            return {"status": False}    
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        with open("./journal.log","a") as journalFile:
            current_time = datetime.now()
            journalFile.write(str(current_time)+':'+'ajouterBL\n')
            journalFile.write( str(fname)+ " "+ str(exc_tb.tb_lineno))
            journalFile.write(str(e)+'\n')
        return {"status": False, "msg": e}
@app.route("/login", methods=["POST"])
def login():
    try:       
        username =  request.json.get("username", None)
        password = request.json.get("password", None) 
        cnx=LDAPCNX()
        reponse=cnx.Connecter(username,password)
        if reponse==False:
            return jsonify({"connected": False, "msg":"wrong credintials."})
        access_token = create_access_token(identity=username)        
        response = jsonify({"connected":True,"access_token":access_token}) 
        set_access_cookies(response, access_token)
        return response
    except Exception as e:
        return {"connected":False,"msg":e}

@app.route("/logout", methods=["GET"])
def logout():
    try:
        response = jsonify({"status": True, "msg": "logout successfully"})
        unset_jwt_cookies(response)
        return response
    except Exception as e:
        #TODO handle exception 
        response = jsonify({"status": False, "msg": "Coudn't logout", "error": str(e)})
        return response
if __name__ == '__main__' :
    app.run(host='0.0.0.0', debug=True ,port=5200)



