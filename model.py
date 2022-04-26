from dbConnection import Connection
import pandas as pd 
from decimal import *

from datetime import datetime
from datetime import date
import sqlalchemy
from pandas._libs.tslibs.timestamps import Timestamp

class reglement:
    def __init__(self):
        a=Connection()
        a.connect("192.168.10.216","sa","123","volteqplus_monjia")
        a.conn
        self.cnx=a.conn 
        self.engine = sqlalchemy.create_engine("mssql+pymssql://sa:123@192.168.10.216/volteqplus_monjia",deprecate_large_types=True)
  
    def getlistOFS(self,doc_num):
        req="select  OFD.OFD_Id,documentD.DocD_QReliquat1 from (volteqplus_monjia.dbo.OFD OFD INNER JOIN volteqplus_monjia.dbo.DocumentD DocumentD ON OFD.OFD_DocLigne=DocumentD.DocD_Id)  INNER JOIN volteqplus_monjia.dbo.OFE OFE ON OFD.OF_Num=OFE.OF_Num WHERE  OFE.OFParam_Code=N'OF' AND DocumentD.Doc_Num ='"+doc_num+"'"
        cursor = self.cnx.cursor(as_dict=True)
        cursor.execute(req)
        ParamList = cursor.fetchall()
        res=[]
        for i in ParamList:
            res.append({i['OFD_Id']:int(i['DocD_QReliquat1'])})
        return  res
    def creationDoc_Num(self):
        today = date.today()
        annee= today.strftime('%Y')
        req="SELECT Cpt_Num FROM volteqplus_monjia.[dbo].[Cpt] where Cpt_Doc like 'BLVR' and Cpt_Annee="+annee+""
        cursor = self.cnx.cursor(as_dict=True)
        cursor.execute(req)
        resultat = cursor.fetchall()
        cpt=resultat[0]['Cpt_Num']
        reqDoc= f"SELECT volteqplus_monjia.[dbo].[fn_CptBL]('{annee}', '{cpt}') AS 'Doc_num'"
        cursor = self.cnx.cursor(as_dict=True)
        cursor.execute(reqDoc)
        resultat = cursor.fetchall()
        Doc_num=resultat[0]['Doc_num']
        return Doc_num
    def inserstionBLtete(self,q):
        now =datetime.now()
        today = date.today()
        req="select * from volteqplus_monjia..Document where doc_num='"+q+"'"
        df = pd.read_sql_query(req,self.cnx)
        df['Doc_Num']=self.creationDoc_Num()
        df['Doc_Date']=today
        
        df['Doc_Type']='BLIVR'
        df['Doc_Nat']='DV'
        df[['Doc_DateP','Doc_DateSys','Date_Create','Date_Validation']]=now
        return  df
            
    def getEXStatus(self,tierscode):
        req="select Tiers_ExoStatut,Tiers_code,Tiers_RS,Tiers_Exo_Deb,Tiers_Exo_Fin from volteqplus_monjia..Tiers where tiers_code = '"+tierscode+"' "
        df = pd.read_sql_query(req,self.cnx)
        return  df

    def getDocumentD(self,id,doc_num,qte,i):
        req="select document.tiers_code,documentD.DocD_Id,documentD.Doc_Num,documentD.DocDOrigine,documentD.Art_Code,documentD.xArt_Des1,documentD.Art_Des,documentD.DocD_Q,documentD.DocD_Unite,documentD.DocD_PUHTB2,documentD.DocD_PUHTB1,documentD.DocD_PUHTB,documentD.DocD_PUTTCB,documentD.DocD_TxRem,documentD.DocD_Rem,documentD.DocD_TxRem1,documentD.DocD_Rem1,documentD.DocD_PUHTN,documentD.DocD_PUTTCN,documentD.DocD_TxTVA,documentD.DocD_TxTPF,documentD.DocD_TxDC,documentD.DocD_THT,documentD.DocD_TPF,documentD.DocD_DC,documentD.DocD_TVA,documentD.DocD_TTC,documentD.DocD_ArtPrix,documentD.DocD_QReliquat2,documentD.DocD_QReliquat1,documentD.DocD_QReliquat,documentD.DocD_NbVariante,documentD.DocD_Car1,documentD.DocD_Car2,documentD.DocD_Car3,documentD.DocD_Car4,documentD.DocD_Car5,documentD.DocD_Lot,documentD.DocD_Statut,documentD.DocD_Emp,documentD.DocD_TVACode,documentD.DocD_DCCode,documentD.DocD_TPFCode,documentD.DocD_IdOrigine,documentD.DocD_QP,documentD.DocD_QC,documentD.DocD_TVAC,documentD.DocD_TRem,documentD.DocD_PHTB,documentD.DocD_GesStock,documentD.DocD_PTTCBRef,documentD.DocD_PHTBRef,documentD.DocD_RemMax,documentD.DocD_Variante1,documentD.DocD_Variante2,documentD.DocD_Variante3,documentD.DocD_Variante4,documentD.DocD_Variante5,documentD.DocD_KitMAJPrix,documentD.DocD_ArtType,documentD.DocD_QLien,documentD.DocD_Lien,documentD.DocD_PUHDUV,documentD.DocD_PUHTNDL,documentD.DocD_VarianteTxt1,documentD.DocD_VarianteTxt2,documentD.DocD_VarianteTxt3,documentD.DocD_VarianteTxt4,documentD.DocD_VarianteTxt5,documentD.DocD_QF,documentD.DocD_QSolde,documentD.DocD_SoldeMotif,documentD.DocD_UniteP,documentD.DocD_UniteC,documentD.DocD_Date,documentD.DocD_ZONE,documentD.DocD_DCC,documentD.DocD_TPFC,documentD.DocD_Solde,documentD.DocD_MvtPMP,documentD.DocD_MvtQi,documentD.DocD_DateP,documentD.DocD_DateR,documentD.DocD_PoidsU,documentD.DocD_PoidsTotal,documentD.DocD_Annexe,documentD.DocD_AnnexeM,documentD.DocD_PUPMP,documentD.DocD_PUDPA,documentD.DataTransfert,documentD.Dibal_Num,documentD.Doc_CMD,documentD.DocD_QUC,documentD.DocD_QReliquat10,documentD.DocD_Surface,documentD.DocD_TxPU,documentD.xDocD_TxAvancement,documentD.xDocD_TypeTravaux,documentD.xDocD_Lot,documentD.xDocD_Pose,documentD.xDocD_Sens,documentD.xDocD_Ouverture,documentD.xDocD_Imposte,documentD.xDocD_CJH,documentD.xDocD_CJB,documentD.xDocD_CJD,documentD.xDocD_CJG,documentD.xDocD_Vitrage,documentD.xDocD_Obs,documentD.xDocD_TypeLame,documentD.xDocD_TypeM,documentD.xDocD_Lieu,documentD.xDocD_PJ,documentD.xDocD_TypeAxe,documentD.xDocD_TypeDVitrage,documentD.xDocD_Emplac1,documentD.xDocD_Emplac2,documentD.xDocD_Emplac3,documentD.xDocD_Emplac4,documentD.xDocD_Tiers,documentD.xDocDNaturePanne,documentD.xDocD_Surface2,documentD.xDocD_Supplement,documentD.xDocD_PrixSupplement,documentD.xDocD_NumOF,documentD.xDocD_Indice,documentD.xDocD_Gamme,documentD.xDocD_Serie,documentD.xDocD_Rabattement,documentD.xDocD_Galandage,documentD.xDocD_Encastrement,documentD.xDocD_PUM2,documentD.xDocD_PUPC,documentD.xIdProjet,documentD.long,documentD.larg,documentD.long2,documentD.larg2,documentD.xDocD_Dep,documentD.xDocD_Casse,documentD.xDocD_QtePrecedente,documentD.xDocD_PourcentagePrec,documentD.xDocD_Pex,documentD.xDocD_Subs,documentD.xDocD_RefSubs,documentD.xDocD_Perso,documentD.xDocD_CodePerso,documentD.xDocD_LieuLivraison,documentD.xDocD_Codechantier,documentD.xDocD_Nomchantier,documentD.xDocD_ArtPan,documentD.xDocD_DesPan,documentD.XDocD_MatVoiture,documentD.xDocD_NomVoiture,documentD.xDocD_NumCarte,documentD.xDocD_MaxCarte,documentD.DocD_Car1xx,documentD.DocD_Car2xx,documentD.DocD_Variante1xx,documentD.DocD_Variante2xx,documentD.xDocD_TS,documentD.xDocD_TA,documentD.DocD_PosId,documentD.DocD_UDA,documentD.DocD_DS,documentD.FP_REP,documentD.FP_OBS,documentD.Affaire_Code,documentD.xDocD_Q1,documentD.xDocD_NumBL,documentD.xDocD_Chauffeur,documentD.xDocD_ETATDOSSSUB,documentD.xDocD_OBSERSUB,documentD.xDocD_dateOBSERSUB,documentD.xDocD_MTSUB,documentD.xDocD_Confirmation,documentD.xDocD_Ref,documentD.DocD_Delai,documentD.DocD_Qualite,documentD.DocD_Cout,documentD.DocD_Quantite,documentD.DocD_Paiement,documentD.xDocD_Cause,documentD.xCautionCont,documentD.xContMtArgent,documentD.xDocD_QDiff,documentD.DocD_xTx,documentD.Art_Code_Origine,documentD.DocD_Car4_Origine,documentD.DocD_QSolde1,documentD.DocD_QSolde2,documentD.DocD_QSolde10,documentD.xDate_Cmd,documentD.xDate_cmdV,documentD.XDocD_Annee,documentD.xDocD_Demande,documentD.xDocD_Justif,documentD.xDocD_AffDemande,documentD.xDocD_Mnt13,documentD.DocD_RemD,documentD.x1,documentD.AL,documentD.VER,documentD.FRET,documentD.PCD,documentD.ACIN,documentD.FRETV,documentD.FRETPCD,documentD.xDoc_PUACFIMAJ,documentD.xDoc_PUACFIReeste,documentD.Claustrat,documentD.xRaison,documentD.CoffreTunel,documentD.Tole,documentD.OV,documentD.FOV,documentD.VR,documentD.FVR,documentD.DR,documentD.FDR,documentD.FRETCOF,documentD.FRETOLE,documentD.xDocD_TypeMR,documentD.x3,documentD.DocD_CreditMnt,documentD.xDocD_MntConge,documentD.xDocD_HSupp,documentD.XDocD_DateD,documentD.XDocD_DateF,documentD.xDocD_MntPrime,documentD.xDocD_MntSalSTC,documentD.xDocD_LigSect,documentD.xDocD_NatureBillet,documentD.xDocD_DestinationVol,documentD.xDocD_OrigineVol,documentD.xDocD_Emplac5\
        from (((volteqplus_monjia.dbo.OFD OFD INNER JOIN volteqplus_monjia.dbo.DocumentD DocumentD ON OFD.OFD_DocLigne=DocumentD.DocD_Id) INNER JOIN volteqplus_monjia.dbo.OFE OFE ON OFD.OF_Num=OFE.OF_Num) RIGHT OUTER JOIN volteqplus_monjia.dbo.Document Document ON DocumentD.Doc_Num=Document.Doc_Num) LEFT OUTER JOIN volteqplus_monjia.dbo.V_TYPELAME V_TYPELAME ON DocumentD.xDocD_TypeLame=V_TYPELAME.DiverSys_Index\
        WHERE  OFE.OFParam_Code=N'OF' and document.Doc_Num='"+doc_num+"'  and ofd.ofd_id='"+id+"'"
        df2 = pd.read_sql_query(req,self.cnx)
        qte=float(qte)
        df=self.getEXStatus(df2['tiers_code'][0])
        df2=df2.drop(['tiers_code'], axis=1)
        print(df['Tiers_Exo_Deb'][0])
        if (df['Tiers_Exo_Deb'][0] == None):
            ts = Timestamp("1999-01-01 00:00:00")
        else:
            ts = Timestamp(df['Tiers_Exo_Deb'][0])
        date_time = str(ts).split()[0]
        if (df['Tiers_Exo_Deb'][0] == None):
            ts1 = Timestamp("1999-01-02 00:00:00")
        else:
            ts1 = Timestamp(df['Tiers_Exo_Fin'][0])
        date_time = str(ts).split()[0]  
        date_time1 = str(ts1).split()[0]
        datenow=str(datetime.now()).split()[0]
        date_3 = datetime.strptime(datenow, '%Y-%m-%d').date()
        date_1 = datetime.strptime(date_time, '%Y-%m-%d').date()
        date_2 =datetime.strptime(date_time1, '%Y-%m-%d').date()
    
                    
        if df['Tiers_ExoStatut'][0]== 0 :
            puHT=df2['DocD_PUHTN']*qte
            tva=(df2['DocD_PUHTN']*(19/100))*qte
            if  df2['DocD_TxTPF'][0] is None :
                df2['DocD_TxTPF']=1
                fodek=(df2['DocD_PUHTN']*(df2['DocD_TxTPF']/100))*qte
                fodekU=df2['DocD_PUHTN']*(df2['DocD_TxTPF']/100)
            else :
                fodek=(df2['DocD_PUHTN']*(df2['DocD_TxTPF']/100))*qte
                fodekU=df2['DocD_PUHTN']*(df2['DocD_TxTPF']/100)
            ttc=puHT+fodek+tva
            uht=df2['DocD_PUHTN']+fodekU+tva

            df2[['DocD_Q','DocD_QReliquat2','DocD_QReliquat1','DocD_QReliquat','DocD_QF','DocD_QP','DocD_QC','DocD_QReliquat10','DocD_QSolde1','DocD_QSolde2','DocD_QSolde10','xDocD_Q1','DocD_QUC','DocD_QLien']]=qte
            df2['DocD_IdOrigine']=df2['DocD_Id']
            df2['DocD_PHTB']= puHT
            df2['DocD_THT']= puHT
            df2['DocD_TPF']=fodek
            df2['DocD_TPFC']=fodek
            df2['DocD_TVA']=tva
            df2['DocD_TVAC']=tva
            df2['DocD_TTC']=ttc 
            df2['DocD_PUTTCN']=uht
            df2['DocD_PUTTCB']=uht
            df2['Doc_Num']=self.creationDoc_Num()
            strcpt=str(i).zfill(4)+"-"+df2['Doc_Num']
            df2['DocDOrigine']=doc_num
            df2['DocD_Id']=strcpt
            return  df2
        if df['Tiers_ExoStatut'][0]== 1 :
            if date_1 < date_3 < date_2:
                puHT=df2['DocD_PUHTN']*qte
                tva=0
                if  df2['DocD_TxTPF'][0] is None :
                    df2['DocD_TxTPF']=1
                    fodek=(df2['DocD_PUHTN']*(df2['DocD_TxTPF']/100))*qte
                    fodekU=df2['DocD_PUHTN']*(df2['DocD_TxTPF']/100)
                else :
                    fodek=(df2['DocD_PUHTN']*(1/100))*qte
                    fodekU=df2['DocD_PUHTN']*(1/100)
                ttc=puHT+fodek+tva
                uht=df2['DocD_PUHTN']+fodekU+tva
                df2[['DocD_Q','DocD_QReliquat2','DocD_QReliquat1','DocD_QReliquat','DocD_QF','DocD_QP','DocD_QC','DocD_QReliquat10','DocD_QSolde1','DocD_QSolde2','DocD_QSolde10','xDocD_Q1','DocD_QUC','DocD_QLien']]=qte
                df2['DocD_IdOrigine']=df2['DocD_Id']
                df2['DocD_PHTB']= puHT
                df2['DocD_THT']= puHT
                df2['DocD_TPF']=fodek
                df2['DocD_TPFC']=fodek
                df2['DocD_TVA']=0
                df2['DocD_TxTVA']=0
                df2['DocD_TVAC']=0
                df2['DocD_PUTTCB']=uht
                df2['DocD_TTC']=ttc
                df2['DocD_PUTTCN']=uht # a corriger TTC
                df2['Doc_Num']=self.creationDoc_Num()
                strcpt=i.zfill(4)+"-"+df2['Doc_Num']
                df2['DocDOrigine']=doc_num
                df2['DocD_Id']=strcpt
                return  df2
        if df['Tiers_ExoStatut'][0] == 2 :
            if date_1 < date_3 < date_2 :
                puHT=df2['DocD_PUHTN']*qte
                tva=0
                if  df2['DocD_TxTPF'][0] is None :
                    df2['DocD_TxTPF']=0
                    fodek=0
                    fodekU=0
                else :
                    fodek=0
                    fodekU=0
                ttc=puHT+fodek+tva
                uht=df2['DocD_PUHTN']+fodekU+tva
                df2[['DocD_Q','DocD_QReliquat2','DocD_QReliquat1','DocD_QReliquat','DocD_QF','DocD_QP','DocD_QC','DocD_QReliquat10','DocD_QSolde1','DocD_QSolde2','DocD_QSolde10','xDocD_Q1','DocD_QUC','DocD_QLien']]=qte
                df2['DocD_IdOrigine']=df2['DocD_Id']
                df2['DocD_PHTB']= puHT
                df2['DocD_THT']= puHT
                df2['DocD_TPF']=fodek
                df2['DocD_TPFC']=fodek
                df2['DocD_TVA']=tva
                df2['DocD_TxTVA']=0
                df2['DocD_PUTTCB']=uht
                df2['DocD_TVAC']=tva
                df2['DocD_PUTTCN']=uht
                df2['DocD_TTC']=ttc # a corriger TTC
                df2['Doc_Num']=self.creationDoc_Num()
                strcpt=i.zfill(4)+"-"+df2['Doc_Num']
                df2['DocDOrigine']=doc_num
                df2['DocD_Id']=strcpt
    
                return  df2
        if (df['Tiers_ExoStatut'][0]==2   and  (date_3 > date_2))|(df['Tiers_ExoStatut'][0]==1   and  (date_3 > date_2)):
            puHT=df2['DocD_PUHTN']*qte
            tva=(df2['DocD_PUHTN']*(19/100))*qte
            if  df2['DocD_TxTPF'][0] is None :
                df2['DocD_TxTPF']=1
                fodek=(df2['DocD_PUHTN']*(df2['DocD_TxTPF']/100))*qte
                fodekU=df2['DocD_PUHTN']*(df2['DocD_TxTPF']/100)
            else :
                fodek=(df2['DocD_PUHTN']*(df2['DocD_TxTPF']/100))*qte
                fodekU=df2['DocD_PUHTN']*(df2['DocD_TxTPF']/100)
            ttc=puHT+fodek+tva
            uht=df2['DocD_PUHTN']+fodekU+tva
            df2[['DocD_Q','DocD_QReliquat2','DocD_QReliquat1','DocD_QReliquat','DocD_QF','DocD_QP','DocD_QC','DocD_QReliquat10','DocD_QSolde1','DocD_QSolde2','DocD_QSolde10','xDocD_Q1','DocD_QUC','DocD_QLien']]=qte
            df2['DocD_IdOrigine']=df2['DocD_Id']
            df2['DocD_PHTB']= puHT
            df2['DocD_THT']= puHT
            df2['DocD_TPF']=fodek
            df2['DocD_TPFC']=fodek
            df2['DocD_TVA']=tva
            df2['DocD_TVAC']=tva
            df2['DocD_PUTTCN']=uht
            df2['DocD_PUTTCB']=uht
            df2['DocD_TTC']=ttc # a corriger TTC
            df2['Doc_Num']=self.creationDoc_Num()
            strcpt=i.zfill(4)+"-"+df2['Doc_Num']
            df2['DocDOrigine']=doc_num
            df2['DocD_Id']=strcpt
            return  df2  
    def updateCpt(self):
    #req  Cpt_num+1
        today = date.today()
        annee= today.strftime('%Y')
        insertCpt=f"update volteqplus_monjia..cpt\
            set cpt_num=cpt_num+1\
            where cpt_doc=   (\
            select cpt_doc from volteqplus_monjia..cpt where cpt_doc like 'BLVR' and cpt_annee={annee}) and cpt_annee={annee}"
        cursor = self.cnx.cursor(as_dict=True)
        cursor.execute(insertCpt)
        self.cnx.commit()
        #select cpt num
        reqCpt_num=f"select cpt_num from volteqplus_monjia.dbo.cpt where cpt_doc= ( select cpt_doc from volteqplus_monjia.dbo.cpt where cpt_doc like 'BLVR' and cpt_annee={annee} )and cpt_annee={annee} "
        cursor = self.cnx.cursor(as_dict=True)
        cursor.execute(reqCpt_num)
        getCpt_num = cursor.fetchall()
        cpt_num=getCpt_num[0]['cpt_num']
        return cpt_num
    def updateqte(self,id,qte):
        q=str(qte)
        req="update  volteqplus_monjia.dbo.DocumentD\
        set DocumentD.DocD_QReliquat1=DocumentD.DocD_QReliquat1-"+q+" from\
        volteqplus_monjia.dbo.OFD as OFD INNER JOIN volteqplus_monjia.dbo.DocumentD as  DocumentD ON OFD.OFD_DocLigne=DocumentD.DocD_Id\
        where  OFD.ofd_id='"+id+"' and OFD.Art_Code='VR'" 
        cursor = self.cnx.cursor(as_dict=True)
        cursor.execute(req)
        self.cnx.commit()
        return 'true'
    

    


    