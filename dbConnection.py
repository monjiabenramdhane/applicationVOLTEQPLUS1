import pymssql
 
class Connection:
    def __init__(self):
        self.conn=None
    def connect(self,hostAdress,u,p,db):
        self.conn=pymssql.connect(server=hostAdress, user=u, password=p, database=db)
    def closeCnx(self):
        self.conn.close()
    def getCnx(self):
        return self.conn
