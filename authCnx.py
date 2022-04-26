import ldap3



class LDAPCNX:
    def __init__(self):
        self.conn=None
        self.result_=False
    def Connecter(self,username,password):        
        try:
            self.conn = ldap3.Connection('ldap://192.168.10.200', user=username+'@mas.local', password=password)
            self.result_ = self.conn.bind()
            return self.result_
        except Exception as e:
            return self.result_