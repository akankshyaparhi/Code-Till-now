import sqlalchemy as db
import urllib
import sqlalchemy_teradata


class DBType:
    def __init__(self, params, db_type):
        self.params = params
        self.db_type = db_type

    def connection(self):
        if self.db_type == 'mssql':
            drivername = 'mssql+pyodbc'
            param_url = db.engine.url.URL(drivername=drivername, username=self.params['username'],
                                          password=self.params['password'],
                                          host=self.params['host'],
                                          port=self.params['port'],
                                          database=self.params['database'],
                                          query=dict(driver=urllib.parse.quote_plus(self.params['driver'])))

        elif self.db_type == 'oracle':
            drivername = 'oracle'
            param_url = db.engine.url.URL(drivername=drivername, username=self.params['username'],
                                          password=self.params['password'],
                                          host=self.params['host'],
                                          port=self.params['port'],
                                          database=self.params['database'],
                                          query=dict(driver=urllib.parse.quote_plus(self.params['driver'])))

        elif self.db_type == 'teradata':
            drivername = 'teradata'
            param_url = db.engine.url.URL(drivername=drivername, username=self.params['username'],
                                          password=self.params['password'],
                                          host=self.params['host'],
                                          database=self.params['database'])
            print(param_url)

        engine = db.create_engine(str(param_url))
        return engine
