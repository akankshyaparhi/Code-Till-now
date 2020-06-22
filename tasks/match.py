from fuzzywuzzy import process
import pandas as pd
import pyodbc
from dotenv import load_dotenv
import os
from datetime import datetime


class Match:
    def __init__(self,request_id):
        self.request_id=request_id
    # Get the matching values
    def match(self, inputval, MatchingList, matchingthreshold, matchtype=1):
        if matchtype == 1:
            y = []
            x = process.extract(inputval, MatchingList)
            # print(x)
            for i in range(len(process.extract(inputval, MatchingList))):
                for j in x[i]:
                    if x[i][1] > matchingthreshold and x[i][0] not in y:
                        y.append(x[i][0])
                        # print(x[i][1])

            return y
        else:
            y = []
            x = (process.extractOne(inputval, MatchingList))[0]
            y.append(x)
            return y

    # Get Approximate match based on Levenshtein

    def approxMatch(self, inputval, inputColNm, Parent_ID, cur, dfDestination, qry, matchingthreshold):
        df1 = dfDestination
        MatchingList = df1['Source_TableName'].tolist()
        Rdf = df1.loc[df1['Source_TableName'].isin(self.match(inputval, MatchingList, matchingthreshold, 1))]
        ColumnMatchingList = Rdf['Source_ColumnName'].tolist()
        CreatedDate = str(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        Finallist = []

        for i in range(len(self.match(inputval, MatchingList, matchingthreshold, 1))):
            for j in range(len(self.match(inputColNm, ColumnMatchingList, matchingthreshold, 1))):
                Finallist.append(tuple((
                    Parent_ID,
                    inputval,
                    inputColNm,
                    self.match(inputval, MatchingList, matchingthreshold, 1)[i],
                    self.match(inputColNm, ColumnMatchingList, matchingthreshold, 1)[j],
                    process.extract(inputColNm, ColumnMatchingList)[j][1],
                    'N',
                    'ML',
                    CreatedDate
                ))
                )

        FinalMatch = pd.DataFrame(list(Finallist), columns=[Parent_ID,
                                                            'SourceTableName',
                                                            'SourceColumnName',
                                                            'DestinationTableName',
                                                            'DestinationColumnName',
                                                            'Match_Percentage',
                                                            'N',
                                                            'ML',
                                                            CreatedDate])
        # print(Finallist)

        for i in range(len(Finallist)):
            cur.execute(qry, Finallist[i])
            print('{0} row inserted successfully.'.format(cur.rowcount))
            cur.commit()
        return FinalMatch

    def main(self):
        print(self.request_id)
        print("done")
        
        # Parameters
        load_dotenv()
        DRIVER = os.environ.get('sqlserverdriver')
        server = os.environ.get('delineator-server')
        db = 'DelineatorDB'
        username = os.environ.get('sqlserveruser')
        password = os.environ.get('sqlserverpass')

   
        if DRIVER == 'SQL Server':
            connection_sting = ('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
        elif DRIVER == 'Oracle':
            connection_sting = ('DRIVER={Oracle in XE};SERVER=' + server + 'uid=' + username + ';pwd=' + password)
        else:
            print("invalid connection")

        conn = pyodbc.connect(connection_sting)
        cur = conn.cursor()
        cur1 = conn.cursor()


        PaentIDQuery = '''
            SELECT  MAX(REQUEST_ID) FROM CONFIGURATIONDETAILS
            '''
        cur1.execute(PaentIDQuery)
        Parent_ID1 = cur1.fetchone()
        cur1.commit()
        Parent_ID = Parent_ID1[0]
        sqlSource = """
            SELECT REQUEST_ID
                  ,Source_TableName
                  ,SOURCE_COLUMNNAME
                  ,TARGET_TABLENAME
                  ,TARGET_COLUMNNAME
                  ,MATCH_PERCENTAGE
                  ,CREATEDBY
                  ,CREATEDDATE
              FROM SOURCETARGETMAPPING
              WHERE MATCH_PERCENTAGE <> 100
              AND REQUEST_ID = (SELECT  MAX(REQUEST_ID) FROM CONFIGURATIONDETAILS)
            """

        sqlDestination = """
    
            SELECT REQUEST_ID
              ,Source_TableName
              ,Source_ColumnName
              ,CREATEDBY
              ,CREATEDDATE
              FROM SourceMetaData
              WHERE REQUEST_ID = (SELECT  MAX(REQUEST_ID) FROM CONFIGURATIONDETAILS)
    
            """
        if DRIVER == 'SQL Server':
            qry = '''INSERT INTO SOURCETARGETDECISION
                  (REQUEST_ID
                  ,Source_TableName
                  ,SOURCE_COLUMNNAME
                  ,TARGET_TABLENAME
                  ,TARGET_COLUMNNAME
                  ,THRESHOLD
                  ,DECISION
                  ,CREATEDBY
                  ,CREATEDDATE
                  )
                VALUES(?,?,?,?,?,?,?,?,?)
                '''

        elif DRIVER == 'Oracle':
            qry = '''INSERT INTO SOURCETARGETDECISION
                  (REQUEST_ID
                  ,Source_TableName
                  ,SOURCE_COLUMNNAME
                  ,TARGET_TABLENAME
                  ,TARGET_COLUMNNAME
                  ,THRESHOLD
                  ,DECISION
                  ,CREATEDBY
                  ,CREATEDDATE
                  )
                VALUES(?,?,?,?,?,?,?,?,TO_DATE(?,'yyyy-mm-dd'))
                '''

        else:
            print("DB not supported")

        dfSource = pd.read_sql(sqlSource, conn)
        dfDestination = pd.read_sql(sqlDestination, conn)
        # loading Metadata info
        tablelist = dfSource['TARGET_TABLENAME'].tolist()
        columnlist = dfSource['TARGET_COLUMNNAME'].tolist()
        matchingthreshold = 0

        for i in range(len(tablelist)):
            # Input Table Name
            inputTableNm = tablelist[i]
            # Input Column Name
            inputColNm = columnlist[i]
            # print('---------------Approx Match-------------------------------------------')
            self.approxMatch(inputTableNm, inputColNm, Parent_ID, cur, dfDestination, qry, matchingthreshold)

        conn.close()


if __name__ == '__main__':
    
    Match().main()
