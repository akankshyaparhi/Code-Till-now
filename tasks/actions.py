"""
Author: "Akshat Tickoo"
Email: "atickoo@deloitte.com"
"""

import sqlalchemy as db
from datetime import datetime
from dotenv import load_dotenv
from os import environ
import pandas as pd
import sys

load_dotenv()
src_path = environ.get('src-path')
sys.path.append(src_path)
from src.utils.connection import DBType
from src.tasks.match import Match
from src.tasks.mssql import MSSQL

MSSQLDRIVER = environ.get('sqlserverdriver')
USERNAME = environ.get('sqlserveruser')
PASSWORD = environ.get('sqlserverpass')
GETDATE = datetime.now()
delin_server = environ.get('delineator-server')
delin_port = environ.get('delineator-port')


class Actions:
    def __init__(self):
        pass

    def get_db(self, params, db_type):
        engine = DBType(params, db_type).connection()
        try:
            connection = engine.connect()
        except db.exc.InterfaceError:
            print('Connection could not be established for the user ' + params['username'])
            return 'Connection could not be established for the user ' + params['username']
        else:
            metadata = db.MetaData()
            return engine, connection, metadata

    def get_max_id(self, engine, connection, metadata):
        config = db.Table('CONFIGURATIONDETAILS', metadata, autoload=True, autoload_with=engine)
        id_q = db.select([db.func.max(config.columns.REQUEST_ID)]).where(config.columns.PROCESS_FLAG == 'N')
        max_id = connection.execute(id_q).fetchall()[0][0]
        return max_id

    def get_db_details(self, engine, connection, metadata, request_id):
        config = db.Table('CONFIGURATIONDETAILS', metadata, autoload=True, autoload_with=engine)
        query = db.select(
            [config.columns.SOURCE_SERVER, config.columns.SOURCE_DB, config.columns.SOURCE_PORT,
             config.columns.SOURCE_TYPE,
             config.columns.SOURCE_SCHEMA_NAME, config.columns.SOURCEDB_USERID, config.columns.SOURCEDB_PASSWORD,
             config.columns.TARGET_SERVER, config.columns.TARGET_DB, config.columns.TARGET_PORT,
             config.columns.TARGET_TYPE,
             config.columns.TARGET_SCHEMA_NAME, config.columns.TARGETDB_USERID,
             config.columns.TARGETDB_PASSWORD]).where(
            config.columns.REQUEST_ID == request_id)
        db_details = connection.execute(query).fetchall()
        return db_details

    def get_source_db_metadata(self, config_db_details, request_id):
        src_server = config_db_details[0][0]
        src_db = config_db_details[0][1]
        src_port = config_db_details[0][2]
        src_type = config_db_details[0][3].lower().replace(" ", "")
        src_user = config_db_details[0][5]
        src_pass = config_db_details[0][6]
        params = {'username': src_user, 'password': src_pass, 'host': src_server, 'port': src_port,
                  'database': src_db, 'driver': MSSQLDRIVER}
        engine, connection, metadata = self.get_db(params=params, db_type=src_type)
        inspector = db.inspect(engine)

        metadata_list = []
        for table in inspector.get_table_names():
            pk_keys = inspector.get_pk_constraint(table)
            unique_keys = MSSQL().get_unique_keys(connection, table)
            for column in inspector.get_columns(table):
                if pk_keys['constrained_columns']:
                    if pk_keys['constrained_columns'][0] == column['name']:
                        metadata_dict = {'REQUEST_ID': request_id, 'Source_TableName': table,
                                         'Source_ColumnName': column['name'], 'IsPrimary_Key': 'Y',
                                         'Createdby': USERNAME, 'CreatedDate': GETDATE, 'Is_UniqueKey': 'N'}
                    else:
                        metadata_dict = {'REQUEST_ID': request_id, 'Source_TableName': table,
                                         'Source_ColumnName': column['name'], 'IsPrimary_Key': 'N',
                                         'Createdby': USERNAME, 'CreatedDate': GETDATE, 'Is_UniqueKey': 'N'}
                else:
                    metadata_dict = {'REQUEST_ID': request_id, 'Source_TableName': table,
                                     'Source_ColumnName': column['name'], 'IsPrimary_Key': 'N', 'Createdby': USERNAME,
                                     'CreatedDate': GETDATE, 'Is_UniqueKey': 'N'}

                for i, v in unique_keys['Column_Name'].items():
                    if column['name'] == unique_keys['Column_Name'][i]:
                        metadata_dict['Is_UniqueKey'] = 'Y'

                metadata_list.append(metadata_dict)

        return metadata_list

    def get_target_db_metadata(self, config_db_details, request_id):
        tgt_server = config_db_details[0][7]
        tgt_db = config_db_details[0][8]
        tgt_port = config_db_details[0][9]
        tgt_type = config_db_details[0][10].lower().replace(" ", "")
        tgt_user = config_db_details[0][12]
        tgt_pass = config_db_details[0][13]
        params = {'username': tgt_user, 'password': tgt_pass, 'host': tgt_server, 'port': tgt_port,
                  'database': tgt_db, 'driver': MSSQLDRIVER}
        engine, connection, metadata = self.get_db(params=params, db_type=tgt_type)
        inspector = db.inspect(engine)

        metadata_list = []
        for table in inspector.get_table_names():
            pk_keys = inspector.get_pk_constraint(table)
            unique_keys = MSSQL().get_unique_keys(connection, table)
            for column in inspector.get_columns(table):
                if pk_keys['constrained_columns']:
                    if pk_keys['constrained_columns'][0] == column['name']:
                        metadata_dict = {'REQUEST_ID': request_id, 'Target_TableName': table,
                                         'TargetTable_ColumnName': column['name'], 'Is_PrimaryKey': 'Y',
                                         'Createdby': USERNAME, 'CreatedDate': GETDATE, 'Is_UniqueKey': 'N'}
                    else:
                        metadata_dict = {'REQUEST_ID': request_id, 'Target_TableName': table,
                                         'TargetTable_ColumnName': column['name'], 'Is_PrimaryKey': 'N',
                                         'Createdby': USERNAME, 'CreatedDate': GETDATE, 'Is_UniqueKey': 'N'}
                else:
                    metadata_dict = {'REQUEST_ID': request_id, 'Target_TableName': table,
                                     'TargetTable_ColumnName': column['name'], 'Is_PrimaryKey': 'N',
                                     'Createdby': USERNAME, 'CreatedDate': GETDATE, 'Is_UniqueKey': 'N'}

                for i, v in unique_keys['Column_Name'].items():
                    if column['name'] == unique_keys['Column_Name'][i]:
                        metadata_dict['Is_UniqueKey'] = 'Y'

                metadata_list.append(metadata_dict)

        return metadata_list

    def insert_into_sourcemetadata(self, engine, connection, metadata, config_db_details, request_id):
        srcmetadata = db.Table('SourceMetaData', metadata, autoload=True, autoload_with=engine)
        db_list = self.get_source_db_metadata(config_db_details, request_id)
        connection.execute(srcmetadata.insert(), db_list)

    def insert_into_targetmetadata(self, engine, connection, metadata, config_db_details, request_id):
        tgtmetadata = db.Table('TargetMetaData', metadata, autoload=True, autoload_with=engine)
        db_list = self.get_target_db_metadata(config_db_details, request_id)
        connection.execute(tgtmetadata.insert(), db_list)

    def compare_src_tgt(self, engine, connection, metadata, request_id):
        srcmetadata = db.Table('SourceMetaData', metadata, autoload=True, autoload_with=engine)
        src_q = db.select([srcmetadata.columns.Source_TableName, srcmetadata.columns.Source_ColumnName,
                           srcmetadata.columns.IsPrimary_Key, srcmetadata.columns.Is_UniqueKey]).where(
            srcmetadata.columns.REQUEST_ID == request_id).where(srcmetadata.columns.MappingRequested == 'Y')
        srcmetadata_df = pd.DataFrame(connection.execute(src_q).fetchall(),
                                      columns=['Source_TableName', 'Source_ColumnName', 'Source_IsPrimaryKey',
                                               'Source_IsUniqueKey'])

        tgtmetadata = db.Table('TargetMetaData', metadata, autoload=True, autoload_with=engine)
        tgt_q = db.select([tgtmetadata.columns.Target_TableName, tgtmetadata.columns.TargetTable_ColumnName,
                           tgtmetadata.columns.Is_PrimaryKey, tgtmetadata.columns.Is_UniqueKey]).where(
            tgtmetadata.columns.REQUEST_ID == request_id).where(tgtmetadata.columns.MappingRequested == 'Y')
        tgtmetadata_df = pd.DataFrame(connection.execute(tgt_q).fetchall(),
                                      columns=['Target_TableName', 'Target_ColumnName', 'Target_IsPrimaryKey',
                                               'Target_IsUniqueKey'])

        join_df = pd.merge(srcmetadata_df, tgtmetadata_df, left_on=['Source_TableName', 'Source_ColumnName'],
                           right_on=['Target_TableName', 'Target_ColumnName'], how='right')
        join_df = join_df.where(pd.notnull(join_df), None)
        return join_df

    def insert_into_srctgtmapping(self, engine, connection, metadata, request_id):
        srctgtmapping = db.Table('SourceTargetMapping', metadata, autoload=True, autoload_with=engine)

        join_df = self.compare_src_tgt(engine, connection, metadata, request_id)
        join_df['Match_Percentage'] = ['0' if x is None else '100' for x in join_df['Source_TableName']]
        join_df['Createdby'] = USERNAME
        join_df['CreatedDate'] = GETDATE
        join_df['REQUEST_ID'] = request_id
        join_list = join_df.to_dict('records')
        connection.execute(srctgtmapping.insert(), join_list)

    def compare_hist_data(self, engine, connection, metadata, request_id):
        srctgtmapping = db.Table('SourceTargetMapping', metadata, autoload=True, autoload_with=engine)

        unmatched_q = db.select(
            [srctgtmapping.columns.Target_TableName, srctgtmapping.columns.Target_ColumnName]).where(
            srctgtmapping.columns.REQUEST_ID == request_id).where(srctgtmapping.columns.Match_Percentage != '100')
        unmatched_df = pd.DataFrame(connection.execute(unmatched_q).fetchall(),
                                    columns=['Target_TableName', 'Target_ColumnName'])

        if not unmatched_df.empty:
            for index, row in unmatched_df.iterrows():
                search_q = db.select([srctgtmapping.columns.Source_TableName, srctgtmapping.columns.Source_ColumnName,
                                      srctgtmapping.columns.Source_IsPrimaryKey,
                                      srctgtmapping.columns.Match_Percentage]).where(
                    srctgtmapping.columns.REQUEST_ID < request_id).where(
                    srctgtmapping.columns.Target_TableName == row['Target_TableName']).where(
                    srctgtmapping.columns.Target_ColumnName == row['Target_ColumnName'])
                search_rs = connection.execute(search_q).fetchone()
                if search_rs:
                    unmatched_df.loc[index, 'Source_TableName'] = search_rs[0]
                    unmatched_df.loc[index, 'Source_ColumnName'] = search_rs[1]
                    unmatched_df.loc[index, 'Source_IsPrimaryKey'] = search_rs[2]
                    unmatched_df.loc[index, 'Match_Percentage'] = search_rs[3]

            matched_df = unmatched_df[unmatched_df['Source_TableName'].notnull()]
        else:
            matched_df = unmatched_df

        if not matched_df.empty:
            for index, row in matched_df.iterrows():
                update_q = srctgtmapping.update().where(srctgtmapping.columns.REQUEST_ID == request_id).where(
                    srctgtmapping.columns.Target_TableName == row['Target_TableName']).where(
                    srctgtmapping.columns.Target_ColumnName == row['Target_ColumnName']).values(
                    Source_TableName=matched_df.loc[index, 'Source_TableName'],
                    Source_ColumnName=matched_df.loc[index, 'Source_ColumnName'],
                    Source_IsPrimaryKey=matched_df.loc[index, 'Source_IsPrimaryKey'],
                    Match_Percentage=matched_df.loc[index, 'Match_Percentage'])
                connection.execute(update_q)

    def update_config_id(self, engine, connection, metadata, request_id):
        config = db.Table('CONFIGURATIONDETAILS', metadata, autoload=True, autoload_with=engine)
        update_q = config.update().where(config.columns.REQUEST_ID == request_id).values(PROCESS_FLAG='Y')
        connection.execute(update_q)

    def main(self):
        params = {'username': USERNAME, 'password': PASSWORD, 'host': delin_server, 'port': delin_port,
                  'database': 'DelineatorDB', 'driver': MSSQLDRIVER}
        engine, connection, metadata = self.get_db(params=params, db_type='mssql')

        max_id = self.get_max_id(engine, connection, metadata)
        config_db_details = self.get_db_details(engine, connection, metadata, max_id)

        self.insert_into_sourcemetadata(engine, connection, metadata, config_db_details, max_id)
        self.insert_into_targetmetadata(engine, connection, metadata, config_db_details, max_id)
        self.insert_into_srctgtmapping(engine, connection, metadata, max_id)
        self.compare_hist_data(engine, connection, metadata, max_id)
        self.update_config_id(engine, connection, metadata, max_id)
        Match().main()
        return 'Success'


if __name__ == '__main__':
    Actions().main()
