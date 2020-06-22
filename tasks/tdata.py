import sqlalchemy as db
from datetime import datetime
from dotenv import load_dotenv
from os import environ
import pandas as pd
import sys

load_dotenv()
src_path = environ.get('src-path')
sys.path.append(src_path)
from src.tasks.actions import Actions

TDDRIVER = environ.get('tddriver')

MSSQLDRIVER = environ.get('sqlserverdriver')
USERNAME = environ.get('sqlserveruser')
PASSWORD = environ.get('sqlserverpass')
GETDATE = datetime.now()
delin_server = environ.get('delineator-server')
delin_port = environ.get('delineator-port')


class TD_metadata():

    def __init__(self):
        pass

    def get_source_db_metadata(self, config_db_details, request_id):
        src_server = config_db_details[0][0]
        src_db = config_db_details[0][1]
        src_port = config_db_details[0][2]
        src_type = config_db_details[0][3].lower().replace(" ", "")
        src_user = config_db_details[0][5]
        src_pass = config_db_details[0][6]
        params = {'username': src_user, 'password': src_pass, 'host': src_server, 'port': src_port,
                  'database': src_db, 'driver': TDDRIVER}
        engine, connection, metadata = Actions().get_db(params=params, db_type=src_type)

        get_col_nm = "select TableName, ColumnName from DBC.COLUMNS WHERE DatabaseName='" + src_db + "'"
        tbl_col_nm = connection.execute(get_col_nm).fetchall()
        get_index = "select TableName, ColumnName from DBC.INDICES WHERE DatabaseName='" + src_db + "' and IndexType='P';"
        print(get_index)
        index = connection.execute(get_index).fetchall()
        get_unique = "select TableName, ColumnName from dbc.indices where UniqueFlag='Y' and DataBaseName='" + src_db + "'"
        unique = connection.execute(get_unique).fetchall()
        print(request_id)
        metadata_list = []

        for rec in tbl_col_nm:
            if rec in index and rec in unique:
                # print(rec)
                metadata_dict = {'REQUEST_ID': request_id, 'Source_TableName': rec[0].strip(),
                                 'Source_ColumnName': rec[1].strip(), 'IsPrimary_Key': 'Y', 'Createdby': USERNAME,
                                 'CreatedDate': GETDATE, 'Is_UniqueKey': 'Y'}
            elif rec in index and rec not in unique:
                # print(rec)
                metadata_dict = {'REQUEST_ID': request_id, 'Source_TableName': rec[0].strip(),
                                 'Source_ColumnName': rec[1].strip(), 'IsPrimary_Key': 'Y', 'Createdby': USERNAME,
                                 'CreatedDate': GETDATE, 'Is_UniqueKey': 'N'}
            elif rec not in index and rec in unique:
                # print(rec)
                metadata_dict = {'REQUEST_ID': request_id, 'Source_TableName': rec[0].strip(),
                                 'Source_ColumnName': rec[1].strip(), 'IsPrimary_Key': 'N', 'Createdby': USERNAME,
                                 'CreatedDate': GETDATE, 'Is_UniqueKey': 'Y'}

            else:
                metadata_dict = {'REQUEST_ID': request_id, 'Source_TableName': rec[0].strip(),
                                 'Source_ColumnName': rec[1].strip(), 'IsPrimary_Key': 'N', 'Createdby': USERNAME,
                                 'CreatedDate': GETDATE, 'Is_UniqueKey': 'N'}
            metadata_list.append(metadata_dict.copy())

        return metadata_list

    def get_target_db_metadata(self, config_db_details, request_id):
        tgt_server = config_db_details[0][7]
        tgt_db = config_db_details[0][8]
        tgt_port = config_db_details[0][9]
        tgt_type = config_db_details[0][10].lower().replace(" ", "")
        tgt_user = config_db_details[0][12]
        tgt_pass = config_db_details[0][13]
        params = {'username': tgt_user, 'password': tgt_pass, 'host': tgt_server, 'port': tgt_port,
                  'database': tgt_db, 'driver': TDDRIVER}
        engine, connection, metadata = Actions().get_db(params=params, db_type=tgt_type)

        get_col_nm = "select TableName, ColumnName from DBC.COLUMNS WHERE DatabaseName='" + tgt_db + "'"
        tbl_col_nm = connection.execute(get_col_nm).fetchall()
        get_index = "select TableName, ColumnName from DBC.INDICES WHERE DatabaseName='" + tgt_db + "' and IndexType='P';"
        index = connection.execute(get_index).fetchall()
        get_unique = "select TableName, ColumnName from dbc.indices where UniqueFlag='Y' and DataBaseName='" + tgt_db + "'"
        unique = connection.execute(get_unique).fetchall()
        print(request_id)
        metadata_list = []

        for rec in tbl_col_nm:
            if rec in index and rec in unique:
                # print(rec)
                metadata_dict = {'REQUEST_ID': request_id, 'Target_TableName': rec[0].strip(),
                                 'TargetTable_ColumnName': rec[1].strip(), 'Is_PrimaryKey': 'Y', 'Createdby': USERNAME,
                                 'CreatedDate': GETDATE, 'Is_UniqueKey': 'Y'}
            elif rec in index and rec not in unique:
                # print(rec)
                metadata_dict = {'REQUEST_ID': request_id, 'Target_TableName': rec[0].strip(),
                                 'TargetTable_ColumnName': rec[1].strip(), 'Is_PrimaryKey': 'Y', 'Createdby': USERNAME,
                                 'CreatedDate': GETDATE, 'Is_UniqueKey': 'N'}
            elif rec not in index and rec in unique:
                # print(rec)
                metadata_dict = {'REQUEST_ID': request_id, 'Target_TableName': rec[0].strip(),
                                 'TargetTable_ColumnName': rec[1].strip(), 'Is_PrimaryKey': 'N', 'Createdby': USERNAME,
                                 'CreatedDate': GETDATE, 'Is_UniqueKey': 'Y'}

            else:
                metadata_dict = {'REQUEST_ID': request_id, 'Target_TableName': rec[0].strip(),
                                 'TargetTable_ColumnName': rec[1].strip(), 'Is_PrimaryKey': 'N', 'Createdby': USERNAME,
                                 'CreatedDate': GETDATE, 'Is_UniqueKey': 'N'}
            metadata_list.append(metadata_dict.copy())

        print(len(metadata_list))
        return metadata_list

    def insert_into_sourcemetadata(self, engine, connection, metadata, config_db_details, request_id):
        srcmetadata = db.Table('SourceMetaData', metadata, autoload=True, autoload_with=engine)
        db_list = self.get_source_db_metadata(config_db_details, request_id)
        print(db_list)
        connection.execute(srcmetadata.insert(), db_list)

    def insert_into_targetmetadata(self, engine, connection, metadata, config_db_details, request_id):
        tgtmetadata = db.Table('TargetMetaData', metadata, autoload=True, autoload_with=engine)
        db_list = self.get_target_db_metadata(config_db_details, request_id)
        connection.execute(tgtmetadata.insert(), db_list)
