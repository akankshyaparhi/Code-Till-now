import sqlalchemy as db
import pandas as pd
# from os import environ
# import sys
# from dotenv import load_dotenv

# load_dotenv()
# src_path = environ.get('src-path')
# sys.path.append(src_path)
# from src.tasks.actions import Actions


class MSSQL:
    def __init__(self):
        pass

    def get_unique_keys(self, connection, tablename):
        statement = "SELECT TC.TABLE_NAME, CCU.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC INNER JOIN " \
                    "INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU ON TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME WHERE " \
                    "TC.TABLE_NAME = '{table}' AND TC.CONSTRAINT_TYPE IN ('UNIQUE', 'PRIMARY KEY')".format(
            table=tablename)

        statement_obj = db.text(statement)
        result_list = connection.execute(statement_obj).fetchall()
        result_df = pd.DataFrame(result_list, columns=['Table_Name', 'Column_Name'])
        return result_df


# class MSAct(Actions):
#     def __init__(self):
#         Actions.__init__(self)
#
#     def insert_source_metadata(self, engine, connection, metadata, config_db_details, request_id):
#         db_list = Actions().get_source_db_metadata(config_db_details, request_id)
#
#         for i in db_list:
#             print(i['Source_TableName'])
#             unique_keys = MSSQL().get_unique_keys(connection, i['Source_TableName'])
#             for item, value in unique_keys['Column_Name'].items():
#                 print('inside for')
#                 if i['Source_ColumnName'] == unique_keys['Source_ColumnName'][item]:
#                     i['Is_UniqueKey'] = 'Y'
#                 else:
#                     i['Is_UniqueKey'] = 'N'
#
#             # if not unique_keys.empty and i['Source_ColumnName'] in unique_keys['Column_Name']:
#             #     i['Is_UniqueKey'] = 'Y'
#             # else:
#             #     i['Is_UniqueKey'] = 'N'
#
#         srcmetadata = db.Table('SourceMetaData', metadata, autoload=True, autoload_with=engine)
#         connection.execute(srcmetadata.insert(), db_list)
