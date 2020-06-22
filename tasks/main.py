from dotenv import load_dotenv
from os import environ
from datetime import datetime
import sys
import json

load_dotenv()
src_path = environ.get('src-path')
sys.path.append(src_path)
from src.tasks.actions import Actions
from src.tasks.match import Match
from src.tasks.tdata import TD_metadata

MSSQLDRIVER = environ.get('sqlserverdriver')
USERNAME = environ.get('sqlserveruser')
PASSWORD = environ.get('sqlserverpass')
GETDATE = datetime.now()
delin_server = environ.get('delineator-server')
delin_port = environ.get('delineator-port')
delin_type = environ.get('delineator-type')
delin_name = environ.get('delineator-name')


def read_json():
    with open(src_path + '/py_call.json') as f:
        file_dict = json.load(f)
    return file_dict


class Callable:
    def __init__(self, request_id):
        self.act_obj = Actions()
        self.td_obj = TD_metadata()
       # self.match_obj=Match()
        self.request_id = request_id

    def insert_metadata(self):
        params = {'username': USERNAME, 'password': PASSWORD, 'host': delin_server, 'port': delin_port,
                  'database': delin_name, 'driver': MSSQLDRIVER}
        engine, connection, metadata = self.act_obj.get_db(params, delin_type)
        config_db_details = self.act_obj.get_db_details(engine, connection, metadata, self.request_id)

        if config_db_details[0][3].lower().replace(" ", "") == 'mssql':
            self.act_obj.insert_into_sourcemetadata(engine, connection, metadata, config_db_details, self.request_id)
        elif config_db_details[0][3].lower().replace(" ", "") == 'teradata':
            self.td_obj.insert_into_sourcemetadata(engine, connection, metadata, config_db_details, self.request_id)

        if config_db_details[0][10].lower().replace(" ", "") == 'mssql':
            self.act_obj.insert_into_targetmetadata(engine, connection, metadata, config_db_details, self.request_id)
        elif config_db_details[0][10].lower().replace(" ", "") == 'teradata':
            self.td_obj.insert_into_targetmetadata(engine, connection, metadata, config_db_details, self.request_id)

    def insert_mapping(self):
        params = {'username': USERNAME, 'password': PASSWORD, 'host': delin_server, 'port': delin_port,
                  'database': delin_name, 'driver': MSSQLDRIVER}
        engine, connection, metadata = self.act_obj.get_db(params, delin_type)

        self.act_obj.insert_into_srctgtmapping(engine, connection, metadata, self.request_id)

    def compare_hist(self):
        params = {'username': USERNAME, 'password': PASSWORD, 'host': delin_server, 'port': delin_port,
                  'database': delin_name, 'driver': MSSQLDRIVER}
        engine, connection, metadata = self.act_obj.get_db(params, delin_type)

        self.act_obj.compare_hist_data(engine, connection, metadata, self.request_id)

    def update_id(self):
        params = {'username': USERNAME, 'password': PASSWORD, 'host': delin_server, 'port': delin_port,
                  'database': delin_name, 'driver': MSSQLDRIVER}
        engine, connection, metadata = self.act_obj.get_db(params, delin_type)

        self.act_obj.update_config_id(engine, connection, metadata, self.request_id)

    def match(self):
        Match(request_id).main()


if __name__ == '__main__':
    py_call = read_json()
    #print(py_call)
    request_id = py_call['request_id']
    function_name = py_call['function_name']
    Callable(request_id).match()

    '''if function_name == 'insert_metadata':
        Callable(request_id).insert_metadata()
    elif function_name == 'insert_mapping':
        Callable(request_id).insert_mapping()
        Callable(request_id).compare_hist()
        Callable(request_id).match()
        Callable(request_id).update_id()'''
