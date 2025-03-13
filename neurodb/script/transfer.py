from ..src import sqliteDBIO
from ..src import datajointDBIO

def sqlite2dj(db_path_sqlite:str, db_config_dj:dict):
    try:
        sqlite = sqliteDBIO(db_path_sqlite)

        db_name_dj = db_config_dj['db_name']
        db_url_dj = db_config_dj['db_url']
        username_dj = db_config_dj['username']
        password_dj = db_config_dj['password']
        dj = datajointDBIO(db_name_dj, db_url_dj, username_dj, password_dj)

        NODES = sqlite.read_nodes()
        EDGES = sqlite.read_edges()

        dj.add_nodes(NODES)
        dj.add_edges(EDGES)

        return True
    except Exception as e:
        print(e)
        return False

def dj2sqlite(db_path_sqlite:str, db_config_dj:dict):
    try:
        sqlite = sqliteDBIO(db_path_sqlite)

        db_name_dj = db_config_dj['db_name']
        db_url_dj = db_config_dj['db_url']
        username_dj = db_config_dj['username']
        password_dj = db_config_dj['password']
        dj = datajointDBIO(db_name_dj, db_url_dj, username_dj, password_dj)

        NODES = dj.read_nodes()
        EDGES = dj.read_edges()

        sqlite.add_nodes(NODES)
        sqlite.add_edges(EDGES)
        
        return True
    except Exception as e:
        print(e)
        return False
        