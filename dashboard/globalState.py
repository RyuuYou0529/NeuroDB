import datajoint as dj
import streamlit as st

from neurodb import NeuroDB

class globalState:
    def __init__(self):
        self.refresh()
        
    def refresh(self):
        if self.authenticated and self.conn is not None:
            self.conn.close()
        self.authenticated = False
        self.db_config = {
            'url': 'localhost:3306',
            'username': '',
            'password': '',
        }
        self.conn = None
        self.db_name_selected = ''
        self.db_name_list = []

        self.neurodb = None
    
    def connected(self):
        self.conn = dj.conn(
            host=self.db_config['url'],
            user=self.db_config['username'], 
            password=self.db_config['password'], 
            reset=True
        )
        self.authenticated = self.conn.is_connected
        if self.authenticated:
            self.neurodb = NeuroDB(db_name=None, config=self.db_config)
        return self.authenticated
    
    def update_db_name_list(self):
        query = """
            SELECT SCHEMA_NAME FROM information_schema.SCHEMATA 
            WHERE SCHEMA_NAME NOT IN (
                'information_schema', 'mysql', 'performance_schema', 'sys'
            )
            ORDER BY SCHEMA_NAME
        """
        db_name_list = self.conn.query(query).fetchall()
        db_name_list = [None] + [db[0] for db in db_name_list]
        self.db_name_list = db_name_list
        return self.db_name_list

    def switch_db(self, db_name):
        if self.authenticated:
            self.db_name_selected = db_name
            self.neurodb.switch_to(self.db_name_selected)
            return self.neurodb
        else:
            return None