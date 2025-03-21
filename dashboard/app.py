import streamlit as st
import datajoint as dj

from neurodb import NeuroDB

class globalState:
    def __init__(self):
        self.authenticated = False
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
    
    def connected(self, username, password):
        self.db_config['username'] = username
        self.db_config['password'] = password
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
        db_name_list = [db[0] for db in db_name_list]
        self.db_name_list = db_name_list
        return self.db_name_list

    def switch_db(self, db_name):
        if self.authenticated:
            self.db_name_selected = db_name
            self.neurodb.switch_to(self.db_name_selected)
            return self.neurodb
        else:
            return None

# Login Page
def login():
    st.title("Login Page")
    
    username = st.text_input("Username", value='root')
    password = st.text_input("Password", type="password", value='neurodb')
    
    if st.button("Login"):
        if st.session_state.GLOBAL.connected(username, password):
            st.success("Login successful!")
            st.rerun()
        else:
            st.error("Invalid username or password")

def logout():
    st.session_state.GLOBAL.refresh()
    st.rerun()

st.session_state.GLOBAL: globalState # type: ignore
if 'GLOBAL' not in st.session_state:
    st.session_state.GLOBAL = globalState()

home_page = st.Page("home.py", title="Homepage")
dashboard_page = st.Page("dashboard.py", title="Dashboard")
logout_page = st.Page(logout, title="Log out", icon=":material/logout:")

if st.session_state.GLOBAL.authenticated:
    pg = st.navigation({'Home': [home_page, dashboard_page], 'Logout': [logout_page]})
    st.session_state.GLOBAL.update_db_name_list()
else:
    pg = st.navigation([st.Page(login)])

pg.run()