import os
import sqlite3
from datetime import datetime

'''
    segs:
        {
            points: [head,...,tail],
            sampled_points: points[::interval],
        }
    nodes:
        {
            nid: int, PRIMARY KEY
            x: int
            y: int
            z: int
            creator: str,
            type: int, # 1 for Soma, 0 for normal node
            checked: int
            status: int, # 1 for show, 0 for hidden(removed)
            date: str, TIMESTAMP
        }
    edges:
        {
            src: int, 
            dst: int,
            creator: str,
            date: str, TIMESTAMP
            PRIMARY KEY: (src,dst)
            CHECK (src <= dst)
        }
'''

class sqliteDBIO:
    def __init__(self, db_path):
        self.db_path = db_path
        db_exists = os.path.exists(db_path)
        if not db_exists:
            self.init_db()
        else:
            upgrade_result = self.upgrade_database_schema(db_path)
            if not upgrade_result:
                print("Database schema upgrade failed. Please check the database schema manually.")
            else:
                print("Database schema is up-to-date.")
            self.inspect_database()

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS segs(
                sid INTEGER PRIMARY KEY,
                points TEXT,
                sampled_points TEXT
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS nodes(
                nid INTEGER PRIMARY KEY,
                coord TEXT,
                x INTEGER,
                y INTEGER,
                z INTEGER,
                type INTEGER,
                checked INTEGER,
                status INTEGER,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            )
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS edges(
                src INTEGER,
                dst INTEGER,
                creator TEXT,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (src,dst),
                CHECK (src <= dst)
            )
            '''
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nodes_nid ON nodes (nid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_edges_src ON edges (src)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_edges_dst ON edges (dst)")

        conn.commit()
        conn.close()

    def add_nodes(self, nodes:list[dict]):
        # given a list of nodes, write them to node table
        # nodes: [{'nid', 'coord', 'creator', 'type', 'checked', 'status', 'date'}]
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        date = datetime.now()
        entries = []
        for n in nodes:
            x,y,z = n['coord']
            entries.append((
                n['nid'],
                x,y,z,
                n['creator'],
                n['type'],
                n['checked'],
                n['status'],
                n.get('date', date)            
            ))
        cursor.executemany(
            "INSERT INTO nodes (nid, x, y, z, creator, type, checked, status, date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            entries
        )
        conn.commit()
        conn.close()

    def add_edges(self, edges:list[dict]):
        # given list of edges, write them to edges table
        # edges: [{'src', 'dst', 'creator', 'date'}]
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        date = datetime.now()
        entries = []
        for e in edges:
            src, dst = e['src'], e['dst']
            if src > dst:
                src, dst = dst, src
            entries.append((src, dst, e['creator'], e.get('date', date)))
        cursor.executemany(
            "INSERT OR IGNORE INTO edges (src, dst, creator, date) VALUES (?, ?, ?, ?)",
            entries
        )
        conn.commit()
        conn.close()
    
    def read_nodes(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM nodes ORDER BY nid")
        rows = cursor.fetchall()
        nodes = []
        for row in rows:
            data = {
                'nid': row['nid'],
                'coord': [row['x'], row['y'], row['z']],
                'creator': row['creator'],
                'type': row['type'],
                'checked': row['checked'],
                'status': row['status'],
                'date': row['date'],
            }
            nodes.append(data)
        conn.close()
        return nodes
    
    def read_edges(self, creator:str=None):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query = "SELECT * FROM edges"
        if creator:
            query += " WHERE creator=?"
            cursor.execute(query, (creator,))
        else:
            cursor.execute(query)
        rows = cursor.fetchall()
        edges = []
        for row in rows:
            data = {
                'src': row['src'],
                'dst': row['dst'],
                'creator': row['creator'],
                'date': row['date'],
            }
            edges.append(data)
        conn.close()
        return edges
    
    def delete_nodes(self, nids):
        # given a list of nid, delete nodes from nodes table and edges from edges table
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM nodes WHERE nid IN ({','.join(map(str, nids))})")
        # Remove edges where either source or destination node is in the given list
        cursor.execute(f"DELETE FROM edges WHERE src IN ({','.join(map(str, nids))}) OR dst IN ({','.join(map(str, nids))})")
        conn.commit()
        conn.close()

    def delete_edges(self, edges):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for src, dst in edges:
            if src > dst:
                src, dst = dst, src
            cursor.execute("DELETE FROM edges WHERE src=? AND dst=?", (src, dst))
        conn.commit()
        conn.close()
    
    def update_nodes(self, nids:list, creator:str=None, type:int=None, checked:int=None, status:int=None, date:datetime=None):
        if all(param is None for param in [creator, type, checked, status]) or not nids:
            return
        if not isinstance(nids, list):
            nids = [nids]
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        update_parts = []
        where_conditions = []
        params = []
        if creator is not None:
            update_parts.append("creator = ?")
            where_conditions.append("(creator IS NULL OR creator != ?)")
            params.extend([creator, creator])
        if type is not None:
            update_parts.append("type = ?")
            where_conditions.append("(type IS NULL OR type != ?)")
            params.extend([type, type])
        if checked is not None:
            update_parts.append("checked = ?")
            where_conditions.append("(checked IS NULL OR checked != ?)")
            params.extend([checked, checked])
        if status is not None:
            update_parts.append("status = ?")
            where_conditions.append("(status IS NULL OR status != ?)")
            params.extend([status, status])
        if date is None:
            date = datetime.now()
        update_parts.append("date = ?")
        params.append(date)
        
        placeholders = ','.join('?' for _ in nids)
        if where_conditions:
            additional_conditions = f" AND ({' OR '.join(where_conditions)})"
            query = f"UPDATE nodes SET {', '.join(update_parts)} WHERE nid IN ({placeholders}){additional_conditions}"
        else:
            query = f"UPDATE nodes SET {', '.join(update_parts)} WHERE nid IN ({placeholders})"
        params.extend(nids)
        
        cursor.execute(query, params)
        conn.commit()
        conn.close()
    
    def check_node(self, nid:int, date:datetime=None):
        self.update_nodes([nid], checked=1, date=date)
    
    def uncheck_nodes(self, nids:list[int], date:datetime=None):
        self.update_nodes(nids, checked=-1, date=date)

    def get_max_nid(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # Retrieve the highest existing nid value
        cursor.execute("SELECT MAX(nid) FROM nodes")
        max_nid = cursor.fetchone()[0] or 0  # If there are no existing items, set max_nid to 0
        conn.commit()
        conn.close()
        return max_nid
    
    def read_nid_within_roi(self, roi):
        offset, size = roi[:3], roi[-3:]
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query = f"SELECT * FROM nodes "
        where = f"WHERE x >= {offset[0]} AND x <= {offset[0]+size[0]} AND " +  \
                f"y >= {offset[1]} AND y <= {offset[1]+size[1]} AND " + \
                f"z >= {offset[2]} AND z <= {offset[2]+size[2]}"
        cursor.execute(query+where)
        rows = cursor.fetchall()
        nids = []
        for row in rows:
            nids.append(row['nid'])
        conn.close()
        return nids
    
    def segs2db(self, segs):
        # insert segs into database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT MAX(sid) FROM segs")
        max_sid = cursor.fetchone()[0] or 0
        for seg in segs:
            max_sid+=1
            cursor.execute(f"INSERT INTO segs (sid, points, sampled_points) VALUES (?, ?, ?)",
                        (max_sid, sqlite3.Binary(str(seg['points']).encode()), sqlite3.Binary(str(seg['sampled_points']).encode())))
        print(f'Number of segs in database: {max_sid}; {len(segs)} newly added.')

        # insert nodes into database
        max_nid = self.get_max_nid()
        # assign unique nid for each node in segs according to index
        nodes = []
        edges = []
        for seg in segs:
            coords = seg['sampled_points']
            for i, coord in enumerate(coords):
                max_nid += 1
                nodes.append({
                    'nid': max_nid,
                    'coord': coord,
                    'creator': 'seger',
                    'type': 0,
                    'checked': 0,
                    'status': 1,
                })
                if i < len(coords)-1:
                    edges.append({'src':max_nid, 'dst':max_nid+1, 'creator':'seger'})

        print(f'Adding {len(nodes)} nodes to database')
        self.add_nodes(nodes)
        print(f'Adding {len(edges)} edges to database')
        self.add_edges(edges)
    
    @staticmethod
    def upgrade_database_schema(db_path):
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA table_info(nodes)")
        node_columns = {col['name']: col for col in cursor.fetchall()}
        try:
            if 'coord' in node_columns and ('x' not in node_columns or 'y' not in node_columns or 'z' not in node_columns):
                print("Upgrading nodes table schema")
                cursor.execute(
                    '''
                    CREATE TABLE nodes_new (
                        nid INTEGER PRIMARY KEY,
                        x INTEGER,
                        y INTEGER,
                        z INTEGER,
                        creator TEXT,
                        type INTEGER,
                        checked INTEGER,
                        status INTEGER,
                        date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    ''')
                cursor.execute("SELECT * FROM nodes")
                NODES = cursor.fetchall()
                cursor.execute("BEGIN TRANSACTION")
                
                for node in NODES:
                    coord = eval(node['coord'])
                    x,y,z = coord
                    cursor.execute(
                        "INSERT INTO nodes_new (nid, x, y, z, creator, type, checked, status, date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            node['nid'], 
                            x, y, z,
                            node['creator'],
                            node['type'],
                            node['checked'],
                            node['status'],
                            node['date']
                        )
                    )
                cursor.execute("DROP TABLE nodes")
                cursor.execute("ALTER TABLE nodes_new RENAME TO nodes")
                conn.commit()
                print('Down')

        except Exception as e:
            conn.rollback()
            conn.close()
            return False
        
        try:
            cursor.execute("PRAGMA table_info(edges)")
            edge_columns = {col['name']: col for col in cursor.fetchall()}
            if "des" in edge_columns and "dst" not in edge_columns:
                print("Upgrading edges table schema")
                cursor.execute(
                    '''
                    CREATE TABLE edges_new (
                        src INTEGER,
                        dst INTEGER,
                        creator TEXT,
                        date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (src,dst),
                        CHECK (src <= dst)
                    )
                    '''
                )
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute("SELECT * FROM edges")
                EDGES = cursor.fetchall()
                for edge in EDGES:
                    src = edge['src']
                    dst = edge['des']
                    if src > dst:
                        src, dst = dst, src
                    cursor.execute(
                        "INSERT OR IGNORE INTO edges_new (src, dst, creator, date) VALUES (?, ?, ?, ?)",
                        (src, dst, edge['creator'], edge['date'])
                    )
                cursor.execute("DROP TABLE edges")
                cursor.execute("ALTER TABLE edges_new RENAME TO edges")
                conn.commit()
                print('Down')
        except Exception as e:
            conn.rollback()
            conn.close()
            return False
        
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_nodes_nid ON nodes (nid)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_edges_src ON edges (src)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_edges_dst ON edges (dst)")
            conn.commit()
        except Exception as e:
            conn.rollback()
            conn.close()
            return False
        
        conn.close()
        return True

    def inspect_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        print("\n==== DATABASE INSPECTION ====")
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"Total tables: {len(tables)}")
        print("Tables:", ", ".join([table[0] for table in tables]))
        
        # Get list of indexes
        cursor.execute("SELECT name, tbl_name FROM sqlite_master WHERE type='index';")
        indexes = cursor.fetchall()
        print(f"\nTotal indexes: {len(indexes)}")
        for idx_name, tbl_name in indexes:
            print(f"  - {idx_name} (on table {tbl_name})")
        
        # Table details
        print("\n== Table Details ==")
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            print(f"Table '{table_name}' ({len(columns)} columns):")
            for column in columns:
                col_id, col_name, col_type, not_null, default, pk = column
                pk_str = " PRIMARY KEY" if pk else ""
                null_str = " NOT NULL" if not_null else ""
                default_str = f" DEFAULT {default}" if default else ""
                print(f"  - {col_name} ({col_type}{pk_str}{null_str}{default_str})")
            
            # Count rows
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            row_count = cursor.fetchone()[0]
            print(f"  Total records: {row_count}")
        
        print("==== ==== ==== ==== ==== ====\n")
        conn.close()
    