import datajoint as dj
from datetime import datetime

class datajointDBIO:
    def __init__(self, db_name:str, db_url:str, username:str, password:str):
        # Configure DataJoint connection
        dj.config['database.host'] = db_url
        dj.config['database.user'] = username
        dj.config['database.password'] = password
    
        if db_name is not None:
            self.switch_to(db_name)

    def switch_to(self, db_name):
        self.schema = dj.Schema(db_name)
        self.init_db()

    def init_db(self):
        @self.schema
        class Segs(dj.Manual):
            definition = """
            sid: int                      # Segment ID (Primary Key)
            ---
            points: longblob              # List of points in the segment
            sampled_points: longblob      # List of sampled points in the segment
            version: int                  # Version of the segment
            date: timestamp               # Creation timestamp
            """
        self.SegsTable = Segs

        @self.schema
        class Nodes(dj.Manual):
            definition = """
            nid: int                            # Node ID (Primary Key)
            ---
            x: int                              # X-coordinate
            y: int                              # Y-coordinate
            z: int                              # Z-coordinate
            creator: varchar(100)               # Creator of the node
            type: int                           # Type/category of the node
            checked: int                        # Checked status
            status: int                         # Status of the node
            -> [nullable] Segs.proj(sid='sid')  # Segment ID (Foreign Key)
            date: timestamp                     # Creation timestamp
            """
        self.NodesTable = Nodes

        @self.schema
        class Edges(dj.Manual):
            definition = """
            -> Nodes.proj(src='nid')        # Source node ID (Foreign Key)
            -> Nodes.proj(dst='nid')        # Destination node ID (Foreign Key)
            ---
            creator: varchar(100)           # Creator of the edge
            date: timestamp                 # Creation timestamp
            """
            def make(self, key):
                # Ensure src <= dst to prevent duplicate edge representations
                if key['src'] > key['dst']:
                    key['src'], key['dst'] = key['dst'], key['src']
                self.insert1(key)
        self.EdgesTable = Edges

    def add_segs(self, seg:list[dict]):
        # seg: [{'sid', 'points', 'sampled_points', 'version', 'date'}]
        with dj.conn().transaction:
            try:
                if seg:
                    self.SegsTable.insert(seg)
            except Exception as e:
                print(f'Error in add_seg: {e}')
                raise e

    def add_nodes(self, nodes:list[dict]):
        # nodes: [{'nid', 'coord', 'creator', 'type', 'checked', 'status', 'sid', 'date'}]
        with dj.conn().transaction:
            entries = []
            date = datetime.now()
            for n in nodes:
                x, y, z = n['coord']
                entries.append({
                    'nid': n['nid'],
                    'x': x,
                    'y': y,
                    'z': z,
                    'creator': n['creator'],
                    'type': n['type'],
                    'checked': n['checked'],
                    'status': n['status'],
                    'date': n.get('date', date),
                    'sid': n.get('sid', None)
                })
            # Insert all nodes at once
            try:
                if entries:
                    self.NodesTable.insert(entries)
            except Exception as e:
                print(f'Error in add_nodes: {e}')
                raise e
    
    def add_edges(self, edges:list[dict]):
        # edges: [{'src', 'dst', 'creator', 'date'}]
        with dj.conn().transaction:
            entries = []
            date = datetime.now()
            for e in edges:
                src, dst = e['src'], e['dst']
                if src > dst:
                    src, dst = dst, src
                entries.append({
                    'src': src,
                    'dst': dst,
                    'creator': e['creator'],
                    'date': e.get('date', date)
                })
            try:
                if entries:
                    self.EdgesTable.insert(entries)
            except Exception as e:
                print(f'Error in add_edges: {e}')
                raise e
    
    def read_segs(self):
        return self.SegsTable.fetch(as_dict=True)

    def read_nodes(self):
        nodes_entries = self.NodesTable.fetch(as_dict=True)
        nodes = []
        for entry in nodes_entries:
            coord = [entry['x'], entry['y'], entry['z']]
            nodes.append({
                'nid': entry['nid'],
                'coord': coord,
                'creator': entry['creator'],
                'type': entry['type'],
                'checked': entry['checked'],
                'status': entry['status'],
                'date': entry['date']
            })
        return nodes

    def read_edges(self, creator:str=None):
        if creator is not None:
            filter = {'creator': creator}
            edges = self.EdgesTable & filter
        else:
            edges = self.EdgesTable
        return edges.fetch(as_dict=True)
    
    def delete_nodes(self, nids:list, safemode=False):
        with dj.conn().transaction:
            filter = {'nid': {'in': nids}}
            try:
                (self.NodesTable & filter).delete(safemode=safemode)
            except Exception as e:
                print(e)
                raise e

    def delete_edges(self, edges:list, safemode=False):
        entries = []
        for src, dst in edges:
            if src > dst:
                src, dst = dst, src
            entries.append({'src': src, 'dst': dst})
        with dj.conn().transaction:
            try:
                (self.EdgesTable & entries).delete(safemode=safemode)
            except Exception as e:
                print(e)
                raise e
    
    def update_nodes(self, nids:list, creator:str=None, type:int=None, checked:int=None, status:int=None, date:datetime=None):
        filter = {'nid': {'in': nids}}
        update = {}
        if creator is not None:
            update['creator'] = creator
            filter['creator'] = {'!=': creator}
        if type is not None:
            update['type'] = type
            filter['type'] = {'!=': type}
        if checked is not None:
            update['checked'] = checked
            filter['checked'] = {'!=': checked}
        if status is not None:
            update['status'] = status
            filter['status'] = {'!=': status}
        if date is None:
            date = datetime.now()
        update['date'] = date

        with dj.conn().transaction:
            try:
                (self.NodesTable & filter).update(update)
            except Exception as e: 
                print(e)
                raise e

    def check_node(self, nid:int, date:datetime=None):
        self.update_nodes([nid], checked=1, date=date)
    
    def uncheck_nodes(self, nids:list[int], date:datetime=None):
        self.update_nodes(nids, checked=-1, date=date)

    def get_max_nid(self):
        max_nid = self.NodesTable.fetch('nid', order_by='nid DESC', limit=1)
        max_nid = max_nid[0] if len(max_nid)>0 else 0
        return max_nid
    
    def get_max_sid_version(self):
        max_sid = self.SegsTable.fetch('sid', order_by='sid DESC', limit=1)
        max_sid = max_sid[0] if len(max_sid) > 0 else 0
        seg_version = self.SegsTable.fetch('version', order_by='version DESC', limit=1)
        seg_version = seg_version[0] if len(seg_version) > 0 else 0
        return max_sid, seg_version
    
    def read_nid_within_roi(self, roi):
        offset, size = roi[:3], roi[-3:]
        # between: [start, end]
        filter = {
            'x': {'between': (offset[0], offset[0]+size[0]-1)},
            'y': {'between': (offset[1], offset[1]+size[1]-1)},
            'z': {'between': (offset[2], offset[2]+size[2]-1)}
        }
        nids = (self.NodesTable & filter).fetch('nid')
        return nids
    
    def segs2db(self, segs):
        with dj.conn().transaction:
            date = datetime.now()
            try:
                max_sid, seg_version = self.get_max_sid_version()
                seg_version += 1
                seg_entries = []
                for seg in segs:
                    max_sid += 1
                    seg_entries.append({
                        'sid': max_sid,
                        'points': seg['points'],
                        'sampled_points': seg['sampled_points'],
                        'version': seg_version,
                        'date': date
                    })
                if seg_entries:
                    self.add_segs(seg_entries)

                max_nid = self.get_max_nid()
                node_entries = []
                edge_entries = []
                for seg in seg_entries:
                    coords = seg['sampled_points']
                    for i, coord in enumerate(coords):
                        max_nid += 1
                        node_entries.append({
                            'nid': max_nid,
                            'coord': coord,
                            'creator': 'seger',
                            'type': 0,
                            'checked': 0,
                            'status': 1,
                            'sid': seg['sid'],
                            'date': date
                        })
                        if i < len(coords)-1:
                            edge_entries.append({'src':max_nid, 'dst':max_nid+1, 'creator':'seger', 'date':date})
                if node_entries:
                    self.add_nodes(node_entries)
                if edge_entries:
                    self.add_edges(edge_entries)
            except Exception as e:
                print(f'Error in segs2db: {e}')
                raise e


