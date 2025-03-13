import datajoint as dj
from datetime import datetime

class datajointDBIO:
    def __init__(self, db_name, db_url, username, password):
        # Configure DataJoint connection
        dj.config['database.host'] = db_url
        dj.config['database.user'] = username
        dj.config['database.password'] = password
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
            """
        self.SegsTable = Segs

        @self.schema
        class Nodes(dj.Manual):
            definition = """
            nid: int                      # Node ID (Primary Key)
            ---
            x: int                        # X-coordinate
            y: int                        # Y-coordinate
            z: int                        # Z-coordinate
            creator: varchar(50)          # Creator of the node
            type: int                     # Type/category of the node
            checked: int                  # Checked status
            status: int                   # Status of the node
            date: timestamp               # Creation timestamp
            """
        self.NodesTable = Nodes

        @self.schema
        class Edges(dj.Manual):
            definition = """
            -> Nodes.proj(src='nid')      # Source node ID (Foreign Key)
            -> Nodes.proj(dst='nid')      # Destination node ID (Foreign Key)
            ---
            creator: varchar(50)          # Creator of the edge
            date: timestamp               # Creation timestamp
            """
            def make(self, key):
                # Ensure src <= dst to prevent duplicate edge representations
                if key['src'] > key['dst']:
                    key['src'], key['dst'] = key['dst'], key['src']
                self.insert1(key)
        self.EdgesTable = Edges

    def add_nodes(self, nodes:list[dict]):
        # nodes: [{'nid', 'coord', 'creator', 'type', 'checked', 'status', 'date'}]
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
                'date': n.get('date', date)
            })
        
        # Insert all nodes at once
        if entries:
            self.NodesTable.insert(entries)
    
    def add_edges(self, edges:list[dict]):
        # edges: [{'src', 'dst', 'creator', 'date'}]
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
        if entries:
            self.EdgesTable.insert(entries)
    
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
    
    def delete_nodes(self, nids:list):
        filter = {'nid': {'in': nids}}
        try:
            (self.NodesTable & filter).delete()
        except dj.DataJointError as e:
            print(e)
            return False
        return True

    def delete_edges(self, edges:list):
        entries = []
        for src, dst in edges:
            if src > dst:
                src, dst = dst, src
            entries.append({'src': src, 'dst': dst})
        try:
            (self.EdgesTable & entries).delete()
        except dj.DataJointError as e:
            print(e)
            return False
        return True
    
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
        try:
            (self.NodesTable & filter).update(update)
        except dj.DataJointError as e: 
            print(e)
            return False
        return True

    def check_node(self, nid:int, date:datetime=None):
        return self.update_nodes([nid], checked=1, date=date)
    
    def uncheck_node(self, nids:list[int], date:datetime=None):
        return self.update_nodes(nids, checked=-1, date=date)

    def get_max_nid(self):
        try:
            max_nid = self.NodesTable.fetch('MAX(nid)')[0]
            return max_nid if max_nid is not None else 0
        except dj.DataJointError:
            return 0
    
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
        try:
            max_sid = self.SegsTable.fetch('MAX(sid)')[0]
            max_sid = max_sid if max_sid is not None else 0
        except dj.DataJointError:
            max_sid = 0
        seg_entries = []
        for seg in segs:
            max_sid += 1
            seg_entries.append({
                'sid': max_sid,
                'points': seg['points'],
                'sampled_points': seg['sampled_points']
            })
        if seg_entries:
            self.SegsTable.insert(seg_entries)

        max_nid = self.get_max_nid()
        node_entries = []
        edge_entries = []
        for seg in segs:
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
                })
                if i < len(coords)-1:
                    edge_entries.append({'src':max_nid, 'dst':max_nid+1, 'creator':'seger'})
        if node_entries:
            self.add_nodes(node_entries)
        if edge_entries:
            self.add_edges(edge_entries)

