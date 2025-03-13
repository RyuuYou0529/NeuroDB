from datetime import datetime
import networkx as nx
import numpy as np

from .datajointDBIO import datajointDBIO
from .sqliteDBIO import sqliteDBIO

class NeuroDB:
    def __init__(self, db_name:str, config:dict=None):
        if db_name.endswith('.db'):
            self.DB = sqliteDBIO(db_name)
        else:
            db_url = config['db_url']
            username = config['username']
            password = config['password']
            self.DB = datajointDBIO(db_name, db_url, username, password)
        
        self.G = nx.Graph()
        self.init_graph()
    
    def init_graph(self):
        NODES = self.read_nodes()
        EDGES = self.read_edges()
        for node in NODES:
            self.G.add_node(
                node['nid'], 
                coord=node['coord'], 
                creator=node['creator'], 
                type=node['type'], 
                checked=node['checked'], 
                status=node['status'], 
                date=node['date']
            )
        for edge in EDGES:
            self.G.add_edge(edge['src'], edge['dst'], creator=edge['creator'], date=edge['date'])

    def add_nodes(self, nodes:list[dict]):
        # nodes: [{'nid', 'coord', 'creator', 'type', 'checked', 'status', 'date'}]
        date = datetime.now()
        for n in nodes:
            if 'date' not in n:
                n['date'] = date
        self.DB.add_nodes(nodes)
        for node in nodes:
            self.G.add_node(
                node['nid'], 
                coord=node['coord'], 
                creator=node['creator'], 
                type=node['type'], 
                checked=node['checked'], 
                status=node['status'], 
                date=node['date']
            )
    
    def add_edges(self, edges:list[dict]):
        # edges: [{'src', 'dst', 'creator', 'date'}]
        date = datetime.now()
        for e in edges:
            if 'date' not in e:
                e['date'] = date
        self.DB.add_edges(edges)
        for edge in edges:
            self.G.add_edge(edge['src'], edge['dst'], creator=edge['creator'], date=edge['date'])
    
    def delete_nodes(self, nids:list):
        self.DB.delete_nodes(nids)
        self.G.remove_nodes_from(nids)
    
    def delete_edges(self, edges:list):
        self.DB.delete_edges(edges)
        for src, dst in edges:
            self.G.remove_edge(src, dst)
    
    def read_nodes(self):
        return self.DB.read_nodes()
    
    def read_edges(self, creator:str=None):
        return self.DB.read_edges(creator)

    def update_nodes(self, nids:list, creator:str=None, type:int=None, checked:int=None, status:int=None):
        def _update_nodes_in_graph(key:str, value:any=None, date:datetime=None):
            if value and (value != self.G.nodes[nid][key]):
                self.G.nodes[nid][key] = value
                self.G.nodes[nid]['date'] = date
        date = datetime.now()
        self.DB.update_nodes(nids, creator, type, checked, status, date)
        for nid in nids:
            _update_nodes_in_graph('creator', creator, date)
            _update_nodes_in_graph('type', type, date)
            _update_nodes_in_graph('checked', checked, date)
            _update_nodes_in_graph('status', status, date)
    
    def check_node(self, nid:int):
        date = datetime.now()
        self.DB.check_node(nid, date)
        self.G.nodes[nid]['checked'] = 1
        self.G.nodes[nid]['date'] = date
    
    def uncheck_node(self, nids:list[int]):
        date = datetime.now()
        self.DB.uncheck_node(nids, date)
        for nid in nids:
            self.G.nodes[nid]['checked'] = -1
            self.G.nodes[nid]['date'] = date

    def get_annotation_info(self, len_threshold:int=0):
        connected_components = list(nx.connected_components(self.G))
        valid_cc = []
        for cc in connected_components:
            if len(cc) < len_threshold:
                continue
            valid = True
            for nid in cc:
                if (self.G.degree(nid)==1 and self.G.nodes[nid]['checked']==0) or (self.G.nodes[nid]['checked']==-1):
                    valid = False
                    break
            if valid:
                valid_cc.append(cc)
        info = []
        for cc in valid_cc:
            sub_G:nx.Graph = self.G.subgraph(cc)
            length = 0
            for src, des in sub_G.edges:
                length += np.linalg.norm(np.array(sub_G.nodes[src]['coord']) - np.array(sub_G.nodes[des]['coord']))
            branch_nid = [nid for nid in sub_G.nodes if sub_G.degree(nid) > 2]
            end_nid = [nid for nid in sub_G.nodes if sub_G.degree(nid) == 1]
            info.append({
                'nid': list(cc),
                'branch_nid': branch_nid,
                'end_nid': end_nid,
                'length': int(length)
            })
        info.sort(key=lambda x:x['length'], reverse=True)
        return info


