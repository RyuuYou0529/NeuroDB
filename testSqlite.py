from neurodb import sqliteDBIO

import tifffile as tiff
import napari

import numpy as np

def show(image_path, NODES, EDGES):
    img = tiff.imread(image_path)

    nodes = {data['nid']: np.asarray(data['coord']) for data in NODES}
    edges = [[nodes[data['src']], nodes[data['dst']]-nodes[data['src']]] for data in EDGES]

    viewer = napari.Viewer(ndisplay=3)
    img_layer = viewer.add_image(img, name='image')
    nodes_layer = viewer.add_points(np.asarray(list(nodes.values())), shading='spherical', size=2, name='nodes')
    edges_layer = viewer.add_vectors(np.asarray(edges), vector_style='line', edge_color='orange', name='edges')
    napari.run()

def test():
    db = sqliteDBIO("test/test_data/dendrites.db")
    # img_path = 'test/data/Z002/data/dendrites_1/image.tif'
    # NODES = db.read_nodes()
    # EDGES = db.read_edges()
    # show(img_path, NODES, EDGES)

if __name__ == "__main__":
    test()