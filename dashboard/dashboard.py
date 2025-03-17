import streamlit as st
import pandas as pd
import numpy as np

neurodb = st.session_state.GLOBAL.neurodb

len_threshold = 30
annotationInfo = neurodb.get_annotation_info(len_threshold=len_threshold)
length_info = [data['length'] for data in annotationInfo]
df_info = pd.DataFrame({
    "index": np.arange(len(annotationInfo)),
    "length": length_info,
    "branch": [len(data['branch_nid']) for data in annotationInfo],
    "end": [len(data['end_nid']) for data in annotationInfo],
})
st.subheader("Annotation Info")
st.markdown(f'''
    There are :red[{len(length_info)}] segments with more than :red[{len_threshold}] nodes.\n
    The :blue[longest] segment has length of :red[{max(length_info)/1000:.3f}]mm.\n
    The :blue[shortest] segment has length of :red[{min(length_info)/1000:.3f}]mm.\n
    The :blue[mean] length of the segments is :red[{np.mean(length_info)/1000:.3f}]mm.\n
    The :blue[median] length of the segments is :red[{np.median(length_info)/1000:.3f}]mm.\n
    The :blue[standard deviation] of length of the segments is :red[{np.std(length_info)/1000:.3f}]mm.
''')
st.bar_chart(
    df_info,
    x_label='Length [um]', x="length",
    y_label='Node Num', y=["branch", "end"],
    color=["#b5354a", "#357bb5"],
)

nodesTab, edgesTab, segsTab = st.tabs(["Nodes", "Edges", "Segs"]) 

nodesTab.subheader("Nodes Table")
NODES = neurodb.DB.read_nodes()
df_nodes = pd.DataFrame(NODES)
nodesTab.dataframe(df_nodes)

edgesTab.subheader("Edges Table")
EDGES = neurodb.DB.read_edges()
df_edges = pd.DataFrame(EDGES)
edgesTab.dataframe(df_edges)

segsTab.subheader("Segs Table")
SEGS = neurodb.DB.read_segs()
df_segs = pd.DataFrame(SEGS)
segsTab.dataframe(df_segs)