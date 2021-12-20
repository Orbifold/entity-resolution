import networkx as nx


def draw(g):
    """
    Custom NetworkX drawing method rendering both node labels and edge labels.
    
    :param g: the graph to render
    """
    labelDic = {n: g.nodes[n]["label"] for n in g.nodes()}
    edgeDic = {e: g.get_edge_data(*e)["label"] for e in g.edges}
    kpos = nx.layout.fruchterman_reingold_layout(g)
    nx.draw(g, kpos, labels = labelDic, with_labels = True, arrowsize = 25)
    nx.draw_networkx_edge_labels(g, kpos, edge_labels = edgeDic, label_pos = 0.4)
