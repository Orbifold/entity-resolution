import networkx as nx
from tqdm import tqdm
import time

from datetime import timedelta


def infer_identity_edges(g):
    """
    Infers new edges between Identity and Device nodes.
    This does not involve any probability.

    :param g:
    :return: the inferred edges
    """
    print("Cloning")
    h = g.copy().to_undirected()

    def contract(label: str):
        print(f"Contracting edges defined by '{label}' nodes.")
        # remove the non-shared cookies
        single_i = [u for u in h.nodes() if h.nodes()[u]["label"]
                    == label and h.degree(u) == 1]
        h.remove_nodes_from(single_i)

        # contract remaining ones
        ids = [u for u in h.nodes() if h.nodes()[u]["label"] == label]
        for i in tqdm(ids):
            neigh = h.neighbors(i)
            for u in neigh:
                for v in neigh:
                    if u < v:
                        h.add_edge(u, v, label = "SIMILAR")
        h.remove_nodes_from(ids)

    contract("Cookie")
    contract("IP")
    contract("Location")

    # remove single device
    single_devices = [u for u in h.nodes() if h.nodes(
    )[u]["label"] == "Device" and h.degree(u) == 0]
    h.remove_nodes_from(single_devices)

    identities = [u for u in h.nodes() if h.nodes()[u]["label"] == "Identity"]

    print("Inferring identities")
    coll = []
    for i in tqdm(identities):
        # can only be devices or identity since all the rest has been removed
        devs = nx.shortest_path(h, i).keys()
        for j in devs:
            if i != j and not h.has_edge(i, j) and h.nodes()[j]["label"] != "Identity":
                h.add_edge(j, i, label = "INFERRED")
                coll.append((j, i))
    return coll


def infer_jaccard_identity(h, threshold = 0.1):
    """
    Identity inference with cumulative edge probability based on Jaccard similarity.

    :param threshold: all predicted edges with probability lower than the specified value will be excluded
    :param g: a graph
    :return: the collection of inferred identities in the shape (u, v, prob)
    """
    # print("Cloning")
    # h = g.copy().to_undirected()
    h = h.to_undirected()
    # add an edge with Jaccard similarity between every couple of device
    devices = [u for u in h.nodes() if h.nodes()[u]["label"] == "Device"]
    device_couples = [(u, v) for u in devices for v in devices if u < v]
    print("Computing similarities")
    jc = {(u, v): p for u, v, p in nx.algorithms.jaccard_coefficient(h, device_couples)}
    print("Creating similarity edges")
    for e in tqdm(jc):
        if jc[e] > 0:
            h.add_edge(*e, label = "SIMILAR", p = jc[e])
    print("Removing unnecessary nodes")
    to_remove = [u for u in h.nodes() if h.nodes()[u]["label"] in ["IP", "Location", "Cookie"]]
    h.remove_nodes_from(to_remove)

    # remove single device
    single_devices = [u for u in h.nodes() if h.nodes()[u]["label"] == "Device" and h.degree(u) == 0]
    h.remove_nodes_from(single_devices)

    identities = [u for u in h.nodes() if h.nodes()[u]["label"] == "Identity"]
    print(f"Inferring {len(identities)} identities")
    coll = []
    for i in tqdm(identities):
        shorts = nx.shortest_path(h, source = i)
        for j in shorts:
            if i != j and h.nodes(data = True)[j]["label"] == "Device":
                seq = shorts[j]
                coupling = 1
                for k in range(len(seq) - 1):
                    e = (seq[k], seq[k + 1])
                    coupling *= h.edges()[e]["p"] if "p" in h.edges()[e] else 1.0
                coupling = round(coupling, 2)
                if coupling < threshold:
                    continue
                if not h.has_edge(i, j):
                    h.add_edge(j, i, label = "INFERRED", p = coupling)
                    coll.append((j, i, {"p": coupling}))

    return coll


def infer_identities(m: nx.DiGraph, threshold = 0.1, return_ids = False, max_predictions = 100):
    """
    Infers identities on the given graph.

    :param m: the graph
    :param threshold: predictions with a probability below this number will be ignored
    :param return_ids: if False the node numbers will be returned
    :return: an array of (u, v, probability) triples
    """
    t = time.process_time()
    g = m.to_undirected()
    components = list(nx.connected_components(g))
    component_sizes = [len(c) for c in sorted(components, key = len, reverse = True)]
    coll = []
    print(f"Analyzing {len(components)} component(s). The largest contains {component_sizes[0]} nodes.")
    for component in tqdm(components):
        if len(coll) >= max_predictions:
            break
        h = g.subgraph(component).copy()
        identities = [u for u in h.nodes() if h.nodes()[u]["label"] == "Identity"]

        devices = [u for u in h.nodes() if h.nodes()[u]["label"] == "Device"]
        # print(f"Component: {len(h.nodes())} nodes, {len(devices)} devices, {len(identities)} identities.")

        device_couples = [(u, v) for u in devices for v in devices if u < v]
        jc = {(u, v): p for u, v, p in nx.algorithms.jaccard_coefficient(h, device_couples)}
        for e in jc:
            if jc[e] > 0:
                h.add_edge(*e, label = "SIMILAR", p = jc[e])
        to_remove = [u for u in h.nodes() if h.nodes()[u]["label"] in ["IP", "Location", "Cookie"]]
        h.remove_nodes_from(to_remove)

        # remove single device
        single_devices = [u for u in h.nodes() if h.nodes()[u]["label"] == "Device" and h.degree(u) == 0]
        h.remove_nodes_from(single_devices)

        for i in identities:
            shorts = nx.shortest_path(h, source = i)
            for j in shorts:
                if i != j and h.nodes(data = True)[j]["label"] == "Device":
                    seq = shorts[j]
                    coupling = 1
                    for k in range(len(seq) - 1):
                        e = (seq[k], seq[k + 1])
                        coupling *= h.edges()[e]["p"] if "p" in h.edges()[e] else 1.0
                    coupling = round(coupling, 2)
                    if coupling < threshold:
                        continue
                    if not h.has_edge(i, j):
                        h.add_edge(j, i, label = "INFERRED", p = coupling)
                        coll.append((j, i, coupling))
                        if len(coll) >= max_predictions:
                            break

    elapsed_time = time.process_time() - t
    print("Predictions", len(coll))
    print("Timing", timedelta(seconds = round(elapsed_time, 1)))
    if return_ids:
        # convert to id's
        ids = list(map(lambda t: (m.nodes()[t[0]]["id"], m.nodes()[t[1]]["id"], t[2]), coll))
    else:
        ids = None
    return coll, ids
