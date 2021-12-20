import json

import neo4j

from .data import create_synthetic_graph
from .inference import *
from .data import *
import os
import networkx as nx
import pandas as pd
from pathlib import Path


def demo(device_count = 150, identity_count = 50, cookie_probability = 0.3, ip_probability = 0.3, location_probability = 0.3, threshold = 0.1):
    """
    This method performs the following:
    - it creates a synthetic dataset/graph
    - infers identities on it
    - saves the predictions to csv
    - save the graph with predictions to a GraphML file
    - opens the default app to present the graph with inferred identities.

    :param device_count: how many devices to generate
    :param identity_count: how many identities to generate (must be less than the device count)
    :param cookie_probability: the probability that a cookie is shared
    :param ip_probability: the probability that an IP node is shared
    :param location_probability: the probability that a Location node is shared
    :param threshold: predictions with a probability below this number will be ignored
    :return: the graph and the predictions dataframe
    """
    g = create_synthetic_graph(device_count, identity_count, cookie_probability, ip_probability, location_probability)
    inf, ids = infer_identities(g, threshold, True)
    g.add_edges_from([(u, v, {"p": p}) for u, v, p in inf], label = "INFERRED")
    graphml_path = os.path.join(os.getcwd(), "synthetic.graphml")
    print("GraphML diagram at ", graphml_path)
    nx.write_graphml(g, graphml_path)
    os.system(f"open {graphml_path}")

    df = inference_to_frame(ids)
    csv_path = os.path.join(os.getcwd(), "predictions.csv")
    df.to_csv(csv_path)
    print("Predictions saved to ", csv_path)
    node_count = len(g.nodes())
    edge_count = len(g.edges())
    identity_count = len([u for u in g.nodes() if g.nodes()[u]["label"] == "Identity"])
    inferred_count = len([e for e in g.edges() if g.edges()[e]["label"] == "INFERRED"])
    print("Nodes:", node_count, "Edges:", edge_count, "Identities:", identity_count, "Inferred:", inferred_count)
    return g, df


def inference_to_frame(inf):
    """
    Creates a data frame from the given inference set.

    :param inf: an array of triple (u, v, probaility)
    :return: a Pandas frame
    """
    rows = list(map(lambda r: (r[0], r[1], r[2]), inf))
    df = pd.DataFrame(rows, columns = ["Device", "Identity", "Probability"])
    df = df.sort_values(by = ["Probability"], ascending = False)
    return df


def raw_to_predictions(raw_directory, threshold = 0.1, save_diagram = False, predictions_path = None, max_predictions = 100):
    """
    Returns the inferred identities directly from the given directory of gzip TSV files.

    :param raw_directory: the directory contains the gzip files.
    :param threshold: predictions with a probability below this number will be ignored
    :param save_diagram: whether to save the generated diagram to GraphML for visualization purposes
    :param predictions_path: the path where the predictions as a CSV file will be save. If not specified it will be saved in the current directory as 'predictions.csv'.
    :param max_predictions: the maximum amount of predictions to generate (top N).
    :return: (graph, predictions frame)
    """

    g = raw_to_graph(raw_directory)
    inf, ids = infer_identities(g, threshold, True)
    g.add_edges_from([(u, v, {"p": p}) for u, v, p in inf], label = "INFERRED")
    if save_diagram:
        graphml_path = os.path.join(os.getcwd(), "synthetic.graphml")
        print("GraphML diagram at ", graphml_path)
        nx.write_graphml(g, graphml_path)

    df = inference_to_frame(ids).head(max_predictions)
    if predictions_path is None:
        predictions_path = os.path.join(os.getcwd(), "predictions.csv")
    df.to_csv(predictions_path)
    print("Predictions saved to ", predictions_path)
    return g, df


def save_open_graph(g):
    """
    Saves the given graph as GraphML and opens the default application to render the GraphML.

    :param g: a graph
    """
    graphml_path = os.path.join(os.getcwd(), "graph.graphml")
    print("GraphML diagram at ", graphml_path)
    nx.write_graphml(g, graphml_path)
    os.system(f"open {graphml_path}")


def neo_to_predictions(db_name = "neo4j", threshold = 0.1, max_predictions = 100, uri = "bolt://localhost:7687", user = "neo4j", password = "123",predictions_path=None, max_nodes = 5000, max_edges = 5000):
    """
    Returns the predictions for the given database.

    :param db_name: the name of the database. If not present it will be created.
    :param threshold: predictions with a probability below this number will be ignored
    :param max_predictions:
    :param uri: the Bolt Url to Neo4j
    :param user: the user name
    :param password: the password
    :param max_nodes: the maximum amount of nodes to consider
    :param max_edges: the maximum amount of edges to consider
    :return:
    """
    neo = Neo(uri, user, password)
    g = neo.get_graph(db_name, max_nodes, max_edges)
    inf, ids = infer_identities(g, threshold, True, max_predictions)
    df = inference_to_frame(ids).head(max_predictions)
    if predictions_path is None:
        predictions_path = os.path.join(os.getcwd(), "predictions.csv")
    df.to_csv(predictions_path)
    print("Predictions saved to ", predictions_path)
    return g, df


def create_synthetic_neo(db_name = "sample", clear_content = True, uri = "bolt://localhost:7687", user = "neo4j", password = "123", device_count = 150, identity_count = 50, cookie_probability = 0.3, ip_probability = 0.3, location_probability = 0.3):
    """
    Creates a synthetic dataset in Neo4j.

    :param db_name: the name of the database. If not present it will be created.
    :param clear_content: whether to truncate the content before adding the graph
    :param uri: the Bolt Url to Neo4j
    :param user: the user name
    :param password: the password
    :param device_count: how many devices to create
    :param identity_count: how many identities to create
    :param cookie_probability: the probability that a cookie is shared
    :param ip_probability: the probability that an IP node is shared
    :param location_probability: the probability that a Location node is shared
    :return: the graph created in the Neo4j database
    """
    g = create_synthetic_graph(device_count, identity_count, cookie_probability, ip_probability, location_probability)
    neo = Neo(uri, user, password)
    neo.write_graph(db_name, g, clear_content)
    return g


def add_predictions_to_neo(db_name = "neo4j", threshold = 0.1, max_predictions = 100, remove_existing_predictions = True, uri = "bolt://localhost:7687", user = "neo4j", password = "123", max_nodes = 5000, max_edges = 5000):
    """
    Infers identities in the specified database and inserts the inferred edges.

    :param db_name: the name of the database. If not present it will be created.
    :param threshold: predictions with a probability below this number will be ignored
    :param max_predictions: the maximum amount of predictions to generate (top N).
    :param uri: the Bolt Url to Neo4j
    :param user: the user name
    :param password: the password
    :return: the frame with predictions
    :param max_nodes: the maximum amount of nodes to consider
    :param max_edges: the maximum amount of edges to consider
    """
    neo = Neo(uri, user, password)
    if remove_existing_predictions:
        neo.remove_inferred_edges(db_name)
    g = neo.get_graph(db_name, max_nodes, max_edges)
    inf, ids = infer_identities(g, threshold, True, max_predictions)
    neo.write_predictions(db_name, predictions = ids)
    df = inference_to_frame(ids).head(max_predictions)
    return df


def entities_json_to_predictions(json_directory, file_extension = "txt", threshold = 0.1, save_diagram = False, predictions_path = None, max_predictions = 100):
    """
    Infers identities from the given directory with JSON entities.

    :param json_directory: the directory with the entities
    :param file_extension: the extension of the files, default '*.json'
    :param threshold: predictions with a probability below this number will be ignored
    :param save_diagram: whether to save the generated diagram to GraphML for visualization purposes
    :param predictions_path: the path where the predictions as a CSV file will be save. If not specified it will be saved in the current directory as 'predictions.csv'.
    :param max_predictions: the maximum amount of predictions to generate (top N).
    :return: (graph, predictions frame)
    """
    if "." in file_extension:
        file_extension = file_extension[file_extension.index(".") + 1:]
    files = get_files(json_directory, f"*.{file_extension}", True)
    coll = []
    for file_path in files:
        with open(file_path, "rt") as f:
            lines = f.readline()
            for line in lines:
                j = json.load(line)
                coll.append(j)
    g = entities_json_to_graph(coll)
    inf, ids = infer_identities(g, threshold, True, max_predictions)
    if predictions_path is None:
        predictions_path = os.path.join(os.getcwd(), "predictions.csv")
    df.to_csv(predictions_path)
    print("Predictions saved to ", predictions_path)
    return g, df


def create_synthetic_entities_json(directory, lines_per_file = 100, device_count = 150, identity_count = 50, cookie_probability = 0.3, ip_probability = 0.3, location_probability = 0.3):
    """
    Create a symthetic graph and dumps it into the specified directory as a set of files containing JSON entities.

    :param directory: the target directory
    :param lines_per_file: how many lines per file, default 100
    :param device_count: how many devices to generate
    :param identity_count: how many identities to generate (must be less than the device count)
    :param cookie_probability: the probability that a cookie is shared
    :param ip_probability: the probability that an IP node is shared
    :param location_probability: the probability that a Location node is shared
    :return: the generated graph
    """
    # ensure the directory is there
    Path(directory).mkdir(parents = True, exist_ok = True)
    g = create_synthetic_graph(device_count, identity_count, cookie_probability, ip_probability, location_probability)
    js = graph_to_entities_json(g)

    file_number = 1
    chunks = get_chunks(js, lines_per_file)
    for block in chunks:
        file_path = os.path.join(directory, f"data{file_number}.txt")
        with open(file_path, "wt") as f:
            f.writelines([json.dumps(b) for b in block])
        file_number += 1
    return g


def create_synthetic_raw(directory, lines_per_file = 100, device_count = 150, identity_count = 50, cookie_probability = 0.3, ip_probability = 0.3, location_probability = 0.3):
    """
    Create a symthetic graph and dumps it into the specified directory as a set of files gzip file containing TSV.

    :param directory: the target directory
    :param lines_per_file: how many lines per file, default 100
    :param device_count: how many devices to generate
    :param identity_count: how many identities to generate (must be less than the device count)
    :param cookie_probability: the probability that a cookie is shared
    :param ip_probability: the probability that an IP node is shared
    :param location_probability: the probability that a Location node is shared
    :return: the generated graph
    """
    g = create_synthetic_graph(device_count, identity_count, cookie_probability, ip_probability, location_probability)
    graph_to_raw(g, directory, lines_per_file)
    return g
