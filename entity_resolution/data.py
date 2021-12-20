from typing import List, Union, Any

import networkx as nx
import numpy as np
import os, uuid, random, multiprocessing, json, glob, gzip
from faker import Faker
from boto3.session import Session
import boto3, uuid
from pathlib import Path
from urllib.parse import urlparse
from tqdm import tqdm
from neo4j import GraphDatabase

fake = Faker()

__transform_keys: List[str] = ["Device", "IP", "Location", "Identity", "Cookie"]
"""The attributes of a reduced record."""

__transform_indices: List[int] = [15, 13, 122, 12, 17]
"""The indices in the raw record where the relevant data is extracted."""

__raw_keys: List[Union[str]] = [
    "APP_ID",
    "PLATFORM",
    "ETL_TSTAMP",
    "COLLECTOR_TSTAMP",
    "DVCE_CREATED_TSTAMP",
    "EVENT",
    "EVENT_ID",
    "TXN_ID",
    "NAME_TRACKER",
    "V_TRACKER",
    "V_COLLECTOR",
    "V_ETL",
    "USER_ID",
    "USER_IPADDRESS",
    "USER_FINGERPRINT",
    "DOMAIN_USERID",
    "DOMAIN_SESSIONIDX",
    "NETWORK_USERID",
    "GEO_COUNTRY",
    "GEO_REGION",
    "GEO_CITY",
    "GEO_ZIPCODE",
    "GEO_LATITUDE",
    "GEO_LONGITUDE",
    "GEO_REGION_NAME",
    "IP_ISP",
    "IP_ORGANIZATION",
    "IP_DOMAIN",
    "IP_NETSPEED",
    "PAGE_URL",
    "PAGE_TITLE",
    "PAGE_REFERRER",
    "PAGE_URLSCHEME",
    "PAGE_URLHOST",
    "PAGE_URLPORT",
    "PAGE_URLPATH",
    "PAGE_URLQUERY",
    "PAGE_URLFRAGMENT",
    "REFR_URLSCHEME",
    "REFR_URLHOST",
    "REFR_URLPORT",
    "REFR_URLPATH",
    "REFR_URLQUERY",
    "REFR_URLFRAGMENT",
    "REFR_MEDIUM",
    "REFR_SOURCE",
    "REFR_TERM",
    "MKT_MEDIUM",
    "MKT_SOURCE",
    "MKT_TERM",
    "MKT_CONTENT",
    "MKT_CAMPAIGN",
    "CONTEXTS",
    "SE_CATEGORY",
    "SE_ACTION",
    "SE_LABEL",
    "SE_PROPERTY",
    "SE_VALUE",
    "UNSTRUCT_EVENT",
    "TR_ORDERID",
    "TR_AFFILIATION",
    "TR_TOTAL",
    "TR_TAX",
    "TR_SHIPPING",
    "TR_CITY",
    "TR_STATE",
    "TR_COUNTRY",
    "TI_ORDERID",
    "TI_SKU",
    "TI_NAME",
    "TI_CATEGORY",
    "TI_PRICE",
    "TI_QUANTITY",
    "PP_XOFFSET_MIN",
    "PP_XOFFSET_MAX",
    "PP_YOFFSET_MIN",
    "PP_YOFFSET_MAX",
    "USERAGENT",
    "BR_NAME",
    "BR_FAMILY",
    "BR_VERSION",
    "BR_TYPE",
    "BR_RENDERENGINE",
    "BR_LANG",
    "BR_FEATURES_PDF",
    "BR_FEATURES_FLASH",
    "BR_FEATURES_JAVA",
    "BR_FEATURES_DIRECTOR",
    "BR_FEATURES_QUICKTIME",
    "BR_FEATURES_REALPLAYER",
    "BR_FEATURES_WINDOWSMEDIA",
    "BR_FEATURES_GEARS",
    "BR_FEATURES_SILVERLIGHT",
    "BR_COOKIES",
    "BR_COLORDEPTH",
    "BR_VIEWWIDTH",
    "BR_VIEWHEIGHT",
    "OS_NAME",
    "OS_FAMILY",
    "OS_MANUFACTURER",
    "OS_TIMEZONE",
    "DVCE_TYPE",
    "DVCE_ISMOBILE",
    "DVCE_SCREENWIDTH",
    "DVCE_SCREENHEIGHT",
    "DOC_CHARSET",
    "DOC_WIDTH",
    "DOC_HEIGHT",
    "TR_CURRENCY",
    "TR_TOTAL_BASE",
    "TR_TAX_BASE",
    "TR_SHIPPING_BASE",
    "TI_CURRENCY",
    "TI_PRICE_BASE",
    "BASE_CURRENCY",
    "GEO_TIMEZONE",
    "MKT_CLICKID",
    "MKT_NETWORK",
    "ETL_TAGS",
    "DVCE_SENT_TSTAMP",
    "REFR_DOMAIN_USERID",
    "REFR_DVCE_TSTAMP",
    "DERIVED_CONTEXTS",
    "DOMAIN_SESSIONID",
    "DERIVED_TSTAMP",
    "EVENT_VENDOR",
    "EVENT_NAME",
    "EVENT_FORMAT",
    "EVENT_VERSION",
    "EVENT_FINGERPRINT",
    "TRUE_TSTAMP"
]
"""The field names in the raw data."""

__basic1 = {
    "nodes": [
        [0, "Identity"],
        [1, "Device"],
        [2, "Cookie"],
        [3, "Location"],
        [4, "Device"],
        [5, "IP"],
        [6, "Device"],
    ],
    "edges": [
        [1, 0, "HAS_IDENTITY"],
        [1, 2, "HAS_COOKIE"],
        [1, 3, "HAS_LOCATION"],
        [4, 2, "HAS_COOKIE"],
        [4, 3, "HAS_LOCATION"],
        [4, 5, "HAS_IP"],
        [6, 5, "HAS_IP"],
    ]
}
"""Defines the structure of a basic sample graph."""

__sample_raw = 'mymcle	web	2021-11-12 01:03:14.153	2021-11-12 01:03:13.672	2021-11-12 01:03:13.789	struct	2986f321-cbe2-48ff-860b-c72d3b0714d1		mc	js-2.16.3	ssc-2.3.0-kinesis	stream-2.0.1-common-2.0.1		122.150.112.14		48392ab4-1e81-41c2-bd48-b86a2bc713f7	1	adbacbcf-7f7d-4987-a43a-10705017ae39												https://www.mymusclechef.com/plans/muscle-gain/		https://www.mymusclechef.com/plans/	https	www.mymusclechef.com	443	/plans/muscle-gain/			https	www.mymusclechef.com	443	/plans/			internal								{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.mymusclechef/subject/jsonschema/1-0-1","data":{"goal":"Muscle Gain","gender":"male"}},{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"92f29c39-57bc-4a0b-a62e-98c73ec98fd5"}},{"schema":"iglu:org.w3/PerformanceTiming/jsonschema/1-0-0","data":{"navigationStart":1636678989713,"unloadEventStart":1636678990152,"unloadEventEnd":1636678990153,"redirectStart":0,"redirectEnd":0,"fetchStart":1636678989722,"domainLookupStart":1636678989722,"domainLookupEnd":1636678989722,"connectStart":1636678989722,"connectEnd":1636678989722,"secureConnectionStart":0,"requestStart":1636678989726,"responseStart":1636678990094,"responseEnd":1636678990098,"domLoading":1636678990173,"domInteractive":1636678990425,"domContentLoadedEventStart":1636678990461,"domContentLoadedEventEnd":1636678990461,"domComplete":1636678991629,"loadEventStart":1636678991629,"loadEventEnd":1636678991639}}]}	ux	select-subject-gender																							Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.74 Safari/537.36						en-GB										1	24	980	1603				Australia/Sydney			412	869	UTF-8	980	2061												2021-11-12 01:03:13.795			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"95","useragentMinor":"0","useragentPatch":"4638","useragentVersion":"Chrome 95.0.4638","osFamily":"Linux","osMajor":null,"osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"Linux","deviceFamily":"Other"}},{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-2","data":{"deviceBrand":"Unknown","deviceName":"Linux Desktop","operatingSystemVersionMajor":"Intel x86","layoutEngineNameVersion":"Blink 95.0","operatingSystemNameVersion":"Linux Intel x86_64","layoutEngineNameVersionMajor":"Blink 95","operatingSystemName":"Linux","agentVersionMajor":"95","layoutEngineVersionMajor":"95","deviceClass":"Desktop","agentNameVersionMajor":"Chrome 95","operatingSystemNameVersionMajor":"Linux Intel x86","deviceCpuBits":"64","operatingSystemClass":"Desktop","layoutEngineName":"Blink","agentName":"Chrome","agentVersion":"95.0.4638.74","layoutEngineClass":"Browser","agentNameVersion":"Chrome 95.0.4638.74","operatingSystemVersion":"Intel x86_64","deviceCpu":"Intel x86_64","agentClass":"Browser","layoutEngineVersion":"95.0"}},{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"X-Forwarded-For","value":"122.150.112.14"}},{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"Host","value":"sp.mymusclechef.com"}},{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"User-Agent","value":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.74 Safari/537.36"}},{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"Origin","value":"https://www.mymusclechef.com"}},{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"Referer","value":"https://www.mymusclechef.com/"}},{"schema":"iglu:com.dbip/location/jsonschema/1-0-0","data":{"city":{"geoname_id":2154855,"names":{"en":"North Sydney","ja":"ノースシドニー"}},"continent":{"code":"OC","geoname_id":6255151,"names":{"de":"Ozeanien","en":"Oceania","es":"Oceanía","fa":"اقیانوسیه","fr":"Océanie","ja":"オセアニア","ko":"오세아니아","pt-BR":"Oceania","ru":"Океания","zh-CN":"大洋洲"}},"country":{"geoname_id":2077456,"is_in_european_union":false,"iso_code":"AU","names":{"de":"Australien","en":"Australia","es":"Australia","fa":"استرالیا","fr":"Australie","ja":"オーストラリア","ko":"오스트레일리아","pt-BR":"Austrália","ru":"Австралия","zh-CN":"澳大利亚"}},"location":{"latitude":-33.8392,"longitude":151.206,"time_zone":"Australia/Sydney","weather_code":"ASXX0274"},"postal":{"code":"2055"},"subdivisions":[{"geoname_id":2155400,"iso_code":"NSW","names":{"en":"New South Wales","fr":"Nouvelle-Galles du Sud","pt-BR":"Nova Gales do Sul","ru":"Новый Южный Уэльс"}},{"geoname_id":7839759,"names":{"de":"North Sydney Council","en":"North Sydney Council","fr":"Conseil de Sydney Nord","ja":"ノース・シドニー・カウンシル","zh-CN":"北雪梨議會"}}]}},{"schema":"iglu:com.dbip/isp/jsonschema/1-0-0","data":{"traits":{"autonomous_system_number":9443,"autonomous_system_organization":"VOCUS PTY LTD","connection_type":"Corporate","isp":"Vocus PTY LTD","organization":"VOCUS"}}}]}	521235a6-dad6-4075-92fd-1c4112025e8b	2021-11-12 01:03:13.666	com.google.analytics	event	jsonschema	1-0-0	9f9f142c86e792929835d910ff3d22ed'
"""Real-world line of raw data for testing purposes."""


class Neo(object):
    """Utility class to access Neo4j."""

    def __init__(self, uri = "bolt://localhost:7687", user = "neo4j", password = "123"):
        self.imports = []
        self._driver = GraphDatabase.driver(uri, auth = (user, password))

    def close(self):
        self._driver.close()

    def get_databases(self):
        """
        Returns the list of database names.

        :return: a list of names
        """
        q = f"Show databases"
        coll = []
        with self._driver.session() as graphDB_Session:
            graphDB_Session.run(q)
            rows = graphDB_Session.run(q)
            for row in rows:
                coll.append(row["name"])
        return coll

    def delete_database(self, name):
        """
        Drops the specified database.

        :param name: the name of the database to remove.
        """
        dbs = self.get_databases()
        if name in dbs:
            q = f"DROP DATABASE {name} IF EXISTS"
            with self._driver.session() as graphDB_Session:
                graphDB_Session.run(q)
            print(f"Database '{name}' has been removed.")
        else:
            print(f"Database '{name}' does not exist.")

    def create_database(self, db_name):
        """
        Creates the specified database.

        :param db_name: the name of the new database
        """
        db_name = str.lower(db_name)
        dbs = self.get_databases()
        if db_name in dbs:
            raise Exception(f"Database'{db_name}' already exists.")

        q = f"create database {db_name}"
        with self._driver.session() as graphDB_Session:
            graphDB_Session.run(q)
        print(f"Database '{db_name}' has been created.")

    def truncate_db(self, db_name):
        """
        Truncates the specified database.

        :param db_name: the name of the database.
        """
        db_name = str.lower(db_name)
        dbs = self.get_databases()
        if not db_name in dbs:
            raise Exception(f"Database'{db_name}' does not exists.")

        q = f"Match (x) detach delete x"
        with self._driver.session(database = db_name) as graphDB_Session:
            graphDB_Session.run(q)

    def write_graph(self, db_name, g, clear_content = True):
        """
        Transfers the given graph to Neo4j.

        :param db_name: the name of the database, if not present it will be created
        :param g: the graph
        :param clear_content: whether to clear the database before inserting the new graph
        """
        db_name = str.lower(db_name)
        dbs = self.get_databases()
        if not db_name in dbs:
            self.create_database(db_name)
        else:
            if clear_content:
                self.truncate_db(db_name)

        with self._driver.session(database = db_name) as graphDB_Session:
            print("Creating nodes")
            for i in tqdm(g.nodes()):
                id = g.nodes()[i]["id"]
                label = g.nodes()[i]["label"]
                q = f"Create (n:{label}{{id: $id}})"
                graphDB_Session.run(q, {"id": id})
            print("Creating edges")
            for u, v in tqdm(g.edges()):
                s_id = g.nodes()[u]["id"]
                t_id = g.nodes()[v]["id"]

                s_label = g.nodes()[u]["label"]
                t_label = g.nodes()[v]["label"]

                edge_label = g.edges()[(u, v)]["label"]
                if edge_label == "INFERRED":
                    p = g.edges()[(u, v)]["p"]
                    q = f"""
                                Match (u:{s_label}{{id: $s}})
                                Match (v:{t_label}{{id: $t}})
                                Merge (u)-[:INFERRED{{p: $p}}]->(v)
                                """
                    graphDB_Session.run(q, {"s": s_id, "t": t_id, "p": p})
                else:
                    q = f"""
                                Match (u:{s_label}{{id: $s}})
                                Match (v:{t_label}{{id: $t}})
                                Merge (u)-[:{edge_label}]->(v)
                                """
                    graphDB_Session.run(q, {"s": s_id, "t": t_id})
        print("Done.")

    def get_graph(self, db_name, max_nodes = 5000, max_edges = 5000):
        """
        Fetches the graph in the specified database.

        :param db_name: the database name
        :param max_nodes: the maximum amount of node to fetch, default 5000
        :param max_edges: the maximum amount of edges to fetch, default 5000
        :return:
        """
        g = nx.DiGraph()
        g.name = db_name

        id_dic = {}

        def id_exists(id):
            return id in id_dic

        with self._driver.session(database = db_name) as graphDB_Session:
            # nodes
            q = f"Match (u) return distinct u.id as id, labels(u)[0] as label limit {max_nodes}"
            rows = graphDB_Session.run(q)
            for row in rows:
                u_id = row["id"]
                u_label = row["label"]
                if u_id is None or u_label is None:
                    continue
                if not id_exists(u_id):
                    i = len(g.nodes())
                    g.add_node(i, label = u_label, id = u_id)
                    id_dic[u_id] = i

            q = f"Match (u)-[r]->(v) return u.id as uid, labels(u)[0] as ulabel, type(r) as elabel, v.id as vid, labels(v)[0] as vlabel limit {max_edges}"
            rows = graphDB_Session.run(q)
            for row in rows:
                u_id = row["uid"]
                u_label = row["ulabel"]
                v_id = row["vid"]
                v_label = row["vlabel"]
                e_label = row["elabel"]
                if not id_exists(u_id):
                    continue
                else:
                    i = id_dic[u_id]
                if not id_exists(v_id):
                    continue
                else:
                    j = id_dic[v_id]
                if not e_label is None:
                    g.add_edge(i, j, label = e_label)
        components = list(nx.connected_components(g.to_undirected()))
        largest = [len(c) for c in sorted(components, key = len, reverse = True)][0]
        print(f"Nodes:", len(g.nodes()), "Edges:", len(g.edges()), "Components:", len(components), "Largest component:", largest)
        return g

    def write_predictions(self, db_name, predictions):
        """
        Write the inferred identities as INFERRED-edges to the database.

        :param db_name: the name of the datbase
        :param predictions: the prediction tuples to export
        """
        with self._driver.session(database = db_name) as graphDB_Session:
            for s, t, p in predictions:
                q = """
                Match (u:Device{id: $u})
                Match (v:Identity{id: $v})
                Merge (u)-[:INFERRED{p: $p}]->(v)
                """
                graphDB_Session.run(q, {"u": s, "v": t, "p": p})
        print(f"Written {len(predictions)} prediction(s) to the '{db_name}' database.")

    def remove_inferred_edges(self, db_name):
        with self._driver.session(database = db_name) as graphDB_Session:
            q = "Match ()-[r:INFERRED]->() delete r"
            graphDB_Session.run(q)
        print(f"All INFERRED edges have been removed from database '{db_name}'.")


def __add_node(g, label, id):
    """
    Adds a node with the given label and id.

    :param g: the graph
    :param label: the label
    :param id: the unique id
    :return: the node index
    """
    i = len(g.nodes())
    g.add_node(i, label = label, id = id)
    return i


def __add_nodes(g, label, count):
    """
    Adds a bunch of nodes with the specified label.

    :param g: the graph
    :param label: the label
    :param count: how many nodes to add
    :return: a mapping from node index to node id
    """
    dic = {}
    for _ in range(count):
        id = str(uuid.uuid4())
        i = __add_node(g, label, id)
        dic[i] = id
    return dic


def __connect(g, i, j, label):
    """
    Connects the given indices

    :param g: the graph
    :param i: the source index
    :param j: the target index
    :param label: the label on the edge
    """
    g.add_edge(i, j, label = label)


def __add_cookies(g, device_dic, p = 0.3):
    """
    All devices have a cookie with the same id by default.
    In addition there is a p chance that another one is shared
    and a (1-p) chance that a dangling one is added.
    """
    dic = {}
    for i in device_dic:
        id = device_dic[i]
        # always add the default cookie
        j = __add_node(g, "Cookie", id)
        dic[j] = id
        __connect(g, i, j, "HAS_COOKIE")
    for i in device_dic:
        #  chance to random __connect to another
        if random.random() < p and len(dic.keys()) > 1:
            j = np.random.choice([u for u in dic.keys() if u != i])
            assert i != j, ">>>"
            __connect(g, i, j, "HAS_COOKIE")
        else:
            id = str(uuid.uuid4())
            j = __add_node(g, "Cookie", id)
            dic[j] = i
            __connect(g, i, j, "HAS_COOKIE")

    return dic


def __add_other(g, device_dic, label, p = 0.5):
    """
    Add the location or ip nodes.

    :param g:
    :param device_dic:
    :param label:
    :param p:
    :return:
    """
    upper_label = str.upper(label)
    capital_label = str.capitalize(label)
    if not upper_label in ["IP", "LOCATION"]:
        raise Exception("Unexpected label '{label}'. Only IP or Location is supported.")
    dic = {}

    def has_other(w):
        return len([v for u, v in g.out_edges(w) if g.nodes()[v]["label"] == label]) > 0

    def get_other(w):
        return [v for u, v in g.out_edges(w) if g.nodes()[v]["label"] == label][0]

    def get_parent_devices(w):
        device_is = list(g.predecessors(w))
        assert len(device_is) >= 2

        u, v = np.random.choice(device_is, 2, replace = False)
        assert u != v
        return u, v

    shared_cookies = [u for u in g.nodes() if g.nodes()[u]["label"] == "Cookie" and (g.degree(u) >= 2)]

    for cookie_i in shared_cookies:
        if random.random() < p:

            u, v = get_parent_devices(cookie_i)
            if has_other(u) or has_other(v):
                continue
            assert not u is None and g.has_node(u)
            assert not v is None and g.has_node(v)
            id = str(uuid.uuid4())
            other_i = __add_node(g, capital_label, id)
            dic[other_i] = id

            __connect(g, u, other_i, f"HAS_{upper_label}")
            __connect(g, v, other_i, f"HAS_{upper_label}")
            assert g.degree(other_i) > 1
    # the one remaining
    for device_i in device_dic:
        if has_other(device_i):
            continue
        else:
            id = str(uuid.uuid4())
            other_i = __add_node(g, capital_label, id)
            dic[other_i] = id
            __connect(g, device_i, other_i, f"HAS_{upper_label}")

    return dic


def create_synthetic_graph(device_count = 150, identity_count = 50, cookie_probability = 0.3, ip_probability = 0.3, location_probability = 0.3) -> object:
    """
    Returns a graph with the same topological features as real-world dataset.

    :param device_count: how many devices to create
    :param identity_count: how many identities to create
    :param cookie_probability: the probability that a cookie is shared
    :param ip_probability: the probability that an IP node is shared
    :param location_probability: the probability that a Location node is shared
    :return: a graph
    """
    if identity_count > device_count:
        raise Exception("The device count should be larger than the identity count.")
    g = nx.DiGraph()
    device_dic = __add_nodes(g, "Device", device_count)
    identity_dic = __add_nodes(g, "Identity", identity_count)
    # assigning the identities to the devices
    target_ids = list(identity_dic.keys())
    source_ids = np.random.choice(
        list(device_dic.keys()), identity_count, replace = False)
    for t in zip(source_ids, target_ids):
        __connect(g, *t, "HAS_IDENTITY")

    cookie_dic = __add_cookies(g, device_dic, cookie_probability)

    ip_dic = __add_other(g, device_dic, "IP", ip_probability)
    location_dic = __add_other(g, device_dic, "Location", location_probability)

    # the info can be used to print out stats
    g.info = f"Nodes: {len(g.nodes())}, Edges: {len(g.edges())}"
    return g


def create_synthetic_graphml_graph(add_inference = True, device_count = 150, identity_count = 50, cookie_probability = 0.3, ip_probability = 0.3, location_probability = 0.3):
    """
        Creates a graph with the same topological features as real-world dataset
        and opens the default GraphML application to present it.

        :param add_inference: whether to run the inference and add the edges to the output
        :param device_count: how many devices to create
        :param identity_count: how many identities to create
        :param cookie_probability: the probability that a cookie is shared
        :param ip_probability: the probability that an IP node is shared
        :param location_probability: the probability that a Location node is shared
        :return: a graph
        """
    g = create_synthetic_graph(device_count, identity_count, cookie_probability, ip_probability, location_probability)
    if add_inference:
        from .inference import infer_identities
        inf = infer_identities(g)
        g.add_edges_from([(u, v, {"p": p}) for u, v, p in inf], label = "INFERRED")
    graphml_path = os.path.join(os.getcwd(), "synthetic.graphml")
    print("GraphML diagram at ", graphml_path)
    nx.write_graphml(g, graphml_path)
    os.system(f"open {graphml_path}")
    return g


def create_synthetic_gephi_graph(device_count = 150, identity_count = 50, cookie_probability = 0.3, ip_probability = 0.3, location_probability = 0.3):
    """
            Creates a graph with the same topological features as real-world dataset
            and opens the Gephi application to present it.

            :param device_count: how many devices to create
            :param identity_count: how many identities to create
            :param cookie_probability: the probability that a cookie is shared
            :param ip_probability: the probability that an IP node is shared
            :param location_probability: the probability that a Location node is shared
            :return: a graph
            """
    g = create_synthetic_graph(device_count, identity_count, cookie_probability, ip_probability, location_probability)
    gephi_path = os.path.join(os.getcwd(), "synthetic.gexf")
    print("GraphML diagram at ", gephi_path)
    nx.write_gexf(g, gephi_path)
    os.system(f"open {gephi_path}")
    return g


def __transform_template_to_graph(j):
    """
    Transforms the simple format to a graph.

    :param j:
    :return:
    """
    g = nx.DiGraph()
    for a in j["nodes"]:
        g.add_node(a[0], label = a[1], id = str(uuid.uuid4()))
    for e in j["edges"]:
        g.add_edge(e[0], e[1], label = e[2])
    return g


def __transform_template_to_entities(j):
    g = __transform_template_to_graph(j)
    return graph_to_entities_json(g)


def get_sample_graph(name: str = "Basic"):
    """
    Returns a representative test-graph.

    :param name: the name of the test-case
    :return: a graph
    """
    if name is None:
        raise Exception("No sample name specified.")
    name = str.lower(str(name))
    if name == "basic":
        g = __transform_template_to_graph(__basic1)
        g.name = "Basic"
        return g


def get_sample_entities(name: str = "Basic"):
    """
    Returns a sample of entities for testing purposes.

    :param name:
    :return:
    """
    if name is None:
        raise Exception("No sample name specified.")
    name = str.lower(str(name))
    if name == "basic":
        return __transform_template_to_entities(__basic1)


def get_sample_entity() -> object:
    """
    Returns a typical set of transformed json.

    :return:
    """
    line = create_random_raw_line()
    return transform_raw_line_to_entity(line)
    # return {k: v for k, v in zip(__transform_keys, __sample_transformed)}


def get_sample_raw_line() -> str:
    """
    Returns a typical line from the raw data.

    :return: a string/line

    """
    return __sample_raw


def get_raw_keys():
    """
    Returns the field names of the raw data.

    :return:
    """
    return __raw_keys


def create_random_raw_line():
    """
    Creates a representative TSV line.

    :return:
    """
    device = str(uuid.uuid4())
    ip = fake.ipv4()
    location = fake.city()
    identity = str(uuid.uuid4())
    cookie = str(uuid.uuid4())
    return create_raw_line(device, ip, location, identity, cookie)


def create_raw_line(device, ip, location, identity, cookie, sep = "\t"):
    """
    Creates a line of raw data corresponding to the raw TSV format.
    See the `transform_raw_line_to_entity` method to parse such a line to an entity.˚

    :param device:
    :param ip:
    :param location:
    :param identity:
    :param cookie:
    :param sep:
    :return:
    """
    # the positions not used are kept blank
    ar = len(__raw_keys) * [""]
    ar[__transform_indices[0]] = device
    ar[__transform_indices[1]] = ip
    # ar[__transform_indices[2]] = f"""{{
    #                                     "data":[
    #                                     {{  "schema":"iglu:com.dbip/location/jsonschema/1-0-0",
    #                                         "data":{{
    #                                             "city":{{ "geoname_id": "{location}",
    #                                                     "names": {{"en": "{location}"}}
    #                                                   }}
    #                                         }}
    #                                     }}
    #                                     ]
    #                                     }}
    #                                 """
    ar[__transform_indices[2]] = f"""{{"data":[{{  "schema":"iglu:com.dbip/location/jsonschema/1-0-0","data":{{"city":{{ "geoname_id": "{location}","names": {{"en": "{location}"}}}}}}}}]}}"""

    ar[__transform_indices[3]] = identity
    ar[__transform_indices[4]] = cookie
    return sep.join(ar)


def get_city_from_string(ctx: str):
    """
    Parses the city out of the JSON contained in the given string.

    :param ctx: a string containing JSON.
    :return: (city id, city name) tuple or (None, None) if not found.
    """
    js = json.loads(ctx)
    for j in js["data"]:
        if "iglu:com.dbip/location/jsonschema/1-0-0" == j["schema"]:
            if "city" in j["data"]:
                city_id = j["data"]["city"]["geoname_id"]
                city_name = j["data"]["city"]["names"]["en"]
                return str(city_id), city_name
    return None, None


def transform_raw_line_to_entity(line, sep = "\t"):
    """
    Turns the raw TSV line into an entity.

    :param line: the TSV line
    :param sep: the separator
    :return: an entity
    """
    ha = np.array(get_raw_keys())
    device_position = np.argwhere(ha == "DOMAIN_USERID")[0][0]
    identity_position = np.argwhere(ha == "USER_ID")[0][0]
    cookie_position = np.argwhere(ha == "NETWORK_USERID")[0][0]
    ip_position = np.argwhere(ha == "USER_IPADDRESS")[0][0]
    context_position = np.argwhere(ha == "DERIVED_CONTEXTS")[0][0]
    if type(line) == bytes:
        values = line.decode("utf-8").split(sep)
    else:
        values = line.split(sep)
    device = str.strip(values[device_position])
    if len(device) == 0:
        None
    identity = values[identity_position]
    ip = values[ip_position]
    cookie = values[cookie_position]
    ctx = values[context_position]
    location = get_city_from_string(ctx)[0]
    entity = {k: v for k, v in zip(__transform_keys, [device, ip, location, identity, cookie])}
    # print([device_position, ip_position, context_position, identity_position, cookie_position])
    return entity


def raw_to_entities_json(raw_directory, output_directory = None):
    """
    Transforms the data contained as gzip files in the given directory to JSON entity in another directory.

    :param raw_directory:
    :param output_directory:
    :return:
    """
    ha = np.array(get_raw_keys())
    device_position = np.argwhere(ha == "DOMAIN_USERID")[0][0]
    identity_position = np.argwhere(ha == "USER_ID")[0][0]
    cookie_position = np.argwhere(ha == "NETWORK_USERID")[0][0]
    ip_position = np.argwhere(ha == "USER_IPADDRESS")[0][0]
    context_position = np.argwhere(ha == "DERIVED_CONTEXTS")[0][0]

    # print(device_position, identity_position, cookie_position, ip_position, context_position)


    coll = []

    files = get_files(raw_directory)
    print(f"Reading {len(files)} file(s) in directory '{raw_directory}'.")
    for file_path in tqdm(files):

        with gzip.open(file_path, "rb") as f:
            lines = f.readlines()
            for i, line in enumerate(lines):
                values = line.decode("utf-8").split("\t")
                device = str.strip(values[device_position])
                if len(device) == 0:
                    continue
                identity = values[identity_position]
                ip = values[ip_position]
                cookie = values[cookie_position]
                ctx = values[context_position]
                location = get_city_from_string(ctx)[0]
                entity = {k: v for k, v in zip(__transform_keys, [device, ip, location, identity, cookie])}
                coll.append(entity)
    if not output_directory is None:
        # ensure the directory is there
        Path(output_directory).mkdir(parents = True, exist_ok = True)

        local_dir = Path(output_directory)
        output_path = os.path.join(local_dir, 'raw.json')
        with open(output_path, 'w') as f:
            json.dump(coll, f)
        print("Written to ", os.path.join(local_dir, 'raw.json'))
    return coll


def get_files(directory, pattern = "*.gz", full_path = True):
    """
    Returns the files in the specified directory.

    :param directory: the directory to scan
    :param pattern: the file extension, default '*.gz'.
    :param full_path: if True the full path to the files is retured, otherwise just the name.
    """
    files = glob.glob(os.path.join(directory, "*.gz"))
    if full_path:
        files = [os.path.join(directory, name) for name in files]
    return files


def graph_to_raw(g, raw_directory, lines_per_file = 1000):
    """
    Fills a directory with gzip files corresponding to the raw format.

    :param g: the graph
    :param raw_directory: the destination
    :param lines_per_file: how many lines per file (per gzip file)
    """
    # ensure the directory is there
    Path(raw_directory).mkdir(parents = True, exist_ok = True)
    coll = []

    file_number = 1
    for i, e in enumerate(g.edges()):
        entity = {
            "Device": "",
            "IP": "",
            "Identity": "",
            "Location": "",
            "Cookie": ""
        }
        u, v = e
        source = g.nodes()[u]
        target = g.nodes()[v]
        entity[source["label"]] = source["id"]
        entity[target["label"]] = target["id"]
        entity = {str.lower(k): entity[k] for k in entity}
        line = create_raw_line(**entity)
        coll.append(line)
        if i % lines_per_file == 0 and i > 0:
            file_path = os.path.join(raw_directory, f"data{file_number}.gz")
            file_number += 1
            with gzip.open(file_path, 'wb') as f:
                f.write("\n".join(coll).encode())
            coll = []
    if len(coll) > 0:
        file_path = os.path.join(raw_directory, f"data{file_number}.gz")
        with gzip.open(file_path, 'wb') as f:
            f.write("\n".join(coll).encode())
    print(f"Written {file_number} file(s) in '{raw_directory}'.")


def raw_to_graph(raw_directory):
    """
    Returns the graph contained in the directory with the raw files.

    :param json_data:
    :return:
    """
    entities_json = raw_to_entities_json(raw_directory)
    return entities_json_to_graph(entities_json)


def entities_json_to_graph(entities_json):
    """
    Turns the given set of entities to a graph.

    :param entities_json: the array of entities
    :return: a graph
    """
    device_nodes = {}
    ip_nodes = {}
    location_nodes = {}
    identity_nodes = {}
    cookie_nodes = {}
    label_dic = {"Device": device_nodes, "IP": ip_nodes, "Location": location_nodes, "Identity": identity_nodes, "Cookie": cookie_nodes}
    g = nx.DiGraph()

    def get_node_index(label, id):
        id = str.strip(str.lower(str(id)))
        if id == "nan" or id == "none" or id == "":
            return None
        dic = label_dic[label]
        if not id in dic:
            i = len(g.nodes())
            g.add_node(i, label = label, id = id)
            dic[id] = i
            # # case of a Device there is always a Cookie with the same id
            # if label == "Device":
            #     j = len(g.nodes())
            #     g.add_node(j, label = "Cookie", id = id)
            #     g.add_edge(i, j, label = "HAS_COOKIE")
        return dic[id]

    def connect(device_index, target_label, target_index):
        if target_index is None:
            return
        g.add_edge(device_index, target_index,
                   label = "HAS_" + str.upper(target_label))

    for r in entities_json:
        device_index = get_node_index("Device", r["Device"])
        if device_index is None:
            print("oops, no device index here")
            # raise Exception("Device with nil id")
            continue
        ip_index = get_node_index("IP", r["IP"])
        location_index = get_node_index("Location", r["Location"])
        identity_index = get_node_index("Identity", r["Identity"])
        cookie_index = get_node_index("Cookie", r["Cookie"])

        connect(device_index, "IP", ip_index)
        connect(device_index, "Location", location_index)
        connect(device_index, "Identity", identity_index)
        connect(device_index, "Cookie", cookie_index)
    return g


def graph_to_entities_json(g):
    """
    Converts the given graph to entities JSON.
    :param g: a graph
    :return: an array of JSON
    """
    entities = []
    for u, v in g.edges():
        entity = {
            "Device": "",
            "IP": "",
            "Identity": "",
            "Location": "",
            "Cookie": ""
        }
        source = g.nodes()[u]
        target = g.nodes()[v]
        entity[source["label"]] = source["id"]
        entity[target["label"]] = target["id"]
        entities.append(entity)
    return entities


def download_s3_folder(s3_uri = "s3://your-thing/your-bucket/", local_dir = "./raw"):
    """
    Download the s3 contents of a folder.
    Note that you need to have aws configured (aws configure in terminal).

    Use for example
        `download_s3_folder("s3://your-thing/your-bucket", "~/temp/bucket")`

    :param s3_uri:the s3 uri to the top level of the files you wish to download
    :param local_dir:a relative or absolute directory path in the local file system
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(urlparse(s3_uri).hostname)
    s3_path = urlparse(s3_uri).path.lstrip('/')
    if local_dir is not None:
        local_dir = Path(local_dir)
    for obj in bucket.objects.filter(Prefix = s3_path):
        target = obj.key if local_dir is None else local_dir / Path(obj.key).relative_to(s3_path)
        target.parent.mkdir(parents = True, exist_ok = True)
        if obj.key[-1] == '/':
            continue
        bucket.download_file(obj.key, str(target))
        print(obj.key)


def create_neo_database(db_name = "newDatabase", uri = "bolt://localhost:7687", user = "neo4j", password = "123"):
    """
    Creates a new Neo4j database.

    :param db_name: the name of the datbase
    :param uri: the Bolt Url to Neo4j
    :param user: the user name
    :param password: the password
    """
    neo = Neo(uri, user, password)
    neo.create_database(db_name)


def delete_neo_database(db_name = "newDatabase", uri = "bolt://localhost:7687", user = "neo4j", password = "123"):
    """
    Deletes the specified database.

    :param db_name: the name of the datbase
    :param uri: the Bolt Url to Neo4j
    :param user: the user name
    :param password: the password
    """
    neo = Neo()
    neo.delete_database(db_name)


def graph_to_neo(g, db_name = "newDatabase", uri = "bolt://localhost:7687", user = "neo4j", password = "123", clear_content = True):
    """
    Transfers the given graph to Neo4j.


    :param g: the graph
    :param db_name: the name of the datbase
    :param uri: the Bolt Url to Neo4j
    :param user: the user name
    :param password: the password
    :param clear_content: whether the database should be truncated before adding the graph
    """
    neo = Neo(uri, user, password)
    neo.write_graph(db_name, g, clear_content)


def get_chunks(lst, n):
    """
     Yield successive n-sized chunks from lst.

     :param lst: a list
     :param n: the size of the chunks

     """

    for i in range(0, len(lst), n):
        yield lst[i:i + n]
