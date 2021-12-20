# -*- coding: utf-8 -*-
import math

from entity_resolution import *
from pathlib import Path
import shutil

import unittest


class DataTests(unittest.TestCase):
    def test_enities_lines(self):
        line = create_raw_line("d", "i", "l", "id", "c")
        entity = transform_raw_line_to_entity(line)
        assert entity["Device"] == "d"
        assert entity["IP"] == "i"
        assert entity["Location"] == "l"
        assert entity["Identity"] == "id"
        assert entity["Cookie"] == "c"

        line = create_random_raw_line()
        entity = transform_raw_line_to_entity(line)
        print(entity)

    def test_raw_and_back(self):
        raw_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw")
        # ensure the directory is there
        Path(raw_dir).mkdir(parents = True, exist_ok = True)

        g = create_synthetic_graph(1200, 200)
        graph_to_raw(g, raw_dir)

        count = len([name for name in os.listdir(raw_dir) if os.path.isfile(os.path.join(raw_dir, name))])
        assert count == math.ceil(len(g.edges()) / 1000)

        h = raw_to_graph(raw_dir)
        assert len(g.nodes()) == len(h.nodes())
        assert len(g.edges()) == len(h.edges())

        # clean up
        shutil.rmtree(raw_dir)

    # def test_create_neo_db(self):
    #     neo = Neo()
    #     dbs = neo.get_databases()
    #     print(",".join(dbs))
    #     assert "neo4j" in dbs
    #
    # def test_write_neo(self):
    #     neo = Neo()
    #     g = create_synthetic_graph(50, 10)
    #     # save_open_graph(g)
    #     # g= get_sample_graph()
    #     graph_to_neo(g)
    #
    # def test_predict_to_neo(self):
    #     g = create_synthetic_graph()
    #     inf = infer_identities(g)
    #     g.add_edges_from(inf, label = "INFERRED")
    #     graph_to_neo(g)
