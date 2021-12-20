# -*- coding: utf-8 -*-


import unittest
from entity_resolution import get_sample_graph, infer_identity_edges, infer_jaccard_identity


class InferenceTests(unittest.TestCase):

    def test_basic_inference(self):
        g = get_sample_graph()
        inferred = infer_identity_edges(g)
        assert len(inferred) == 2
        inferred.sort()
        assert inferred[0] == (4, 0)
        assert inferred[1] == (6, 0)

    def test_jaccard_inference(self):
        g = get_sample_graph()
        inferred = infer_jaccard_identity(g)
        assert len(inferred) == 2
        print(inferred)


if __name__ == '__main__':
    unittest.main()
