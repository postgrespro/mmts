#!/usr/bin/env sage
import sys, os

from sage.all import *
from subprocess import Popen, PIPE, STDOUT
from random import randrange, randint
import unittest

def run_stdin(input):
    mydir = os.path.dirname(os.path.realpath(__file__))
    binfile = mydir + "/../src/a.out"

    p = Popen(binfile, stdout=PIPE, stdin=PIPE, stderr=STDOUT)
    grep_stdout = p.communicate(input=input)[0]
    return grep_stdout.decode()

def run_bkb(g):
    n = len(g)
    params = str(n) + "\n"
    for i in range(n):
        row = 0
        row |= 1 << i
        for j in range(n):
            if g.has_edge(i, j):
                row |= 1 << j
        params += str(row) + "\n"

    # print(params)
    res = run_stdin(params).strip()
    res = [int(n) for n in res.split(' ')]
    return res


class TestCliqueBKB(unittest.TestCase):

    # test only that max clique size is ok
    def test_random_graphs_size(self):

        for _ in range(1000):
            n_nodes = randint(1, 60)
            n_edges = randrange(1 + (n_nodes * (n_nodes - 1) / 2))
            print("graph |V|={}, |E|={}>".format(n_nodes, n_edges))
            g = graphs.RandomGNM(n_nodes, n_edges)

            clique, clique_size = run_bkb(g)
            clique_members = []
            for i in range(n_nodes):
                if (clique & (1 << i)) != 0:
                    clique_members.append(i)

            sage_clique_maximum = g.clique_maximum()

            print(clique, clique_members, clique_size, sage_clique_maximum, len(sage_clique_maximum))
            self.assertEqual(clique_size, len(sage_clique_maximum))

    # test that found graph is indeed the clique, much more expensive
    def test_random_graphs(self):

        for _ in range(1000):
            n_nodes = randint(1, 30)
            n_edges = randrange(1 + (n_nodes * (n_nodes - 1) / 2))
            print("graph |V|={}, |E|={}>".format(n_nodes, n_edges))
            g = graphs.RandomGNM(n_nodes, n_edges)

            clique, clique_size = run_bkb(g)
            clique_members = []
            for i in range(n_nodes):
                if (clique & (1 << i)) != 0:
                    clique_members.append(i)

            sage_maxcliques = g.cliques_maximal()
            print(sage_maxcliques[0])

            found = False
            for sc in sage_maxcliques:
                if sc == clique_members:
                    found = True
            self.assertTrue(found)

            print(clique, clique_members, clique_size, sage_maxcliques[0], len(sage_maxcliques[0]))



if __name__ == '__main__':
    unittest.main()
