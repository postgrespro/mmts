#!/usr/bin/env sage
import sys, os

from sage.all import *
from subprocess import Popen, PIPE, STDOUT
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

    print(params)
    res = run_stdin(params).strip()
    res = [int(n) for n in res.split(' ')]
    return res


class TestCliqueBKB(unittest.TestCase):

    def test_random_graphs(self):

        for _ in range(1000):
            while True:
                g = graphs.RandomGNM(60,1700)
                if g.is_connected():
                    break

            clique, clique_size = run_bkb(g)

            print(clique, clique_size, len(g.clique_maximum()))



if __name__ == '__main__':
    unittest.main()
