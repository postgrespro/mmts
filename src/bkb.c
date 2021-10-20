/*
 * bkb.c
 *
 * Bron–Kerbosch algorithm to find maximum clique in a graph.
 *
 * Copyright (c) 2017-2021, Postgres Professional
 *
 */
#ifndef TEST
#include "bkb.h"

#else
#include <assert.h>
#include <stdint.h>
#define Assert(expr) assert(expr)
typedef uint64_t nodemask_t;
#define MAX_NODES 64
#define BIT_CHECK(mask, bit) (((mask) & ((nodemask_t)1 << (bit))) != 0)
#define BIT_SET(mask, bit)   (mask |= ((nodemask_t)1 << (bit)))
#endif

typedef struct {
	int size;
	int nodes[MAX_NODES];
} NodeList;

static void
_list_append(NodeList* list, int n)
{
	list->nodes[list->size++] = n;
}

static void
_list_copy(NodeList* dst, NodeList const* src)
{
	int i;
	int n = src->size;
	dst->size = n;
	for (i = 0; i < n; i++) {
		dst->nodes[i] = src->nodes[i];
	}
}

static nodemask_t
_list_to_nodemask(NodeList *list)
{
	nodemask_t res = 0;
	int i;

	for (i = 0; i < list->size; i++)
		BIT_SET(res, list->nodes[i]);
	return res;
}

/*
 * See original paper
 *   Bron, Coen; Kerbosch, Joep (1973), "Algorithm 457: finding all cliques of
 *   an undirected graph", Commun. ACM, ACM, 16 (9): 575–577
 * or wiki article (I recommend the latter). Var names (and generally the code)
 * here closely resemble ones in the original paper and deserve some deciphering:
 *   - cur is R in wiki
 *   - oldSet[0; ne) is X in wiki
 *   - oldSet[ne; ce) is P in wiki
 *
 * Pristine Bron-Kerbosch algorithm calculates *all* max cliques. In mtm we
 * don't need that, so we return in result only one biggest max clique
 * (actually, this means we could avoid maintaining X altogether).
 * What we do need though is deterministic calculation, so that whenever we
 * have a majority of nodes seeing each other, *all* members of some such
 * majority calculate *the same* clique. e.g. with topology
 *
 *     2
 *    /|\
 *   1 | 3
 *    \|/
 *     4
 *
 * 2 and 4 must calculate the same clique, or we won't converge.
 * To this end, we compare max cliques by nodemask and pick the
 * smallest one.
 */
static void
extend(NodeList* cur, NodeList* result, nodemask_t* graph, int* oldSet, int ne, int ce)
{
	int nod = 0;
	int minnod = ce;
	int fixp = -1; /* pivot (u in wiki) */
	/* index in oldSet of next vertice we'll include in R -- vertex v in wiki*/
	int s = -1;
	int i, j, k;
	int newce, newne;
	int sel; /* the vertex moved P->R itself, pointed to by s -- v in wiki */
	int newSet[MAX_NODES];

	/* Choose the pivot vertex fixp */
	for (i = 0; i < ce && minnod != 0; i++)
	{
		int p = oldSet[i];
		int cnt = 0;
		int pos = -1;

		/*
		 * Count how many non-neighbours of potential pivot we have in P.
		 * Counterintuitively, we require input to have self-loops, so node is
		 * sorta neighbour of itself, though we must also recurse into it and
		 * thus we miss it here (in cnt) and count it in nod instead.
		 * This mumbo-jumbo is important as it forces (cnt < minnod) be true
		 * when P contains only one vertex (minnod=1 initially).
		 * I'd actually make initial minnod bigger and remove self loops...
		 */
		for (j = ne; j < ce && cnt < minnod; j++)
		{
			if (!BIT_CHECK(graph[p], oldSet[j]))
			{
				cnt++;
				pos = j;
			}
		}

		if (cnt < minnod)
		{
			minnod = cnt;
			fixp = p;
			if (i < ne)
			{
				/* if pivot is from X, not P, take random non-neighbour */
				s = pos;
			}
			else
			{
				/*
				 * else, process pivot itself first, otherwise we won't find
				 * it in the loop below as pivot is a neighbour of itself
				 */
				s = i;
				/* don't forget to increment num of nodes to recurse to */
				nod = 1;
			}
		}
	}

	for (k = minnod + nod; k >= 1; k--)
	{
		Assert(s >= 0);
		Assert(s < MAX_NODES);
		Assert(ne >= 0);
		Assert(ne < MAX_NODES);
		Assert(ce >= 0);
		Assert(ce < MAX_NODES);

		/*
		 * put (wiki) v on the border of X and P, we'll move the border to
		 * relocate the vertex
		 */
		sel = oldSet[s];
		oldSet[s] = oldSet[ne];
		oldSet[ne] = sel;

		newne = 0;
		/* form X for recursive call -- leave only v's neighbours */
		for (i = 0; i < ne; i++) {
			if (BIT_CHECK(graph[sel], oldSet[i])) {
				newSet[newne++] = oldSet[i];
			}
		}

		newce = newne;
		/*
		 * similarly, form P for recursive call -- leave only v's neighbours
		 *
		 * + 1 skips v itself, which is moved to R (again the crutch
		 * introduced by self loops)
		 */
		for (i = ne + 1; i < ce; i++) {
			if (BIT_CHECK(graph[sel], oldSet[i])) {
				newSet[newce++] = oldSet[i];
			}
		}
		/* push v to R */
		_list_append(cur, sel);
		if (newce == 0) { /* both P and X are empty => max clique */
			if (result->size < cur->size ||
				(result->size == cur->size &&
				 _list_to_nodemask(result) > _list_to_nodemask(cur))) {
				_list_copy(result, cur);
			}
		} else if (newne < newce) { /* P is not empty, so recurse */
			if (cur->size + newce - newne > result->size)  {
				extend(cur, result, graph, newSet, newne, newce);
			}
		}
		/* remove v back from R for the next iteration */
		cur->size -= 1;
		/* move v from P to X */
		ne += 1;
		/* and find in P next non-neighbour of pivot */
		if (k > 1)
		{

			for (s = ne; BIT_CHECK(graph[fixp], oldSet[s]); s++)
			{
				Assert(s < MAX_NODES);
			}
		}
	}
}

/*
 * Deterministically (c.f. extend) calculates biggest max clique of the graph.
 * The matrix must be symmetric (undirected graph) and must have 1 on the
 * diagonal (self loops).
 *
 * Note that this API renders impossible to distinguish absent node from node
 * without any edges -- absent nodes with ids <= n_nodes must still have 1
 * on the diagonal. This is fine as we are not interested much in cliques
 * of size 1, they never form majority; well, not as far as we don't support
 * cluster of size 1.
 */
nodemask_t
MtmFindMaxClique(nodemask_t* graph, int n_nodes, int* clique_size)
{
	NodeList tmp;
	NodeList result;
	int all[MAX_NODES];
	int i;
	int j;

	tmp.size = 0;
	result.size = 0;
	for (i = 0; i < MAX_NODES; i++)
		all[i] = i;

	/* check that matrix is symmetric */
	for (i = 0; i < n_nodes; i++)
	for (j = 0; j < n_nodes; j++)
		Assert(BIT_CHECK(graph[i], j) == BIT_CHECK(graph[j], i));

	/* algorithm requires diagonal elements to be set */
	for (i = 0; i < n_nodes; i++)
		Assert(BIT_CHECK(graph[i], i));

	extend(&tmp, &result, graph, all, 0, n_nodes);

	*clique_size = result.size;
	return _list_to_nodemask(&result);
}

#ifdef TEST
#include <stdio.h>

/*
 * To run some randomized tests, compile with -DTEST to ./a.out, e.g.
 * gcc -ggdb3 -O0 -DTEST bkb.c
 * , install sage and run ./test_bkb.sage.py
 */

int main()
{
	nodemask_t matrix[64] = {0};
	nodemask_t clique;
	int clique_size;
	int n_nodes;

	n_nodes = 4;
	matrix[0] = 15; /* 1111 */
	matrix[1] = 15; /* 1111 */
	matrix[2] = 7;	/* 0111 */
	matrix[3] = 11; /* 1011 */

	scanf("%d", &n_nodes);
	for (int i = 0; i < n_nodes; i++)
	{
		nodemask_t row;
		scanf("%ld", &row);
		matrix[i] = row;
	}

	clique = MtmFindMaxClique(matrix, n_nodes, &clique_size);
	printf("%ld %d\n", clique, clique_size);
	return 0;
}
#endif
