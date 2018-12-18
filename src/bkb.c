#include <stdint.h>
#include "bkb.h"

#define BIT_CHECK(mask, bit) (((mask) & ((nodemask_t)1 << (bit))) != 0)
#define BIT_SET(mask, bit)   (mask |= ((nodemask_t)1 << (bit)))

/*
 * Bron–Kerbosch algorithm to find maximum clique in graph
 */

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


static void findMaximumIndependentSet(NodeList* cur, NodeList* result, nodemask_t* graph, int* oldSet, int ne, int ce) 
{
    int nod = 0;
    int minnod = ce;
    int fixp = -1;
    int s = -1;
	int i, j, k;
	int newce, newne;
	int sel;
    int newSet[MAX_NODES];

    for (i = 0; i < ce && minnod != 0; i++) {
		int p = oldSet[i];
		int cnt = 0;
		int pos = -1;
		
		for (j = ne; j < ce; j++) { 
			if (BIT_CHECK(graph[p], oldSet[j])) {
				if (++cnt == minnod) { 
					break;
				}
				pos = j;
			}
		}
		if (minnod > cnt) {
			minnod = cnt;
			fixp = p;
			if (i < ne) {
				s = pos;
			} else {
				s = i;
				nod = 1;
			}
		}
    }
	

    for (k = minnod + nod; k >= 1; k--) {
        sel = oldSet[s];
		oldSet[s] = oldSet[ne];
		oldSet[ne] = sel;
		
		newne = 0;
		for (i = 0; i < ne; i++) {
			if (!BIT_CHECK(graph[sel], oldSet[i])) {
				newSet[newne++] = oldSet[i];
			}
		}
	    newce = newne;
		for (i = ne + 1; i < ce; i++) {
			if (!BIT_CHECK(graph[sel], oldSet[i])) { 
				newSet[newce++] = oldSet[i];
			}
		}
		_list_append(cur, sel);
		if (newce == 0) {
			if (result->size < cur->size) {
				_list_copy(result, cur);
			}
		} else if (newne < newce) {
			if (cur->size + newce - newne > result->size)  {
				findMaximumIndependentSet(cur, result, graph, newSet, newne, newce);
			}
		}
		cur->size -= 1;
		ne += 1;
		if (k > 1) {
			for (s = ne; !BIT_CHECK(graph[fixp], oldSet[s]); s++);
		}
	}
}

nodemask_t MtmFindMaxClique(nodemask_t* graph, int n_nodes, int* clique_size)
{
	NodeList tmp;
	NodeList result;
	nodemask_t mask;
	int all[MAX_NODES];
	int i;

	tmp.size = 0;
	result.size = 0;
	for (i = 0; i < n_nodes; i++) { 
		all[i] = i;
	}
	findMaximumIndependentSet(&tmp, &result, graph, all, 0, n_nodes);
	mask = 0;
	for (i = 0; i < result.size; i++) { 
		BIT_SET(mask, result.nodes[i]);
	}
	*clique_size = result.size;
	return mask;
}

#ifdef TEST
#include <stdio.h>
#include <stdint.h>
#include "bkb.h"

int main()
{
	nodemask_t matrix[64] = {0};
	nodemask_t clique;
	int clique_size;
	matrix[0] = 6;
	matrix[1] = 4;
	matrix[2] = 1;
	matrix[4] = 3;
	clique = MtmFindMaxClique(matrix, 64, &clique_size);
	printf("Clique=%llx\n", clique);
	return 0;
}
#endif
