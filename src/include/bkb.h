/*
 * Bron–Kerbosch algorithm to find maximum clique in graph
 */  
#ifndef __BKB_H__
#define __BKB_H__

#include "postgres.h" /* nodemask_t */

#define MAX_NODES sizeof(nodemask_t)
typedef uint64 nodemask_t;

extern nodemask_t MtmFindMaxClique(nodemask_t* matrix, int n_modes, int* clique_size);

#endif
