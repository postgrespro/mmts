/*
 * Bron–Kerbosch algorithm to find maximum clique in graph
 */  
#ifndef __BKB_H__
#define __BKB_H__

#include "postgres.h"

#define MAX_NODES sizeof(uint64)

extern uint64 MtmFindMaxClique(uint64* matrix, int n_modes, int* clique_size);

#endif
