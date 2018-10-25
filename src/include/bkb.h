/*
 * Bron–Kerbosch algorithm to find maximum clique in graph
 */  
#ifndef __BKB_H__
#define __BKB_H__

#include "mm.h"

extern nodemask_t MtmFindMaxClique(nodemask_t* matrix, int n_modes, int* clique_size);

#endif
