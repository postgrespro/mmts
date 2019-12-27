/*
 * Bron–Kerbosch algorithm to find maximum clique in graph
 */
#ifndef __BKB_H__
#define __BKB_H__

#include "postgres.h"

#include "multimaster.h" /* xxx move nodemask to separate file */

extern uint64 MtmFindMaxClique(uint64 *matrix, int n_modes, int *clique_size);

#endif
