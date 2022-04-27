#ifndef MTMCOMPAT_H
#define MTMCOMPAT_H

/* EE pooler gets rid of static variable */
#ifdef PGPRO_EE
#define FeBeWaitSetCompat() (MyProcPort->pqcomm_waitset)
#else
#define FeBeWaitSetCompat() (FeBeWaitSet)
#endif

#ifdef PGPRO_EE /* atx */
#define BeginTransactionBlockCompat() (BeginTransactionBlock(false, NIL))
#define UserAbortTransactionBlockCompat(chain) (UserAbortTransactionBlock(false, (chain)))
#else
#define BeginTransactionBlockCompat() (BeginTransactionBlock())
#define UserAbortTransactionBlockCompat(chain) (UserAbortTransactionBlock(chain))
#endif

#define on_commits_compat() (on_commits)

#endif							/* MTMCOMPAT_H */
