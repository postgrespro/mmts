#ifndef MTMCOMPAT_H
#define MTMCOMPAT_H

/* EE pooler gets rid of static variable */
#ifndef PGPRO_EE
#define FeBeWaitSetCompat() (MyProcPort->pqcomm_waitset)
#else
#define FeBeWaitSetCompat() (FeBeWaitSet)
#endif

#ifdef PGPRO_EE /* atx */
#define BeginTransactionBlockCompat() (BeginTransactionBlock())
#define UserAbortTransactionBlockCompat(chain) (UserAbortTransactionBlock((chain)))
#else
#define BeginTransactionBlockCompat() (BeginTransactionBlock())
#define UserAbortTransactionBlockCompat(chain) (UserAbortTransactionBlock(chain))
#endif

/* atx renames this for some reason */
//#ifdef PGPRO_EE
//#define on_commits_compat() (pg_on_commit_actions)
//#else
#define on_commits_compat() (on_commits)
//#endif

#endif							/* MTMCOMPAT_H */
