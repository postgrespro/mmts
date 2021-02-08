#ifndef MTMCOMPAT_H
#define MTMCOMPAT_H

/* EE pooler gets rid of static variable */
/* TODO: uncomment once pooler lands into 13 */
/* #ifdef PGPRO_EE */
/* #define FeBeWaitSetCompat() (MyProcPort->pqcomm_waitset) */
/* #else */
#define FeBeWaitSetCompat() (FeBeWaitSet)
/* #endif */

#ifdef PGPRO_EE /* atx */
#define BeginTransactionBlockCompat() (BeginTransactionBlock(false))
#define UserAbortTransactionBlockCompat(chain) (UserAbortTransactionBlock(false, (chain)))
#else
#define BeginTransactionBlockCompat() (BeginTransactionBlock())
#define UserAbortTransactionBlockCompat(chain) (UserAbortTransactionBlock(chain))
#endif

/* atx renames this for some reason */
#ifdef PGPRO_EE
#define on_commits_compat() (pg_on_commit_actions)
#else
#define on_commits_compat() (on_commits)
#endif

#ifdef XID_IS_64BIT
#define pq_sendxid(s, xid) (pq_sendint64((s), (xid))
#define pq_getmsgxid(s) (pq_getmsgint64(s))
#else
#define pq_sendxid(s, xid) (pq_sendint32((s), (xid))
#define pq_getmsgxid(s) (pq_getmsgint32(s))
#endif


#endif							/* MTMCOMPAT_H */
