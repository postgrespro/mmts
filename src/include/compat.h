
#ifndef MTMCOMPAT_H
#define MTMCOMPAT_H

/* EE pooler gets rid of static variable */
/* TODO: uncomment once pooler lands into 13 */
/* #ifdef PGPRO_EE */
/* #define FeBeWaitSetCompat() (MyProcPort->pqcomm_waitset) */
/* #else */
#define FeBeWaitSetCompat() (FeBeWaitSet)
/* #endif */

/* TODO: uncomment once atx lands into 13 */
/* #ifdef PGPRO_EE /\* atx *\/ */
/* #define BeginTransactionBlockCompat() (BeginTransactionBlock(false)) */
/* #else */
#define BeginTransactionBlockCompat() (BeginTransactionBlock())
/* #endif */

#ifdef XID_IS_64BIT
#define pq_sendxid(s, xid) (pq_sendint64((s), (xid))
#define pq_getmsgxid(s) (pq_getmsgint64(s))
#else
#define pq_sendxid(s, xid) (pq_sendint32((s), (xid))
#define pq_getmsgxid(s) (pq_getmsgint32(s))
#endif

#endif							/* MTMCOMPAT_H */
