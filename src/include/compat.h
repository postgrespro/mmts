#ifndef MTMCOMPAT_H
#define MTMCOMPAT_H

#define FeBeWaitSetCompat() (FeBeWaitSet)

#ifdef PGPRO_EE /* atx */
#define BeginTransactionBlockCompat() (BeginTransactionBlock(false, NIL))
#define UserAbortTransactionBlockCompat(chain) (UserAbortTransactionBlock(false, (chain)))
#else
#define BeginTransactionBlockCompat() (BeginTransactionBlock())
#define UserAbortTransactionBlockCompat(chain) (UserAbortTransactionBlock(chain))
#endif

#define on_commits_compat() (on_commits)

#endif							/* MTMCOMPAT_H */
