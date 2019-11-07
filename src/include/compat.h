
#ifndef MTMCOMPAT_H
#define MTMCOMPAT_H

/* Allow use of rdma functions on non-rdma enabled postgres */
#define pg_socket(a, b, c, d) socket(a, b, c)
#define pg_setsockopt(a, b, c, d, e, f) setsockopt(a, b, c, d, e)
#define pg_getsockopt(a, b, c, d, e, f) getsockopt(a, b, c, d, e)
#define pg_set_noblock(a, b) pg_set_noblock(a)
#define pg_bind(a, b, c, d) bind(a, b, c)
#define pg_listen(a, b, c) listen(a, b)
#define pg_connect(a, b, c, d) connect(a, b, c)
#define pg_accept(a, b, c, d) accept(a, b, c)
#define pg_select(a, b, c, d, e, f) select(a, b, c, d, e)
#define pg_send(a, b, c, d, e) send(a, b, c, d)
#define pg_recv(a, b, c, d, e) recv(a, b, c, d)
#define pg_closesocket(a, b) closesocket(a)

#define PQselect(a, b, c, d, e, f) select(a, b, c, d, e)
#define PQisRsocket(a) false

#endif							/* MTMCOMPAT_H */
