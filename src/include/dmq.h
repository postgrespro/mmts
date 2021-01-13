#ifndef DMQ_H
#define DMQ_H

#include "libpq-fe.h"
#include "lib/stringinfo.h"

typedef int8 DmqDestinationId;

#define DMQ_NAME_MAXLEN 32
/* mm currently uses xact gid as stream name, so this should be >= GIDSIZE */
#define DMQ_STREAM_NAME_MAXLEN 200

extern void dmq_init(int send_timeout, int connect_timeout);

#define DMQ_N_MASK_POS 16 /* ought to be >= MTM_MAX_NODES */
extern DmqDestinationId dmq_destination_add(char *connstr, char *sender_name,
											char *receiver_name, int8 recv_mask_pos,
											int ping_period);
extern void dmq_destination_drop(char *receiver_name);
extern void dmq_destination_reconnect(char *receiver_name);

extern void dmq_attach_receiver(char *sender_name, int8 mask_pos);
extern void dmq_detach_receiver(char *sender_name);

extern void dmq_terminate_receiver(char *name);

extern void dmq_reattach_receivers(void);
extern void dmq_stream_subscribe(char *stream_name);
extern void dmq_stream_unsubscribe(void);

extern void dmq_get_sendconn_cnt(uint64 participants, int *sconn_cnt);
extern bool dmq_pop(int8 *sender_mask_pos, StringInfo msg, uint64 mask);
extern bool dmq_pop_nb(int8 *sender_mask_pos, StringInfo msg, uint64 mask, bool *wait);
extern uint64 dmq_purge_failed_participants(uint64 participants, int *sconn_cnt);

extern void dmq_push(DmqDestinationId dest_id, char *stream_name, char *msg);
extern void dmq_push_buffer(DmqDestinationId dest_id, char *stream_name, const void *buffer, size_t len);

typedef void (*dmq_hook_type) (char *);
extern void *(*dmq_receiver_start_hook)(char *sender_name);
extern dmq_hook_type dmq_receiver_stop_hook;
extern void (*dmq_receiver_heartbeat_hook)(char *sender_name, StringInfo msg, void *extra);
extern dmq_hook_type dmq_sender_connect_hook;
extern void (*dmq_sender_heartbeat_hook)(char *receiver_name, StringInfo buf);
extern dmq_hook_type dmq_sender_disconnect_hook;

#endif
