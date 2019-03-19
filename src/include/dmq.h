#ifndef DMQ_H
#define DMQ_H

#include "libpq-fe.h"
#include "lib/stringinfo.h"

typedef int8 DmqDestinationId;
typedef int8 DmqSenderId;

#define DMQ_NAME_MAXLEN 32

extern void dmq_init(void);

extern DmqDestinationId dmq_destination_add(char *connstr, char *sender_name,
											char *receiver_name, int ping_period);
extern void dmq_destination_drop(char *receiver_name);

extern int dmq_attach_receiver(char *sender_name, int mask_pos);
extern void dmq_detach_receiver(char *sender_name);

extern void dmq_terminate_receiver(char *name);

extern void dmq_stream_subscribe(char *stream_name);
extern void dmq_stream_unsubscribe(char *stream_name);

extern bool dmq_pop(DmqSenderId *sender_id, StringInfo msg, uint64 mask);
extern bool dmq_pop_nb(DmqSenderId *sender_id, StringInfo msg, uint64 mask);

extern void dmq_push(DmqDestinationId dest_id, char *stream_name, char *msg);
extern void dmq_push_buffer(DmqDestinationId dest_id, char *stream_name, const void *buffer, size_t len);

typedef void (*dmq_hook_type) (char *);
extern dmq_hook_type dmq_receiver_start_hook;
extern dmq_hook_type dmq_receiver_stop_hook;
extern dmq_hook_type dmq_sender_connect_hook;
extern dmq_hook_type dmq_sender_disconnect_hook;

#endif