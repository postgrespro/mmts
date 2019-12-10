---- MODULE MtmPrimitiveCurrentMasksFixed ----

\* This is 'fixed' version of MtmPrimitiveCurrentMasks.tla: here node always
\* recovers from node n when asking n to enable it. Though it seems to work,
\* (and TLC doesn't mind) it is not simple and elegant: we must be cautious
\* of concurrent recovery sessions and force online nodes to recover from
\* stale ones on their return.
\* Also, since here participant masks in prepares don't have any order (i.e.
\* number establishing it), in such implementation node must abort its prepares
\* before recovering from anyone; otherwise it risks to get them committed in
\* wrong (before recovery) order as others will happily acknowledge them as soon
\* they enable the node. Model in this file is too simplistic to show this issue
\* though, as prepare happens immediately on all participants.
\*
\* Unneeded remnants of current implementation (e.g. walsenders/walreceivers)
\* are not stripped here, which contributes to number of states explosion.
\* Depth 1 generates 25*10^6 distinct and 10^8 walked states; 22 mins on hydra
\* Depth 2 generates 244*10^6 distinct and 10^9 walked states; 3h 26 mins on hydra
\* Depth 3 generates 2*10^9 distinct and 20*10^9 walked states; 1d 15h on hydra


EXTENDS Integers, Sequences, FiniteSets, TLC
VARIABLES state, logs

\* depth is the model constraint: it sets max number of xacts in the log of single
\* node.
CONSTANT nodes, depth
ASSUME depth \in Nat

\* For specifying symmetry set in cfg file
perms == Permutations(nodes)

n_nodes == Cardinality(nodes)


\**************************************************************************************
\* Helpers
\**************************************************************************************

\* is s1 subsequence of s2?
IsSubSeq(s1, s2) ==
    /\ Len(s1) <= Len(s2)
    /\ SubSeq(s2, 1, Len(s1)) = s1

\* All possible sequences with values in S and len <= n.
\* Thanks Hillel.
SeqMaxLen(S, n) ==  UNION {[1..m -> S] : m \in 0..n}

quorum(mask) == Cardinality({i \in DOMAIN mask : mask[i] = 1}) >= (n_nodes \div 2 + 1)

max(set) == IF set = {} THEN 0 ELSE CHOOSE e1 \in set: \A e2 \in set: e1 >= e2

maxlen(seqs) == max({Len(seqs[n]) : n \in DOMAIN seqs})

\* max lsn of given origin in given log
maxlsn(log, origin) == max({log[j].olsn : j \in {i \in DOMAIN log : log[i].origin = origin }})

\* how far each node's changes are applied in given log?
rep_state(log) == [n \in nodes |-> maxlsn(log,n)]

log_newer_than(log, origin_vec) == SelectSeq(log, LAMBDA e: e.olsn > origin_vec[e.origin])

\* returns not just new status but record with new state because masks might change
new_state(n, old_status, view, enabled, wsndmask, wrcvmask) ==
    LET
      new_status == CASE
        \* This is hardly needed; safety won't be altered if we are in recovery
        \* with less than majority in view mask
        ~ quorum(view) -> "disabled"
        [] quorum(view) /\ old_status = "disabled" -> "recovery"
        \* recovery -> recovered done explicitly in do_recovery()
        [] quorum(view) /\ old_status = "recovered" /\ view = enabled /\ view = wsndmask /\ view = wrcvmask -> "online"
        \* I don't think we need that, nothing should be prepared with minority enabled anyway
        [] quorum(view) /\ old_status = "online" /\ ~quorum(enabled) -> "disabled"
        [] OTHER -> old_status
      \* all zeros but me
      zeros == [[_n \in nodes |-> 0] EXCEPT ![n] = 1]
      new_enabled == IF new_status = "disabled" THEN zeros ELSE enabled
      new_wsndmask == IF new_status = "disabled" THEN zeros ELSE wsndmask
      new_wrcvmask == IF new_status = "disabled" THEN zeros ELSE wrcvmask
    IN
      \* next_lsn goes unchanged
      [state[n] EXCEPT !.status = new_status,
                       !.view = view,
                       !.enabled = new_enabled,
                       !.walsenders = new_wsndmask,
                       !.walreceivers = new_wrcvmask]


\* Type assertions
MtmTypeOk ==
    /\ state \in [nodes -> [next_lsn: 1..(depth + 1),
                            status: {"disabled", "recovery", "recovered", "online"},
                            view: [nodes -> {0, 1}],
                            enabled: [nodes -> {0, 1}],
                            walsenders: [nodes -> {0, 1}],
                            walreceivers: [nodes -> {0, 1}]]]
    /\ logs \in [nodes -> SeqMaxLen([origin: nodes, olsn: 1..depth, participants: [nodes -> {0, 1}]], depth)]


\**************************************************************************************
\* Initial
\**************************************************************************************


Init == /\ state = [n \in nodes |-> [
                        next_lsn |-> 1,
                        status |-> "disabled",
                        view |-> [[_n \in nodes |-> 0] EXCEPT ![n] = 1],
                        enabled |-> [[_n \in nodes |-> 0] EXCEPT ![n] = 1],
                        walsenders |-> [[_n \in nodes |-> 0] EXCEPT ![n] = 1],
                        walreceivers |-> [[_n \in nodes |-> 0] EXCEPT ![n] = 1]
                    ]]
        /\ logs =  [n \in nodes |-> << >>]

\**************************************************************************************
\* Actions
\**************************************************************************************


\* n1 disconnects n2
disconnect(n1, n2) ==
    /\ n1 /= n2
    /\ state[n1].view[n2] = 1

    /\ logs' = logs
    /\  LET
            view == [state[n1].view EXCEPT ![n2] = 0]
            enabled == [state[n1].enabled EXCEPT ![n2] = 0]
            n1_state == new_state(n1, state[n1].status, view, enabled, state[n1].walsenders, state[n2].walreceivers)
        IN
        state' = [state EXCEPT ![n1] = n1_state]


connect(n1, n2) ==
    /\ n1 /= n2
    /\ state[n1].view[n2] = 0

    /\ logs' = logs
    /\  LET
            view == [state[n1].view EXCEPT ![n2] = 1]
            n1_state == new_state(n1, state[n1].status, view, state[n1].enabled, state[n1].walsenders, state[n1].walreceivers)
        IN
            state' = [state EXCEPT ![n1] = n1_state]

\* n1 recovers from n2
do_recovery(n1, n2) ==
    /\ n1 /= n2
    /\ state[n1].view[n2] = 1
    \* Apparently this ensures we won't keep dead node as enabled
    /\ state[n2].view[n1] = 1

    /\  LET
            origin_vec == rep_state(logs[n1])
            new_entries == log_newer_than(logs[n2], origin_vec)
            \* enable n1
            n2_enabled == [state[n2].enabled EXCEPT ![n1] = 1]
            n2_walsenders == [state[n2].walsenders EXCEPT ![n1] = 1]
            n2_state == new_state(n2, state[n2].status, state[n2].view, n2_enabled, n2_walsenders, state[n2].walreceivers)
            n1_walreceivers == [state[n1].walreceivers EXCEPT ![n2] = 1]
            n1_state == new_state(n1, "recovered", state[n1].view, state[n1].enabled, state[n1].walsenders, n1_walreceivers)
        IN
        /\ logs' = [logs EXCEPT ![n1] = logs[n1] \o new_entries]
        /\ state' = [state EXCEPT  ![n1] = n1_state,
                                   ![n2] = n2_state]

do_tx(node) ==
    \* model depth constraint
    /\ Len(logs[node]) < depth
    /\ state[node].status = "online"
    /\ quorum(state[node].enabled)
    \* make sure set of enabled nodes is the same on all participants
    /\ \A n \in nodes: state[node].enabled[n] = 0 \/ state[n].enabled = state[node].enabled
    /\ logs' = [n \in nodes |->
                    IF state[node].enabled[n] = 1
                    THEN Append(logs[n], [origin |-> node, olsn |-> state[node].next_lsn, participants |-> state[node].enabled])
                    ELSE logs[n]]
    /\ state' = [state EXCEPT ![node].next_lsn = state[node].next_lsn + 1]


\**************************************************************************************
\* Final spec
\**************************************************************************************


Next ==     \/ \E n1,n2 \in nodes : connect(n1,n2)
            \/ \E n1,n2 \in nodes : disconnect(n1,n2)
            \/ \E n1,n2 \in nodes : do_recovery(n1,n2)
            \/ \E n \in nodes : do_tx(n)

spec == Init /\ [][Next]_<<state, logs>>


\**************************************************************************************
\* Stuff to check
\**************************************************************************************

\* Make sure every log is sublog of the longest one
OrderOk ==
  LET
      most_advanced_node == CHOOSE n1 \in nodes: \A n2 \in nodes: Len(logs[n1]) >= Len(logs[n2])
  IN
      \A n \in nodes: IsSubSeq(logs[n], logs[most_advanced_node])

LogLen == \A n \in nodes: Len(logs[n]) <= 1 /\ state[n].next_lsn <= 2

====