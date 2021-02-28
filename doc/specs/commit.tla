---- MODULE commit ----

\* XXX: here OC sends messages strictly after his own stable storage
\* write. It makes sence to check optimization where we can send state
\* concurrently with write (but then check that local write already happend
\* before proceeding to the next phase).

\* XXX: try to model recovering node somehow

\* XXX: we may add 'any -> idle' edge in resolver
\* state machine to simulate resolver crash

\* XXX: maybe remove copypasted a/c and pa/pc code

EXTENDS Integers, FiniteSets, TLC
VARIABLES node_data, msgs

n_nodes   == 3
nodes     == 1..n_nodes \* {"node1","node2","node3"}
terms     == 1..4
majority  == n_nodes \div 2 + 1
max(s)    == CHOOSE x \in s : \A y \in s : x >= y
max2(s)   == CHOOSE t1 \in s : \A t2 \in s : t1[1] > t2[1] \/ (t1[1] = t2[1] /\ t1[2] >= t2[2])
decided(s)== s \in {"c", "a"}
Send(m)   == msgs' = msgs \cup {m}

init ==
    /\ node_data = [n \in nodes |-> [
                        le |-> 1,
                        ce |-> 0,
                        la |-> 0,
                        ca |-> 0,
                        state |-> "w",
                        resolver_state |-> "idle"
                   ]]
    /\ msgs = {}

\**************************************************************************************
\* Type checks
\**************************************************************************************

3PCStates == {"w", "p", "pc", "pa", "c", "a"}

possible_messages ==
    [type: {">prepare",">commit",">abort"}, from: nodes]
        \cup
    {m \in [type: {">precommit",">preabort",">status"}, from: nodes, le: terms, ce: nodes]: m.ce = m.from}
        \cup
    {m \in [type: {"<prepared","<aborted"}, to: nodes, from: nodes]: m.to /= m.from}
        \cup
    {m \in [type: {"<precommitted", "<preaborted"}, to: nodes, from: nodes, le: terms, ce: nodes]: m.to = m.ce}
        \cup
    [type: {"<status"}, to: nodes, from: nodes, le: terms, ce: nodes,
                        la: terms \cup {0}, ca: nodes \cup {0}, state: 3PCStates]

\* Resolver process state machine:
\*
\* idle -> ask_status -> recv_statuses -> done
\*                             |-> await_acks -> done
ResolverStates == {
    "idle",
    "ask_status",
    "recv_statuses",
    "await_acks",
    "done"
}

types_correct1 ==
    msgs \subseteq possible_messages
    
types_correct2 ==
    \A n \in nodes:
        /\ node_data[n].state \in 3PCStates
        /\ node_data[n].resolver_state \in ResolverStates

types_correct ==
    types_correct1 /\ types_correct2

consistency ==
  \A n1, n2 \in nodes : ~ /\ node_data[n1].state = "a"
                          /\ node_data[n2].state = "c"




\**************************************************************************************
\* Original coordinator actions
\**************************************************************************************

oc_prepare(n) ==
    /\ node_data[n].resolver_state = "idle"
    /\ node_data[n].state = "w"
    /\ \A m \in msgs: m.type /= ">prepare" \* oc is just first who executed oc_prepare()
    /\ Send([type |-> ">prepare", from |-> n])
    /\ node_data' = [node_data EXCEPT ![n].state = "p"]

oc_on_any_abort(n) ==
    /\ node_data[n].resolver_state = "idle"
    /\ \E m \in msgs: m.type /= ">prepare" /\ m.from = n
    /\ ~decided(node_data[n].state)
    /\ \E m \in msgs:
        /\ m.type = "<aborted"
        /\ m.to = n
        /\ Send([type |-> ">abort", from |-> n])
        /\ node_data' = [node_data EXCEPT ![n].state = "a"]

oc_all_prepared(n) ==
    /\ node_data[n].resolver_state = "idle"
    /\ \E m \in msgs: m.type /= ">prepare" /\ m.from = n
    /\ node_data[n].state = "p"
    /\ (node_data[n].le = 1 /\ node_data[n].ce = 0)
    /\  LET
            pmset == {m \in msgs : m.type = "<prepared" /\ m.to = n}
        IN
        /\ Cardinality({m.from : m \in pmset} \cup {n}) = n_nodes
        /\ Send([type |-> ">precommit", from |-> n, le |-> 1, ce |-> 0])
        /\ node_data' = [node_data EXCEPT ![n].state = "pc",
                                          ![n].la = 1,
                                          ![n].ca = 0]

oc_maj_precommit(n) ==
    /\ node_data[n].resolver_state = "idle"
    /\ \E m \in msgs: m.type /= ">prepare" /\ m.from = n
    /\ node_data[n].state = "pc"
    /\ (node_data[n].le = 1 /\ node_data[n].ce = 0)
    /\  LET
            pmset == {m \in msgs : m.type = "<precommitted" /\ m.to = n /\ m.ce = 0 /\ m.le = 1}
        IN
        /\ Cardinality({m.from : m \in pmset} \cup {n}) >= majority \* XXX
        /\ Send([type |-> ">commit", from |-> n])
        /\ node_data' = [node_data EXCEPT ![n].state = "c"]

\**************************************************************************************
\* Peer actions
\**************************************************************************************

peer_on_prepare(n) ==
    /\ node_data[n].state = "w"
    /\ \E m \in msgs: 
        /\ m.type = ">prepare"
        /\ m.from /= n
        /\  \/  /\ Send([type |-> "<aborted",  to |-> m.from, from |-> n])
                /\ node_data' = [node_data EXCEPT ![n].state = "a"]
            \/  /\ Send([type |-> "<prepared", to |-> m.from, from |-> n])
                /\ node_data' = [node_data EXCEPT ![n].state = "p"]

peer_on_abort(n) ==
    /\ ~decided(node_data[n].state)
    /\ \E m \in msgs:
        /\ m.type = ">abort"
        /\ m.from /= n
        /\ node_data' = [node_data EXCEPT ![n].state = "a"]
        /\ msgs' = msgs

peer_on_commit(n) ==
    /\ ~decided(node_data[n].state)
    /\ \E m \in msgs:
        /\ m.type = ">commit"
        /\ m.from /= n
        /\ node_data' = [node_data EXCEPT ![n].state = "c"]
        /\ msgs' = msgs

peer_on_precommit(n) ==
    /\ ~decided(node_data[n].state)
    /\ \E m \in msgs: 
        /\ m.type = ">precommit"
        /\ m.from /= n
        /\ (m.le > node_data[n].le \/ (m.le = node_data[n].le /\ m.from >= node_data[n].ce)) \* XXX: replace with macros
        /\ node_data' = [node_data EXCEPT ![n].state = "pc",
                                          ![n].le = m.le,
                                          ![n].ce = m.from,
                                          ![n].la = m.le,
                                          ![n].ca = m.from,
                                          ![n].resolver_state = IF m.from /= node_data[n].ce
                                                                THEN "idle"
                                                                ELSE node_data[n].resolver_state
                                          ]
        /\ Send([type |-> "<precommitted", to |-> m.from, from |-> n, le |-> m.le, ce |-> m.from])

peer_on_preabort(n) ==
    /\ ~decided(node_data[n].state)
    /\ \E m \in msgs: 
        /\ m.type = ">preabort"
        /\ m.from /= n
        /\ (m.le > node_data[n].le \/ (m.le = node_data[n].le /\ m.from >= node_data[n].ce))
        /\ node_data' = [node_data EXCEPT ![n].state = "pa",
                                          ![n].le = m.le,
                                          ![n].ce = m.from,
                                          ![n].la = m.le,
                                          ![n].ca = m.from,
                                          ![n].resolver_state = IF m.from /= node_data[n].ce
                                                                THEN "idle"
                                                                ELSE node_data[n].resolver_state
                                          ]
        /\ Send([type |-> "<preaborted", to |-> m.from, from |-> n, le |-> m.le, ce |-> m.from])

peer_on_status(n) ==
    \E m \in msgs:
        /\ m.type = ">status"
        /\ m.from /= n
        /\ (m.le > node_data[n].le \/ (m.le = node_data[n].le /\ m.from > node_data[n].ce))
        /\ node_data' = [node_data EXCEPT ![n].le = m.le,
                                          ![n].ce = m.from,
                                          ![n].resolver_state = "idle"] \* any better way?
        /\ Send([type |-> "<status",
                 from |-> n,
                 to |-> m.from,
                 le |-> m.le,
                 ce |-> m.from,
                 la |-> node_data[n].la,
                 ca |-> node_data[n].ca,
                 state |-> node_data[n].state
                ])


\**************************************************************************************
\* Resolver states
\**************************************************************************************

resolver_start(n) ==
    /\ node_data[n].resolver_state = "idle"
    /\ ~decided(node_data[n].state)

    /\  LET
            \* XXX: asking everybody, should be changed to majority
            max_le == max({node_data[nd].le : nd \in DOMAIN node_data})
        IN
        /\ (max_le + 1) \in terms
        /\ node_data' = [node_data EXCEPT ![n].resolver_state = "ask_status",
                                          ![n].le = max_le + 1,
                                          ![n].ce = n]
        /\ msgs' = msgs

resolver_ask_status(n) ==
    /\ node_data[n].resolver_state = "ask_status"
\*     /\ node_data[n].ce = n
    /\ Send([type |-> ">status", from |-> n, le |-> node_data[n].le, ce |-> node_data[n].ce])
    /\ node_data' = [node_data EXCEPT ![n].resolver_state = "recv_statuses"]

resolver_recv_status(n) ==
    /\ node_data[n].resolver_state = "recv_statuses"
\*     /\ node_data[n].ce = n
    /\  LET
            msgs_w_me == msgs \cup {[type |-> "<status",
                from |-> n,
                le |-> node_data[n].le,
                ce |-> n,
                la |-> node_data[n].la,
                ca |-> node_data[n].ca,
                state |-> node_data[n].state
            ]}
            stset == {m \in msgs_w_me : m.type = "<status" /\ m.ce = n /\ m.le = node_data[n].le}
        IN
        \/  \E m \in stset:
                /\ m.state = "a"
                /\ node_data' = [node_data EXCEPT ![n].state = "a",
                                                  ![n].resolver_state = "done"]
                /\ Send([type |-> ">abort", from |-> n])

        \/  \E m \in stset:
                /\ m.state = "c"
                /\ node_data' = [node_data EXCEPT ![n].state = "c",
                                                  ![n].resolver_state = "done"]
                /\ Send([type |-> ">commit", from |-> n])
            
        \/  LET
                ma == max2({<<m.la, m.ca>> : m \in stset} \cup {<<node_data[n].la, node_data[n].ca>>})
                imac == \A m \in stset: (m.la = ma[1] /\ m.ca = ma[2]) => m.state = "pc"
                quorum == Cardinality({m.from : m \in stset} \cup {n}) >= majority \* XXX
            IN
            \/  /\ imac
                /\ quorum
                /\ node_data' = [node_data EXCEPT ![n].state = "pc",
                                              ![n].la = node_data[n].le,
                                              ![n].ca = node_data[n].ce,
                                              ![n].resolver_state = "await_acks"]
                /\ Send([type |-> ">precommit", from |-> n, le |-> node_data[n].le, ce |-> node_data[n].ce])

            \/  /\ ~imac
                /\ quorum
                /\ node_data' = [node_data EXCEPT ![n].state = "pa",
                                              ![n].la = node_data[n].le,
                                              ![n].ca = node_data[n].ce,
                                              ![n].resolver_state = "await_acks"]
                /\ Send([type |-> ">preabort", from |-> n, le |-> node_data[n].le, ce |-> node_data[n].ce])

resolver_await_acks(n) ==
    /\ node_data[n].resolver_state = "await_acks"
    /\  LET
            acks_set == {m \in msgs : /\ m.type \in {"<precommitted", "<preaborted"}
                                      /\ m.ce = n /\ m.le = node_data[n].le}
            acks_value == CHOOSE s \in {m.type : m \in acks_set} : TRUE
            
            quorum == Cardinality({m.from : m \in acks_set} \cup {n}) >= majority \* XXX
            
            value == IF acks_value = "<precommitted" THEN ">commit" ELSE ">abort"
            \* XXX: assume cardinality = 1
        IN
        /\ quorum
        /\ Send([type |-> value, from |-> n])
        /\ node_data' = [node_data EXCEPT ![n].state = IF acks_value = "<precommitted" THEN "c" ELSE "a",
                                          ![n].resolver_state = "done"]
                                          
resolver_crash(n) ==
    /\ node_data[n].resolver_state /= "idle"
    /\ node_data' = [node_data EXCEPT ![n].resolver_state = "idle"]
    /\ msgs' = msgs
    

\**************************************************************************************
\* Spec and type checks
\**************************************************************************************

next ==
    \/ \E n \in nodes: oc_prepare(n)
    \/ \E n \in nodes: oc_on_any_abort(n)
    \/ \E n \in nodes: oc_all_prepared(n)
    \/ \E n \in nodes: oc_maj_precommit(n)

    \/ \E n \in nodes: peer_on_prepare(n)
    \/ \E n \in nodes: peer_on_abort(n)
    \/ \E n \in nodes: peer_on_commit(n)
    \/ \E n \in nodes: peer_on_precommit(n)
    \/ \E n \in nodes: peer_on_preabort(n)
    \/ \E n \in nodes: peer_on_status(n)
    
    \/ \E n \in nodes: resolver_start(n)
    \/ \E n \in nodes: resolver_ask_status(n)
    \/ \E n \in nodes: resolver_recv_status(n)
    \/ \E n \in nodes: resolver_await_acks(n)
    
\* 457M states (57m on my laptop) with following:
\*     \/ \E n \in nodes: resolver_crash(n)

spec == init /\ [][next]_<<node_data, msgs>>

====