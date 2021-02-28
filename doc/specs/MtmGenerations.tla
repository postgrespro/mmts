---- MODULE MtmGenerations ----

\* Model of generations as proposed in generations2.md. Here prepares don't
\* happen instantly at all members, however there is obviously still quite a
\* number of simplifications:
\*  - New generation is elected (voted for) instantly. I'm sure this is
\*    ok; it is easy to make them more fair, but that would greatly increase num
\*    of states. Note, however, that nodes *don't learn* about new generation
\*    existence instantly.
\*  - There is no parallel apply from single node.
\*  - Commit protocol (i.e. Paxos) is not modeled at all, of course.
\*  - Acks of getting Prepare can't be lost. Surely this influences only liveness.
\*  - Liveness is out of scope here. We don't even pull xacts in recovery mode
\*    until gen with us is elected, though this is easy to alter.
\*  - We assume all xacts conflict: no two xacts can be in PREPARED state on the node.

\* <max_xacts, max_gen> and num of states:
\* <2, 3>  28*10^6 distinct, 1*10^8 walked states

EXTENDS Integers, Sequences, FiniteSets, TLC
VARIABLES
  state, \* state[n] is generation state, WAL (we have only prepares there) and
         \* local clog of node n
  acks, \* tx -> number of PREPARE acks
  clog,  \* tx -> TRUE (commit) | FALSE (abort): global clog (paxos emulation)
  gens   \* Sequence of all ever elected generations. Models possiblity of
         \* learning newer gen existence at any moment.


\* Note that with max_gen = 2 recovery doesn't happen at all, because 1 is init
\* and in 2 nobody needs recovery, so reasonable values > 2.
CONSTANT
  nodes,
  max_xacts, \* model constraint: max number of xacts allowed on single node.
             \* Actually, since we check this only at DoTx, total num of
             \* *committed* xacts in the system <= max_xacts; however, single node
             \* generally might have to max_xacts * NumNodes aborted ones.
  max_gen \* model constraint: max allowed generation number
ASSUME max_xacts \in Nat /\ max_gen \in Nat /\ max_gen >= 1

\* For specifying symmetry set in manual cfg file, see
\* https://github.com/tlaplus/tlaplus/issues/404
perms == Permutations(nodes)


\********************************************************************************
\* Helpers
\********************************************************************************

\* Max of two numbers
Max(a, b) == IF a > b THEN a ELSE b

\* is s1 subsequence of s2?
IsSubSeq(s1, s2) ==
    /\ Len(s1) <= Len(s2)
    /\ SubSeq(s2, 1, Len(s1)) = s1

\* Set of values of sequence
SeqValues(seq) == {seq[i]: i \in DOMAIN seq}

\* All possible sequences with values in set S and len <= n.
\* Thanks Hillel.
SeqMaxLen(S, n) ==  UNION {[1..m -> S] : m \in 0..n}

\* Sort of 0 for functions
EmptyF == [x \in {} |-> 42]
IsEmptyF(f) == DOMAIN f = {}

Maximum(S) ==
  (*************************************************************************)
  (* If S is a set of numbers, then this define Maximum(S) to be the       *)
  (* maximum of those numbers, or -1 if S is empty.                        *)
  (*************************************************************************)
  IF S = {} THEN -1
            ELSE CHOOSE n \in S : \A m \in S : n \geq m

\*****************

NumNodes == Cardinality(nodes)

Quorum(ns) == Cardinality(ns) >= (NumNodes \div 2 + 1)

\* Next record rn node fetches from sn node. is_recovery means consider
\* all origins.
\* If there is no one, returns EmptyF instead of normal record to be able to
\* distinguish.
NextPrepare(rn, sn, is_recovery) ==
    LET
      new_prepares == SelectSeq(state[sn].wal,
                                LAMBDA p: /\ p \notin SeqValues(state[rn].wal)
                                          /\ is_recovery \/ p.origin = sn
                                          \* Online nodes refuse to apply xacts from old gens.
                                          \* To avoid stucking on them, skip aborted ones.
                                          /\ (p \notin DOMAIN clog \/ clog[p]))
    IN
      IF Len(new_prepares) = 0 THEN EmptyF ELSE Head(new_prepares)


\* status of the node
Status(n) ==
    LET
      s == state[n]
    IN
      \* I am member of this gen and I am recovered in it
      IF s.last_online_in = s.current_gen.num THEN "online"
      \* I present in current gen and I can get there online without violating
      \* the promise not be ONLINE in gens < last_vote (and > last_online_in during voting).
      ELSE IF n \in s.current_gen.members /\ s.current_gen.num = s.last_vote THEN "recovery"
      ELSE "disabled"

\* We assume all xacts conflict, so node n can apply xact iff is has no prepares.
\* Since *all* conflict, it is enough to check just the latest record.
\* CanApply(n) == \/ Len(state[n].clog) = 0 \/ state[n].clog[Len(state[n].clog)] /= "prepared"
\* Nope, to my surprise the above didn't work: TLC evals the second term and fails.
\* Let's check everything then.
CanApply(n) == \A i \in DOMAIN state[n].clog: state[n].clog[i] /= "prepared"

\********************************************************************************
\* Type assertion
\********************************************************************************
Quorums == {subset \in SUBSET nodes: Quorum(subset)}
IsXact(x) == x \in [origin: nodes, olsn: 1..max_xacts, gen_num: 1..max_gen]
IsGen(g) == g \in [num: 1..max_gen, members: Quorums, donors: SUBSET nodes]
\* Defining sets of all possible tuples and using them in TypeOk in usual
\* all-tuples constructor is not practical because such definitions force
\* TLC to enumerate them, while they are are horribly enormous
\* (TLC screams "Attempted to construct a set with too many elements"
\* with max_gen=3, max_xacts=2). So instead define operators like IsXact and
\* check types manually.
TypeOk ==
    /\ \A n \in nodes:
      /\ DOMAIN state[n] = {"next_lsn", "current_gen", "last_online_in", "last_vote", "wal", "clog"}
      /\ state[n].next_lsn \in 1..(max_xacts + 1)
      /\ IsGen(state[n].current_gen)
      /\ state[n].last_online_in \in 1..max_gen
      /\ state[n].last_vote \in 1..max_gen
      /\ \A i \in DOMAIN state[n].wal:
         /\ IsXact(state[n].wal[i])
         /\ state[n].clog[i] \in {"prepared", "committed", "aborted"}
    /\ \A xact \in DOMAIN acks: IsXact(xact) /\ acks[xact] \in 1..NumNodes
    /\ \A xact \in DOMAIN clog: IsXact(xact) /\ clog[xact] \in {TRUE, FALSE}
    /\ \A i \in DOMAIN gens: IsGen(gens[i])


\********************************************************************************
\* Initial
\********************************************************************************

\* Initially all nodes are in gen 1 containing all nodes; everyone is online there
AllOnes == [n \in nodes |-> 1]
Init ==
    /\ state = [n \in nodes |-> [
                   next_lsn |-> 1,
                   current_gen |-> [num |-> 1, members |-> nodes, donors |-> nodes],
                   last_online_in |-> 1,
                   last_vote |-> 1,
                   wal |-> << >>,
                   \* This is a sequence parallel to wal, it keeps node-local
                   \* status of xacts which is not replicated with xact itself
                   \* (paxos emulation).
                   clog |-> << >>
               ]]
    /\ acks = EmptyF
    /\ clog = EmptyF
    /\ gens = <<[num |-> 1, members |-> nodes, donors |-> nodes]>>


\********************************************************************************
\* Actions
\********************************************************************************

\* Elect new generation: majority instantly votes for it.
\* For simplicity, set of voters is the same as set of members. This is not
\* necessary, but just simpler and in the implementation we would do the same.
ElectNewGen ==
    \* 1..max_gen should be actually Nat in the unbouned model, but TLC won't
    \* swallow it
    \E num \in 1..max_gen, members \in SUBSET nodes:
      /\ num <= max_gen
      /\ Quorum(members)
      /\ \A n \in members: state[n].last_vote < num
      /\ state' = [n \in nodes |->
                    IF n \in members THEN
                      [state[n] EXCEPT !.last_vote = num]
                    ELSE
                      state[n]
                  ]
      /\ LET \* determine donors
           max_last_online_in == Maximum({state[n].last_online_in: n \in members})
           donors == {n \in nodes: n \in members /\ state[n].last_online_in = max_last_online_in}
         IN
           gens' = Append(gens, [num |-> num, members |-> members, donors |-> donors])
      /\ UNCHANGED <<acks, clog>>

\* Node n learns about new generation existence.
\* (ConsiderGenSwitch in generations2.md)
LearnNewGen(n) ==
    \E i \in DOMAIN gens:
      /\ state[n].current_gen.num < gens[i].num
      /\ state' = [state EXCEPT ![n].current_gen = gens[i],
                                ![n].last_vote = Max(gens[i].num, state[n].last_vote),
                                \* Get ONLINE immediately if we are donor in this gen
                                \* and if we can't do that without breaking the promise
                                ![n].last_online_in =
                                  IF /\ n \in gens[i].members
                                     /\ n \in gens[i].donors
                                     /\ state[n].last_vote <= gens[i].num THEN
                                    gens[i].num
                                  ELSE
                                    state[n].last_online_in]
      /\ UNCHANGED <<acks, clog, gens>>

\* Receiver on node rn in RECOVERY sees P of its gen is going to be applied
\* (from sn node) next. It means it has recovered in this gen and can
\* (actually, *must* before applying it or it would miss answer to coordinator)
\* switch to ONLINE. In practice that would be normally done via separate
\* ParallelSafe message to ensure convergence when there is no new xacts. However,
\* modeling ParallelSafe doesn't add anything significant; moreover, we should
\* anyway have this logic due to potential possibility of pulling such xact from
\* non-donor node, see generations2.md.
GetOnline(rn, sn) ==
    /\ rn /= sn
    /\ Status(rn) = "recovery"
    \* Technically, absense of this wouldn't harm safety: pulling in recovery from
    \* anyone is ok.
    /\ sn \in state[rn].current_gen.donors
    /\ LET
         next_p == NextPrepare(rn, sn, TRUE)
       IN
         /\ ~IsEmptyF(next_p)
         /\ next_p.gen_num = state[rn].current_gen.num
    /\ state' = [state EXCEPT ![rn].last_online_in = state[rn].current_gen.num]
    /\ UNCHANGED <<acks, clog, gens>>

\* Apply next prepare in recovery. Note that both PullRecovery and PullNormally
\* ignore xacts from higher gens; we must LearnNewGen before applying them.
PullRecovery(rn, sn) ==
    /\ rn /= sn
    /\ Status(rn) = "recovery"
    \* Technically, absense of this wouldn't harm safety: pulling in recovery from
    \* anyone is ok.
    /\ sn \in state[rn].current_gen.donors
    /\ CanApply(rn)
    /\ LET
         next_p == NextPrepare(rn, sn, TRUE)
       IN
         /\ ~IsEmptyF(next_p)
         \* Comparison is strict because we must switch to ONLINE to ack xact from
         \* our gen before eating it; c.f. GetOnline.
         /\ next_p.gen_num < state[rn].current_gen.num
         /\ state' = [state EXCEPT ![rn].wal = Append(state[rn].wal, next_p),
                                   ![rn].clog = Append(state[rn].clog, "prepared")]
    /\ UNCHANGED <<acks, clog, gens>>

\* Apply next prepare in normal mode.
PullNormal(rn, sn) ==
    /\ rn /= sn
    /\ Status(rn) = "online"
    /\ CanApply(rn)
    /\ LET
         next_p == NextPrepare(rn, sn, FALSE)
       IN
         /\ ~IsEmptyF(next_p)
         /\ next_p.gen_num = state[rn].current_gen.num
         /\ state' = [state EXCEPT ![rn].wal = Append(state[rn].wal, next_p),
                                   ![rn].clog = Append(state[rn].clog, "prepared")]
         /\ acks' = [acks EXCEPT ![next_p] = acks[next_p] + 1]
    /\ UNCHANGED <<clog, gens>>


\* Write new xact, stamping it with current gen. Bumps it to local WAL only.
DoPrepare(n) ==
    /\ Len(state[n].wal) < max_xacts
    /\ Status(n) = "online"
    /\ CanApply(n)
    /\ LET
         prepare == [origin |-> n, olsn |-> state[n].next_lsn, gen_num |-> state[n].current_gen.num]
       IN
         /\ state' = [state EXCEPT ![n].wal = Append(state[n].wal, prepare),
                                   ![n].clog = Append(state[n].clog, "prepared"),
                                   ![n].next_lsn = state[n].next_lsn + 1]
         \* Could use @@ from TLC here, but dunno about efficiency.
         \* Real key doesn't include gen_num, but keeping it is simpler.
         /\ acks' = [xact \in DOMAIN acks \cup {prepare} |->
                       IF xact = prepare THEN 1 ELSE acks[xact]]
    /\ UNCHANGED <<clog, gens>>

\* Atomically abort some tx on node n. Node itself (and others) will learn
\* about it later in LearnFinish.
\* Well, actually in this and two following actions we could just check
\* the last xact since all conflict as in CanApply.
AbortTx(n) ==
    /\ \E i \in DOMAIN state[n].wal:
         LET
           prepare == state[n].wal[i]
         IN
           /\ prepare \notin DOMAIN clog \* it must not be finished yet
           \* push abort
           /\ clog' = [xact \in DOMAIN clog \cup {prepare} |->
                         IF xact = prepare THEN FALSE ELSE clog[xact]]
    /\ UNCHANGED <<state, acks, gens>>

\* Atomically commit some tx on node n. Node itself (and others) will learn
\* about it later in LearnFinish.
\* We commit transaction only if node is still in xact's gen for two reasons:
\*  - Though committing xact once acks from all gen members are gathered is
\*      safe, in practice after gen switch there is a risk never to get response
\*      for it even with live TCP connections as members might get it in recovery;
\*      so it is simpler to abort all backend waiting for prepare acks immediately
\*      on gen switch;
\*  - For simplicity in the model we don't carry gen members in the prepares at
\*    all, so we can compare num of acks only with current gen.
CommitTx(n) ==
    /\ \E i \in DOMAIN state[n].wal:
         LET
           prepare == state[n].wal[i]
         IN
           /\ prepare \notin DOMAIN clog \* it must not be finished yet
           /\ prepare.gen_num = state[n].current_gen.num
           /\ acks[prepare] = Cardinality(state[n].current_gen.members)
           \* push commit
           /\ clog' = [xact \in DOMAIN clog \cup {prepare} |->
                         IF xact = prepare THEN TRUE ELSE clog[xact]]
    /\ UNCHANGED <<state, acks, gens>>

\* Learn that some prepared xact on n was committed/aborted.
LearnFinish(n) ==
    /\ \E i \in DOMAIN state[n].wal:
         LET
           prepare == state[n].wal[i]
         IN
           /\ state[n].clog[i] = "prepared"
           /\ prepare \in DOMAIN clog \* it must be finished
           /\ state' = [state EXCEPT ![n].clog[i] = IF clog[prepare] THEN "committed" ELSE "aborted"]
    /\ UNCHANGED <<acks, clog, gens>>


\*******************************************************************************
\* Final spec
\*******************************************************************************

Next ==
  \/ ElectNewGen
  \/ \E n \in nodes: LearnNewGen(n)
  \/ \E rn, sn \in nodes: GetOnline(rn, sn)
  \/ \E rn, sn \in nodes: PullRecovery(rn, sn)
  \/ \E rn, sn \in nodes: PullNormal(rn, sn)
  \/ \E n \in nodes: DoPrepare(n)
  \/ \E n \in nodes: AbortTx(n)
  \/ \E n \in nodes: CommitTx(n)
  \/ \E n \in nodes: LearnFinish(n)

Spec == Init /\ [][Next]_<<state, acks, clog, gens>>


\********************************************************************************
\* Invariants
\********************************************************************************

\* Basic assertions
LastVoteGECurrentGen == \A n \in nodes: state[n].last_vote >= state[n].current_gen.num
LocalClogMirrorsWal == \A n \in nodes: Len(state[n].wal) = Len(state[n].clog)
FinishedXactsInClog == \A n \in nodes: \A i \in DOMAIN state[n].wal: \/ state[n].clog[i] = "prepared"
                                                                     \/ state[n].wal[i] \in DOMAIN clog

\* Important properties

EachGenHasDonor == \A i \in gens: Cardinality(gens[i].donors) >= 1

\* Make sure every log is sublog of the longest one, ignoring prepared/aborted
\* records. We check global clog here -- it is even more strict check than local one
\* and it is simpler to write.
OrderOk ==
  LET
      filtered_logs == [n \in nodes |-> SelectSeq(state[n].wal,
                                                  LAMBDA xact: xact \in DOMAIN clog /\ clog[xact])]
      most_advanced_node == CHOOSE n1 \in nodes: \A n2 \in nodes:
                              Len(filtered_logs[n1]) >= Len(filtered_logs[n2])
  IN
      \A n \in nodes: IsSubSeq(filtered_logs[n], filtered_logs[most_advanced_node])


====