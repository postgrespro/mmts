# Atomic commit

For performance reasons we allow receiver workers to reorder transactions, so some transactions may fail to be applied due to the conflict with local transactions [1] or due to a global deadlock. So to maintain atomicity we first need to ensure that all parties are certain that they can commit given transaction. Such promise should also be persistent to survive node reboot. Or in other words, we need a two-phase commit (`2PC`) protocol. Luckily Postgres have such functionality built-in in form of `PREPARE TRANSACTION`/`COMMIT PREPARED`/`ABORT PREPARED` statements.

Also, we want our cluster to survive the failure of some nodes, so we need to decide to commit or abort the transaction when some participants are absent. Such a property of commit protocol called non-blocking property. Unfortunately, the two-phase commit is blocking in that sense. In the simplest example imagine that we have three nodes in states (committed, prepared, prepared) and first node crashes. So two survived nodes have only (prepared, prepared) states and can neither commit nor abort since the first one can be committed or aborted. If we state that transaction coordinator is one of the nodes and will prepare and commit transaction on itself strictly before doing that on other nodes, then it may seem as we can devise non-blocking recovery rules for `2PC` for 3 nodes: in case when we see two prepared nodes and coordinator is among them we should abort as the third node may be in aborted or prepared state; when we see two prepared nodes and coordinator is not among them we should commit as coordinator definitely prepared this transaction and possibly committed it. However such rules contravene with non-recovery protocol: if we prepared a transaction everywhere including coordinator we want to commit that transaction, not abort it. So if the original coordinator is working simultaneously with the transaction recovery process on another node they may reach a different conclusion. Also in case of 5 or more nodes blocking still be a problem if the coordinator dies along with one other node -- there is just not enough information on alive nodes to commit or abort the transaction.

To address the problem of blocking in presence of failures Skeen developed [Ske82] quorum-based three-phase commit (`Q3PC` or just `3PC`) and also proved that no protocol with the single committable state (e.g. `2PC`) can be non-blocking. But that algorithm can still block a quorum in the case when several recovery processes were coexistent or in the case when failures cascade [Kei95]. Keidar and Dolev later developed an `E3PC` algorithm [Kei95] that has the same message flow in the non-faulty case as in 3PC, but always allows quorum of nodes to proceed if there were no failures for a sufficiently long period. However it is actually not easy to derive algorithm implementation out of [Kei95] for two reasons: at first paper state the new coordinator should be elected but without discussing how to do it; at second paper mostly discuss so-called Strong Atomic Commit (if all sites voted Prepare and there were no failures, then decision should be committed) that can be solved only with perfect failure detectors. In the end, authors discuss that perfect failure detector is not practical and that given algorithm should also solve Weak Atomic Commit (if all sites voted Prepare and there were no suspicion about failures, then a decision should be to commit) but without discussing how that change affects coordinator election and recovery protocol restart. Luckily `E3PC` employs the same protocol for reaching consensus on a single value as in Viewstamped replication [Lis] and single decree Paxos (also known as Synod) [Lam01] that were created a few years before and had full description without referring to external election algorithm, and unclear parts of `E3PC` can be taken out of Paxos.

So taking into account all aforementioned statements it looks that it is easier to start a discussion of our commit protocol by looking at single decree Paxos for any value without referring to commit problem at all, and then specialize it for commit.

## Single decree Paxos

Single decree Paxos allow for a group of processes to reach a decision for some value and then never change it. The protocol itself is formulated in terms of three types of processes: `proposers`, `acceptors`, and `learners`. That separation exits mostly for explanatory purposes to bring some modularity to the protocol, but in the practical system, it is totally okay for each node to colocate all roles. The protocol starts when the client connects to `proposer` and gives a value to propose, then the following procedure happens (citing [Lam01]):
```
Phase 1.
    (a) A proposer selects a proposal number n and sends a prepare request with number n to a majority of acceptors.
    (b) If an acceptor receives a prepare request with number n greater than that of any prepare request to which it has already responded, then it responds to the request with a promise not to accept any more proposals numbered less than n and with the highest-numbered proposal (if any) that it has accepted.

Phase 2.
    (a) If the proposer receives a response to its prepare requests (numbered n) from a majority of acceptors, then it sends an accept request to each of those acceptors for a proposal numbered n with a value v, where v is the value of the highest-numbered proposal among the responses, or is any value if the responses reported no proposals.
    (b) If an acceptor receives an accept request for a proposal numbered n, it accepts the proposal unless it has already responded to a prepare request having a number greater than n.
```

The same procedure in pseudocode ([6.824]):

```
        --- Paxos Proposer ---

     1	proposer(v):
     2    while not decided:
     2	    choose n, unique and higher than any n seen so far
     3	    send prepare(n) to all servers including self
     4	    if prepare_ok(n, na, va) from majority:
     5	      v' = va with highest na; choose own v otherwise
     6	      send accept(n, v') to all
     7	      if accept_ok(n) from majority:
     8	        send decided(v') to all

        --- Paxos Acceptor ---

     9	acceptor state on each node (persistent):
    10	 np     --- highest prepare seen
    11	 na, va --- highest accept seen

    12	acceptor's prepare(n) handler:
    13	 if n > np
    14	   np = n
    15	   reply prepare_ok(n, na, va)
    16   else
    17     reply prepare_reject

    18	acceptor's accept(n, v) handler:
    19	 if n >= np
    20	   np = n
    21	   na = n
    22	   va = v
    23	   reply accept_ok(n)
    24   else
    25     reply accept_reject
```

## `E3PC`

Having reviewed single decree Paxos it is now straightforward to construct commit protocol: await responses for all prepares, calculate logical conjunction of results and run a consensus round on an obtained value. We also can do the following adjustments to the protocol:

* In a failure-free case, we may skip phase 1 completely by initializing `np` on all acceptor to some predefined constant and requiring that all proposers should use strictly bigger proposal numbers. Or putting the same in other words information flow from phase 1 for initial proposer can be done not by the means of network communications at runtime, but by a programmer at development time.
* Only safety requirement for choosing proposal numbers for acceptors is that they should be unique among the set of proposers. Usually this is done by generating numbers of form $n = n_nodes * local_count + node_id$, however we follow ``E3PC`` and choose `n` to to be tuples of `< local_count, node_id >` and compare such `n` lexicographically. It looks that such proposal numbers will be more informative in cases when things went south.
* When proposer hears phase1b message from the majority of nodes it should choose a value with maximal acceptance number among phase1b responses. Taking into account that we are agreeing on a boolean variable (with values being 'precommit' of 'preabort') we may follow ``E3PC`` and choose a value by the following procedure, where statuses is a set of phase1b responses from all nodes including self:
```math
ma = max(\{msg.na : msg \in responses\}) \\
is\_max\_attempt\_commitable = \forall msg \in responses: (msg.la = ma) => msg.state = "precommit"
```
(_XXX is that actually possible to have different accepted values if proposal numbers were unique?_)

So we can assemble the following algorithm for Postgres:


```python
#
#   Each node has following global state
#
self.n_nodes
self.generate_new_term = False
self.majority = self.n_nodes // 2 + 1
self.global_txs = {} # Dict[gid:str -> GTX]

#
#   Each self.global_txs member has following fields
#
@dataclass
class GTX:
    acquired_by: int
    gid: str
	status: string
    proposal_term: Tuple[int, int]
    accepted_term: Tuple[int, int]
    # node_id -> StatusResp[gid, node_id, status, proposal_term, accepted_term]
    remote_statuses: Dict[int, StatusResp]
    # node_id -> AckResp[gid, node_id, status]
    resolver_acks: Dict[int, AckResp]


###############################################################################
#
#   Helper functions for concurrent work with gtxes
#
###############################################################################

def create_gtx(gid):
    pg.LWLockAcquire(pg.GtxLock, LW_EXCLUSIVE)
    gtx = GTX()
    gtx.gid = gid
    gtx.proposal_term = (1,0)
    gtx.accepted_term = (0,0)
    gtx.acquired_by = pg.MyProcId
    self.global_txs[gid] = gtx
    pg.LWLockRelease(pg.GtxLock)
    return gtx

def aquire_gtx(gid, locked=False):
    if not locked:
        pg.LWLockAcquire(pg.GtxLock, LW_EXCLUSIVE)

    while self.global_txs[gid].acquired_by != 0:
        pg.LWLockRelease(pg.GtxLock)
        time.sleep(0.1)
        pg.LWLockAcquire(pg.GtxLock, LW_EXCLUSIVE)

    gtx = self.global_txs[gid]
    pg.LWLockRelease(pg.GtxLock)
    return gtx

# XXX: if we load non-final status from disk is it an error since we
# already should do that in recovery, or no?
def gtx_acquire_or_load(gid)
    pg.LWLockAcquire(pg.GtxLock, LW_EXCLUSIVE)
    if gid in self.global_txs:
        return aquire_gtx(gid)
    else:
        gtx = pg.WalReaderLoadStatus(gid)
        self.global_txs[gid] = gtx
        pg.LWLockRelease(pg.GtxLock)
        return gtx

def release_gtx(gtx)
    pg.LWLockAcquire(pg.GtxLock, LW_EXCLUSIVE)
    gtx = self.global_txs[gid]
    gtx.acquired_by = 0
    if gtx.status == "commited" or gtx.status == "aborted":
        del self.global_txs[gid]
    pg.LWLockRelease(pg.GtxLock)

#
#   Called from mtm-launcher during boot
#
def load_gtx_on_start(gtx)
    pg.LWLockAcquire(pg.GtxLock, LW_EXCLUSIVE)
    for pgtx in pg.GetPreparedTransactions:
        self.global_txs[pgtx.gid] = GTX(parse(pgtx.3pc_state))
    pg.LWLockRelease(pg.GtxLock)


def local_last_term(self):
    last_term = (1,0)
    for gid, gtx in global_txs.items():
        last_term = max(last_term, gtx.proposal_term)
    return last_term


###############################################################################
#
#   Algorithm itself:
#       backend_commit -- tx backend, original coordinator
#       apply_commit -- walreceiver handler of transaction finish
#       resolver -- resolver worker
#       status -- status worker
#
###############################################################################


#
#   backend_commit is called when a client start transaction commit and
#   changes ordinary commit to our protocol.
#
def backend_commit(self, gid):

    gtx = create_gtx(gid)
    pg.PrepareTransactionBlock(gid)
    gtx.status = "prepared"
    release_gtx(gtx)

    # Gather from all alive nodes.
    votes = dmq_gather('all')

    if 'aborted' in votes:
        gtx = aquire_gtx(gid)
        pg.FinishPreparedTransaction(gid, false)
        delete_gtx(gtx)
        raise "Aborted on peer node"

    # If some node disconnects during gather error will not be raised unless
    # we lost majority. So after we need to check that n_votes == n_alive_nodes.
    if len(votes) != self.n_nodes:
        raise "Commit sequence interrupted"

    gtx = aquire_gtx(gid)
    if gtx.proposal_term != (1, 0):
        release(gtx)
        raise "Commit sequence interrupted"
    gtx.accepted_term = (1, 0)
    gtx.status = "precommit"
    pg.SetPreparedTransactionState(gid, "precommit", gtx)
    release(gtx)

    acks = dmq_gather('majority')
    gtx = aquire_gtx(gid)
    if all([ack.proposal_term == (1, 0) for ack in acks]):
        pg.FinishPreparedTransaction(gid, true)
        delete_gtx(gtx)
    else:
        release(gtx)
        raise "Commit sequence interrupted"
        # XXX: is there any error return code that pg driver will not interpret as an abort?

#
#   apply_commit is a walreceiver worker function that is called upon
#   receiving transaction finish record.
#
def apply_commit(self, record):

    if record.type = "prepare":
        gtx = create_gtx(record.gid)
        status = pg.PrepareTransactionBlock(record.gid)
        release_gtx(gtx)
        dmq_push(record.sender, record.gid, status)

    elif record.type = "commit" or record.type = "abort":
        gtx = aquire_gtx(record.gid, missing_ok=True)
        pg.FinishPreparedTransaction(gid, record.type == "commit")
        delete_gtx(gtx)

    elif record.type = "precommit" or record.type = "preabort":
        gtx = aquire_gtx(record.gid)
        if gtx.proposal_term != record.term:
            gtx.resolver_state = "idle" # XXX
        gtx.proposal_term = record.term
        gtx.accepted_term = record.term
        gtx.status = record.type
        pg.SetPreparedTransactionState(gid, record.type, gtx)
        release_gtx(gtx)
        dmq_push(record.sender, record.gid, "ack")

    else:
        assert(False)


#
#   resolver is a bgworker that is signalled to wakeup on node disconnect
#   and before recovery.
#
def resolver(self, tx_to_resolve):

    while True:

        # any arrivng message will wake us form this
        pg.WaitLatch(MyLatch)

        # XXX: resolver probably should also periodically check if there
        # any:
        # * transactions from this node that aren't belonging to
        #   any active backend
        # * transactions from disconnected nodes
        # and set self.generate_new_term to True if needed.
        #
        # It looks like that is more robust than fighting races between
        # node disconnect and resolver start.

        if self.generate_new_term:
            dmq_scatter("get_last_term")
            max_term = max(dmq_gather())
            max_term = max(max_term, local_last_term())
            new_term = (max_term[0] + 1, node_id)

            for gid in tx_to_resolve:
                gtx = gtx_acquire(gid)
                gtx.proposal_term = new_term
                # that probably can be optimized to write only once to WAL for
                # all transactions, but at cost of complicating recovery
                pg.FinishPreparedTransaction(gid, gtx.status, gtx)
                gtx_release(gtx)

            for gid, gtx in global_txs.items():
                dmq_scatter("get_status", gid)

            self.generate_new_term = False


        response = dmq_pop()

        gtx = gtx_acquire(response.gid)
        if not gtx.awaiting_acks:

            if response.status == "commit" or response.status == "abort":
                pg.FinishPreparedTransaction(gid, response.status == "commit")

            else:
                if response.proposal_term = gtx.proposal_term:
                    gtx.remote_statuses[response.node_id] = response
                    max_attempt = max([r.accepted_term for r in gtx.statuses] + [gtx.accepted_term])
                    quorum = (1 + len(gtx.remote_statuses)) >= self.majority

                    max_attempt_statuses = [r.status for r in gtx.remote_statuses if r.accepted_term == max_attempt]
                    max_attempt_statuses += [gtx.status] if gtx.accepted_term == max_attempt] else {}
                    imac = set(max_attempt_statuses) == {'pc'}

                    if quorum:
                        decision = "precommit" if imac else "preabort"
                        gtx.accepted_term = record.term
                        gtx.awaiting_acks = True
                        pg.SetPreparedTransactionState(gid, decision, gtx)

        else:
            if response.proposal_term = gtx.proposal_term:
                gtx.resolver_acks[response.node_id] = response
                n_acks = len(gtx.resolver_acks)
                n_acks += 1 if gtx.proposal_term == response.proposal_term] else 0
                if n_acks > self.majority:
                    pg.FinishPreparedTransaction(gid, gtx.status == "precommit")

        gtx_release(gtx)


#
#   Status is a bgworker listening on "status" dmq channel and sending
#   responses to two kind of messages: `get_last_term` and `get_status`.
#
def status(self):
    request = dmq_pop()

    if request.type = "get_last_term":
        dmq_push(request.sender, "get_last_term", local_last_term())

    elif request.type = "get_status":
        gtx = gtx_acquire_or_load(gid)
        if request.term > gtx.proposal_term:
            resp = (gid, gtx.proposal_term, gtx.accepted_term, gtx.status)
            dmq_push(request.sender, "get_status", resp)
        gtx_release(gid)
```


## Model

More or less same algorithm is implemented in [commit.tla] and can be checked in tla model checker.



Notes:

[1] Now we do not abort in this case but just wait on lock. However if that local transaction will be successfully prepared then it will definetly catch a global deadlock with tx waiting for it. So it may be a good idea to abort one of them earlier -- probably a later one, since it didn't yet used resources of walsender/walreceivers and that will be cheaper for whole system.

Bibliography:

[Ske82] D. Skeen. A Quorum Based Commit Protocol. Berkeley Workshopon Distributed Data Management and Computer Networks,(6):69-80, February 1982

[Kei95] I. Keidar, D. Dolev. Increasing the resilience of atomic commit, at no additional cost. Proc. of the 14th ACM PoDs, pages 245-254, May 1995.

[Lam01] L. Lamport. Paxos made simple. ACM SIGACT News (Distributed Computing Column), 2001.

[6.824] http://nil.csail.mit.edu/6.824/2015/notes/paxos-code.html
