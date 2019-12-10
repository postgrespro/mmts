# Generations

This file and TLA+ specs in this directory describe the problem of reordering
xacts in mtm and propose a way to deal with it. The file contents:
 - Problem description;
 - Algorithm based on centralized election live nodes sets, each stamped
   with logical clock value (generations);
 - Discussion of alternatives;

The specs/models are:
 - MtmPrimitiveCurrent.tla is primitive (immediate prepare everywhere) but
   close model of current (as of this writing) implementation.
 - MtmPrimitiveCurrentMasks.tla is naive attempt to fix it by stamping prepares
   with set of enabled nodes and refusing to accept prepare if its enabled mask
   doesn't match acceptor one.
 - MtmPrimitiveCurrentMasksFixed.tla is sort of fixed version of previous one:
   node always recovers from node it asks to enable it in. It works, but seems
   suboptimal, see "alternatives".
 - MtmGenerations.tla is model of generations as described below.

All specs have top comment explaining them and *.cfg file with the model.

## Basics:

 - We use ACP (atomic commit protocol -- e3pc, paxos, whatever) to ensure that
   each xact either committed or aborted everywhere.
 - However, if minority fails, we want to continue working. Thus we are
   cheating: unlike in traditional ACP, we want to be able to commit xact
   without asking some nodes' (minority, to be able to progress when majority is live)
   opinion at all. We follow ACP in everything apart
   from this detail, so the consensus on xact status will definitely be
   preserved: commit or abort everywhere. But this cheat introduces problem of
   ordering: if we commit xact without minority's opinion, we must
   prove (and arrange things) so that all nodes still get all conflicting xacts
   which are going to be committed in the same order everywhere -- otherwise they
   might fail to apply them, or data will silently diverge.

How to ensure that? Starting idea: let's require that any xact must be accepted
by *majority* before getting committed. This gives us the following: at any
moment, if we have live majority (and therefore we must progress), for each xact
which was or will be committed there is at least one live node who has this
xact. So, we can (and must) ensure new xacts appear only after obtaining all old
ones who might be committed, to avoid reordering.

As an illustration of the problem we are dealing with, consider the following
sausage issue:
 - A recovers from B, B and C work normally, no link between A and C.
 - A reaches p.s. point, henceforth it applies only B's own xacts from stream to
   B.
 - C commits some 'update x' on C and B.
 - B commits 'update x' on A, B, C.
 - Later link between A and C appears and A pulls first update from C,
   overwriting later one.

Or consider another question. Say, nodes BC are working normally, and at some
point restored node A joins the cluster. C doesn't need recovery after this
configuration change; it was definitely recovered in BC. So it seems like it can
just connect to A in 'normal' mode (parallel apply of only A's xacts). However,
while this request 'stream to me in normal mode' travels to A, many things could
change, in particular A and B could throw C off the cluster. We need some
causal relationship between these events to make sure apply is safe.


## The algorithm for safety

The goal is to avoid reordering of conflicting xacts. We don't want to always
wait for all nodes PREPARE confirmation before committing; however, dealing with
any node waiting for whoever it wants is... cumbersome. Let's then instead
invent generations. Generation is <generation_number, list of members who must
at be at least majority>.  Generation numbers are unique; they act as logical
clocks. Each transaction is assigned generation number it belongs to
(current generation of the coordinator). The idea is that xact can't be
committed unless all its generation members received PREPARE. Once node
switched into some generation and, if needed, recovered (more on this below),
it accepts (and creates) only xacts belonging to this generation;
there is a barrier between writing down PREPARES of old and new generation.

We don't need raft/paxos or something for choosing generations, because there is
no trade on single value: we don't have problem 'we already voted and can't do
it again' -- in case of voting problems we just restart voting with greater
number. We should make it similar to how master is elected in raft. Anyone can
at any time offer to vote for any next generation, but for better liveness
(avoid flip-flopping and choosing dead nodes) it makes sense to propose and vote
for cliques here. Ideally we should also ensure only reasonably recovered nodes
(lag not too high) get elected, but that's an optimization; let's focus on safety
for now.

Node which is a member of some generation n considers itself *online* or in
recovery in it. If it is online, it means node must have in its WAL all PREPAREs
of all gens < n who might ever get committed; no new committable prepares of
those old generations are possible. Only online node can participate in voting
for xacts of its generation. Otherwise (node not online in gen where it
is a member) node must recover and then become online. If node is not a member
of generation at all, it never participates in voting for this gen xacts. It
does nothing or (see notes on liveness) recovers in single-threadeded, all
origins mode.

Note that conflicting committable transactions (we might for simplicity here
assume all xacts are conflicting -- otherwise, their order doesn't matter) lie
in the same order on all generation members, because xact won't be committed
unless *all* generation members confirmed PREPARE receival.

General statement about generations is the following. If node is online
(recovered) in gen n, it definitely has all prepares who can be committed of all
gens < n. For each m < n, all committable prepares of gen m lie before all
committable prepares of gen m + 1; and the order of committable prepares within
one gen is the same as order of them on this gen's members.

The algorithm itself it described below. Here let's show by induction that this
statment is true. (here and below, comparing generations means comparing
their numbers)

Initially all nodes live in gen < 1, all nodes> and everyone is online there.
For n <= 1 statement is trivially true: there are no xacts of gens < 1.

Now, let this statement be true for all m < n. We need to prove it is true for
n. Node gets online in generation n in two cases:
 1) It is donor of generation n.
 2) It recovers in single-threaded mode from donor of generation n until
   ParallelSafe<n>.

First, why donors of generation n have all xacts of gens < n in correct order?
Appearance of generation n with some donor A means the following has happened:
 - Majority of nodes had voted for gen n. With each vote, they had sent their
   current last_online_in (last generation in which they participated online,
   i.e. created and applied prepares) and had promised that they will never be
   enabled in gens m: last_online_in < m < n.
 - Let A's last_online_in during voting was p. p < n because
   last_online_in can't be greater than current_gen, which in turn can't be
   greater last_vote, and we never vote if last_vote >= than proposed gen num.
   Which, by assumption, means that A has all committable prepares of gens < p.
 - By construction, p >= than collected last_online_in of all voters. Which means
   all voters never were and never will be enabled in generations m: p < m < n
   by their promises. 'All voters' is majority, and any generation
   contains at least majority members, so they have to intersect; thus,
   committable transactions of gens m: p < m < n don't exist as at least one
   member of each such gen would never be online in it -- he will never accept
   PREPARE.
 - As for the generation p itself, once A becomes aware that gen n was chosen, it
   switches into it and stops applying PREPAREs of gen p, which means at this
   moment it has all commitable xacts of gen p (no xacts can commit without A's
   consent). To summarize, immediately after
   switching,
    - A has all committable prepares of gens < p in right order by assumption.
	- A has all committable prepares of gen p because he is a participant which
	  stopped applying. All of them lie after prepares of gens < p because
	  A won't apply/create prepares of gen p until becoming online in p, at
	  which point all < p are already there by assumption.
    - Committable prepares of gens m: p < m < n don't exist.

   So, A has all committable prepares of all gens < n in right order.

Why some other non-donor nodes, e.g. B, will have all xacts < n in right order
once it will be enabled? Well, B switches to n only if its current_gen < n.
Which means its q = last_online_in also < n, as discussed above. By assumption,
B has all xacts < q in right order. Order of q's xacts is also fine because B
was member of q; they follow xacts < q, because B start accepting/writing q
prepares only after enabling itself. In gens m: q < m < n node B never was
enabled, otherwise last_online_in would be updated. This means it never got any
such xact in normal mode. It will get them only in gen n in recovery mode
(single-threaded, all origins) from gen n's donor which, as shown above, has all
of them and in correct order, enabling itself only afterwards (on ParallelSafe).


Some data structures:

```c

struct Generation {
  int64 num; /* generation number */
  nodemask_t members; /* generation members */
}

/*
 * Node status in current generation.
 * This status is just a convenience for readablity and less typing; it can
 * always be computed from genstate->{current_gen, last_online_in, last_vote}
 */
enum StatusInGen {
  /* can never be online in this gen */
  DISABLED,

  /* The rest of states are possible only if node is member of the gen */
  /*
   * We are in the process of recovery in this gen, genstate->donors shows from
   * whom we should recover.
   * XXX: this is like 'RECOVERED' in current mtm. ISTM better to name these 3
   * states CATCHING_UP (we are just decreasing the lag, no one waits for us
   * yet), RECOVERY -- others already wait for us, we should quickly recover to
   * ParallelSafe and switch to the third state, ONLINE.
   */
  RECOVERY,
  /*
   * We have recovered in this gen, if that was needed, and work normally.
   */
  ONLINE,
}

/* generation state in shmem mostly about me, protected by GenLock */
struct GenState {
  /*
   * My current generation. Persists to disk on update; must never go backwards.
   */
  Generation current_gen;
  /*
   * subset of 'current_gen.members' which definitely has all xacts of gens < num; always
   * has at least one node. From these nodes we can recover to participate in
   * this gen, persisted along with current_gen.
   */
  nodemask_t donors;

  StatusInGen status; /* status in this generation */
  /*
   * Last generation I was online in. Must be persisted to disk before
   * setting ONLINE in the generation; used for determining donors.
   */
  Generation last_online_in;

  /*
   * Oldest gen for which we have voted. Persisted on update.
   * Used for not voting twice and to keep the promise 'once we voted for n,
   * never become online in any gen < n', which allows to learn who are donors
   * during the voting.
   * Keeping this promise requires voting and switching gen to take the same
   * lock.
   */
  Generation last_vote;
}
```

### The voting procedure:

In addition to structures above, when conducting voting,
```c
struct Vote {
  NodeId voter;
  Generation last_online_in;
}
struct Campaign {
  Generation proposed_gen;
  Vote []collected_votes; /* register received votes here */
} my_campaign;
```
is also kept in shmem.

Initially we set first generation <1, all nodes>, in which everyone is recovered
(last_online_in = 1).
 - Whenever node decides to change generation (i.e. wants to join the cluster), it
     - decides who should be new_members
     - LWLockAcquire(GenLock, LW_EXCLUSIVE);
     - sets my_campaign.proposed_gen.members = new_members and last_vote.members = new_members
     - sets my_campaign.proposed_gen.num = ++last_vote.num;
     - fsyncs last_vote;
     - adds its own vote to my_campaign.collected_votes;
      -LWLockRelease(GenLock);
     - and broadcasts RequestVote<my_campaign.proposed_gen>
 - On receival of RequestVote<proposed_gen> by recepient, under GenLock:
     if proposed_gen == last_vote
       respond VoteOk(proposed_gen.num, genstate->last_online_in)
       (already voted for these members in this gen; useful if several nodes try
        to conduct elections with the same number and members, which is probably likely)
     else if proposed_gen.num <= last_vote.num
       respond VoteGenNumTooLow<last_vote.num>
         (can't vote, retry voting with higher gen num)
     else, if proposed_members makes sense (i.e. clique is ok), vote under GenLock:
       - increment && fsync last_vote; if (genstate->status == RECOVERY) { genstate->status = DISABLED }
         respond VoteOk<proposed_gen.num, genstate->last_online_in>
 - Processing of messages above by elections initiator:
     On VoteGenNumTooLow, restart elections with number at least
       received last_vote.num + 1 (local last_vote.num adjusted accordingly)

     On VoteOk, remember the vote in collected_votes if we are still conducting
     elections with this num. If majority is collected, vote is successfull,
     calculate donors which are online members of last gen among last_online_in in votes:

     ```c
     {
       Generation latest_gen = { .num = 0 }
       foreach v in my_campaign->collected_votes {
         if v.last_online_in.num > latest_gen {
           latest_gen = v.last_online_in
		   donors = [ v.voter ]
		 } else if v.last_online_in.num == latest_gen.num {
		   donors += v.voter
		 }
       }
     }
     ```
     execute ConsiderGenSwitch(my_campaign->proposed_gen, donors) and broadcast
     CurrentGenIs<current_gen, donors>
 - On CurrentGenIs<gen, donors> receival, ConsiderGenSwitch(gen, donors) is always executed.
 - At any time vote initiator may restart elections if it finds that reasonable:
   e.g. after failing to send/receieve any of messages above to/from
   proposed_members.


### Generation switching procedure
executed whenever node learned about existence
of generation higher than its current (CurrentGenIs, START_REPLICATION
command, PREPARE, parallel safe arrived, PREPARE replies):

```c
ConsiderGenSwitch(Generation gen, nodemask_t donors) {
  LWLockAcquire(GenLock, LW_EXCLUSIVE);
  if (genstate->current_gen.num >= gen.num) {
    /* our gen is already at least that old */
    LWLockRelease(GenLock);
    return;
  }

  /* voting for generation n <= m is pointless if gen m was already elected */
  if genstate->last_vote.num < gen.num
    genstate->last_vote = gen /* will be fsynced below along with rest of genstate */

  genstate->current_gen = gen;
  genstate->donors = donors;

  /* We are not member of this generation... */
  if !IsMemberOfGen(me, gen) ||
     /*
      * .. or we can't be online in it due to promise: when we voted for last_vote.num,
      * we promised that the oldest gen among gens with num <= last_vote.num in
      * which we ever can be online (and thus create xacts) is last_online_in
      * on the moment of voting, and it should stay forever. To keep that
      * promise, prevent getting ONLINE in gens with <= last_vote.num numbers.
      */
     genstate->last_vote.num > gen.num {
    /*
     * We can never create xacts in this gen; nothing much to do. Walreceivers
     * and backends will halt.
     *
     * XXX before proposing to vote for generation with us, we would like
     * minimize the recovery lag to decrease downtime of the cluster. For that,
     * we need to determine the donor and recover in exactly this state: when we
     * are in 'dead' generation where we can never be online.
     * This is a performance optimization which doesn't influence safety:
     * recovering in single-thread mode applying all origins from anyone is always
     * okay. So it is omitted from this description; here, nothing just happens
     * if we are not member of the current gen.
     */
    genstate->status = DISABLED;
    fsync genstate
    LWLockRelease(GenLock);
    return;
  }

  /*
   * Decide whether we need to recover in this generation or not.
   */
  if IsInMask(me, donors) {
    /* we were recovered in previous generation, no need to recovery */
    genstate->status = ONLINE;

    /*
     * Note that this description assumes backends/receivers hold GenStateLock in
     * shared mode during the whole PREPARE writing operation, which means once we
     * got it in excl mode, nobody will do PREPARE without being aware about gen
     * switch. Barrier between stopping applying/creation xacts from old gen and
     * starting writing new gen xacts, embodied on donors by ParallelSafe record,
     * is crucial; once any new gen PREPARE appeared in WAL, accepting old
     * one must be forbidden because old gen members might not have this new PREPARE
     * and thus can only get it later, creating reordering.
     *
     * This lwlock is not very nice, however; first, it makes Ctrl-c-ing
     * query during PREPARE writing impossible, second, it doesn't sound efficient,
     * third, unfairness of lwlocks might make taking it in excl mode impossible
     * (that can be fought with sleep hack though).
     * An alternative (already present in mtm)  is announcing
     * 'I'm preparing'/'I'm changing enabled mask' in shmem under spinlock and
     * using condvars for waking each other after changing this.
     */
     Write to WAL ParallelSafe<gen.num> message, which is a mark for those who
     will recover from us in this generation that they are recovered: all
     following xacts can't commit without approval of all new gen members,
     all committed xacts of previous generations lie before ParallelSafe.

     /*
      * Remember that we are online in this generation. This is crucial before
      * allowing xacts because it defines the donor who contains all previous
      * xacts during recovery. Another way is don't record last_online_in at
      * all, but recover from *all* gen members applying all origins, as the
      * right donor will definitely be among them; however, that seems more
      * complicated.
      */
     genstate->last_online_in = gen;
  } else {
    /* we need to recover */
    genstate->status = RECOVERY;
  }

  /*
   * Note that we fsync gen switch only here, after writing down p.s.; it
   * wouldn't be nice to switch gen without p.s. at all -- might lead to
   * infinite recovery.
   */
  fsync genstate->current_gen, genstate->last_online_in, genstate->last_vote (if updated)

  LWLockRelease(GenLock);

  /*
   * Tell backends the gen has changed; if they wait for PREPARED votes, they
   * should give up because answer might never come.
   */
  Wake (e.g. setlatch) all live backends.
}


### Backend actions:

 - During writing PREPARE to wal, lock GenLock in shared mode and
     - if genstate->status == DISABLED, bail out with 'I'm disabled (can't be online in current gen)'
     - if genstate->status == RECOVERY, bail out with 'node is in recovery'
     - else, stamp prepare with genstate->current_gen and collect *all* gen members
       PREPARED acks before committing xact.

gen members might reply WontPrepare<reason, payload>. Abort transaction
immediately whatever the reason is. However, if it is MyGenIsHigher, execute
ConsiderGenSwitch(gen from message).

On each wakeup during collecting PREPARED votes, check out current gen num; if
it has changed, abort. Aborting only on node disconnection might be not enough
because if e.g. we had BC, then sausage A-B-C, and clique convention says to us
that in this case quorum must be AB, next gen might exclude C even if C is alive
and connected to B.

### Walreceiver:

```c
enum
{
  REPLMODE_RECOVERY, /* stream all origins */
  REPLMODE_NORMAL /* stream only sender xacts */
} MtmReplicationMode;


struct MtmReceiverContext {
  NodeId sender_id;
  MtmReplicationMode repl_mode;
}

/* main loop */
/*
 * XXX it would be better to replace all those "sleep and restart" or "die" with
 * kind of subscription to genstate changes, probably via conditional vars.
 * It would make reactions immediate; also, this might save us reconnections.
 */
receiver_main() {
  ReceiverContext rcv_ctx;

reconnect:
    kill all parallel workers
    drop old connection, if any

    /* learn repl mode */
    LWLockAcquire(GenLock, LW_SHARED);
    current_gen = genstate->current_gen;
    status = genstate->status;
    donor = first donor from genstate->donors;
    LWLockRelease(GenLock);
    if status == DISABLED {
      /*
       * sleep a bit and restart; dead generation for us
       */
       sleep; goto reconnect;
    }
    if status == RECOVERY {
      if rcv_ctx.sender_id != donor {
        /*
         * sleep a bit and restart; we are in recovery, and our sender is not donor.
         */
         sleep; goto reconnect;
      }
      rcv_ctx.mode = REPLMODE_RECOVERY;
    }
    else {
      Assert(status == ONLINE)
      rcv_ctx.mode = REPLMODE_NORMAL;
    }

    connect to sender in rcv_ctx.mode

    /* record is message or full xact (prepare) for simplicity */
    while (record = new record from stream) {
      /*
       * This ensures walreceivers eventually converge. Doing this under
       * GenLock each time is expensive; I think it is better
       * to have maxnodes len array in shmem with recovery mode for each receiver,
       * which can be checked out atomically without locks, similar to current recovery_count.
       *
       * Also interlocking must ensure that walreceiver in recovery mode excludes
       * any other walreceiver to prevent applying record twice.
       */
      check whether our rcv_ctx.mode still apply, goto reconnect if not

      if record.type == ParallelSafe {
        if HandleParallelSafe(record)
          /*
           * ParallelSafe asked us to reconnect. Note that since we hadn't
           * written anything, after restart we will get this p.s. second time,
           * which is perfectly fine, it will be just skipped if already took
           * action.
           */
          goto reconnect;
      } else if record.type == PREPARE {
        if rcv_ctx.mode == REPLMODE_NORMAL {
          feed record to parallel worker, c.f. parallel_worker_main
        } else {
          /*
           * We are in recovery.
           */
           if HandlePrepare(record)
             goto reconnect;
        }
     /*
      * the rest of records is not affected by generations at all, accept them
      * always.
      */
     } else if record.type is PC, CP, AP {
       if rcv_ctx.mode == REPLMODE_NORMAL {
         feed record to parallel worker, c.f. parallel_worker_main
       } else {
         HandleCommit(record)
       }
     }
   }
}

/*
 * Returns true if we need to reconnect afterwards -- that is, p.s. took action
 * and switched us to ONLINE or we can't apply it due to wrong current conn mode.
 */
bool HandleParallelSafe(ps) {
  /* Make sure we know about this gen */
  ConsiderGenSwitch(ps.gen)
  LWLockAcquire(GenLock, LW_EXCLUSIVE);

  /*
   * Either we are not interested in this gen (we are in newer one or promised
   * not to join this one or not a member of it) or we are already online.
   */
  if (genstate->current_gen.num != ps.gen.num ||
      genstate->status != RECOVERY) {
    LWLockRelease(GenLock);
    return false;
  }
  /* IOW, that condition above was equivalent to */
   * genstate->current_gen.num != ps.gen.num ||
   * genstate->current_gen.num < last_vote.num ||
   * !IsMemberOfGen(me, genstate->current_gen) ||
   * genstate->last_online_in == genstate->current_gen.num
   */

  /*
   * Catching p.s. in normal mode and tranferring to its gen is not allowed;
   * we probably just have given out all prepares before it to parallel
   * workers without applying them. Reconnect in recovery.
   */
  if (ctx->replMode == ONLINE) {
   LWLockRelease(GenLock);
   return true;
  }

  /*
   * Ok, so this parallel safe indeed switches us into ONLINE.
   */
   EnableMyself();

   LWLockRelease(GenLock);
   return true;
}

EnableMyself() {
   genstate->last_online_in = genstate->current_gen;
   /*
    * Now backends and walreceivers may proceed in normal mode.
    */
   genstate->status = ONLINE;
   fsync genstate->last_online_in;
}

ParallelWorkerMain() {
  ReceiverContext rcv_ctx;
  rcv_ctx.sender_id = arg;
  /* parallel worker always works in normal mode */
  rcv_ctx.mode = REPLMODE_NORMAL;

  while (record = new record from main receiver) {
    if record.type == PREPARE {
      if HandlePrepare(record, rcv_ctx)
        die /* must be in recovery */
    } else {
      HandleCommit(record, rcv_ctx)
    }
  }
}

/*
 * Returns true if we need to reconnect.
 */
bool HandlePrepare(prepare, rcv_ctx) {
  /*
   * xxx it is better (and easy) to avoid taking excl lock inside
   * ConsiderGenSwitch in most cases, but let's keep things simpler here.
   */
  /* Make sure we know about this gen */
  ConsiderGenSwitch(prepare.gen);

  LWLockAcquire(GenLock, LW_SHARED);
  /*
   * Make sure our current connection mode makes sense: applying normally when
   * we are in recovery is unacceptable as we might get xact out of order.
   * Applying in recovery when we are online is also not ok because we would miss
   * reply to coordinator; though I don't see how that might be possible.
   * Again, here we do nothing at all if we are in dead for us gen; in practice,
   * we would apply in recovery mode to decrease the gap.
   */
  if !(genstate->status == RECOVERY && rcv_ctx.mode == REPLMODE_RECOVERY ||
       genstate->status == ONLINE && rcv_ctx.mode == REPLMODE_NORMAL) {
       LWLockRelease(GenLock);
       return true;
   }

  if rcv_ctx.mode == RECOVERY
    if prepare.gen.num == genstate->current_gen.num {
      /*
       * Depending on implementation, under extremely unlikely circumstances due
       * to slow wareceivers convergence we might get P of gen where we are in
       * RECOVERY before corresponding ParallelSafe, if we happen to have working
       * walreceiver in RECOVERY from non-donor gen member. Enable myself then:
       * we definitely have eaten all previous gens xacts, i.e. recovered.
       * Actually, the only reason for ParallelSafe existence is convergence to
       * ONLINE state when no new xacts are happening; otherwise we could leave
       * only this branch and go through RECOVERY->ONLINE on first xact from new gen.
       */
       LWLockRelease(GenLock, LW_SHARED);
       LWLockAcquire(GenLock, LW_EXCLUSIVE);
       if prepare.gen.num == genstate->current_gen.num {
         EnableMyself()
       }
      return true;
    }
    apply prepare, ERROR is unacceptable -- restart recovery if it happens
  } else { /* normal mode */
    if prepare.gen.num < genstate->current_gen.num {
      /* won't prepare xacts from old gen */
      reply to coordinator WontPrepare<MyGenIsHigher, genstate->current_gen>
    } else {
      /* ok, xact from our gen */
      Assert(prepare.gen.num == genstate->current_gen.num);
      apply prepare, reply to coordinator PREPARED or ERROR, if it happens
    }
  }

  LWLockRelease(GenLock);
  return false;
}

/* PC, CP, AP */
HandleCommit(record, rcv_ctx) {
  apply record
  if rcv_ctx.mode == NORMAL {
    reply to coordinator, if needed (precommit)
  }
}

```


## Liveness.

As said above, anyone can at any time propose any generations and we ought to be
safe. However, to make sure the system is live, sane generations should be
proposed. Apart from simple 'offer only nodes who seem to be alive now',
restored node should offer itself when it already recovered to some reasonable
degree (RECOVERY state in current mtm). Here we discuss how this can be done.

DMQ heartbeats carrying view mask and current gen should constantly flow between
live nodes. A heartbeat has some timeout during which it is considered fresh
and thus included into clique (and that view mask in heartbeats) calculation.
Each node periodically (probably also forced on node connect/disconnect)
considers whether is should change gen. First, clique is calculated.
Clique must be calculated in unequivocal manner so that in sausages like A-B-C
majority comes to the same stable clique. Node wants to change the
generation whenever it is present in the clique and either
 1) node can never be online in genstate->current_gen:
   a) either it is just not a member of it
   b) or being online there would violate the last_vote promise
     (genstate->last_vote.num is larger than genstate->current_gen)
   However, node must believe its lag is short before offering gen with it here.
 2) node is online at genstate->current_gen, but clique misses some nodes which
   present in current_gen. i.e. we should exclude someone to proceed.

How to recover initially, to decrease the lag without forcing nodes to wait for
us? The idea is to collect with heartbeats also last_online_in of neightbours.
And node always before initiaing voting for adding itself would either make sure
it most probably (unless many events pass during voting period) won't need
recovery at all (its last_online_in is the same as clique's max) or it first
recovers from node with max last_online_in until lag is less than some
configured bound (or just to last fsync as currently). Obviously, the fresher
last_online_in of other nodes we consider, the less change we would need long
recovery while we think we don't.

Whom to propose exactly? On the first glance, a clique, but here is a kind of
issue which especially subtle on >=5 nodes. We shouldn't propose other nodes if
they were not present in current gen even if they are in clique, because their
lag might be arbitrary big: let them decide on their own when to join. Thus we
should propose something like current_gen.members & clique + me. However, with
>=5 nodes such formula might always yield minority, even if majority is alive
(if this majority consists of one node from latest gen and two laggers) unless
we allow to elect gens with minority members. To sum up,
 - Propose for voting current_gen.members & clique + me.
 - Allow voting for minority gens. Otherwise, e.g. if we have 123 live from
   12345 with 1 and 2 in deep recovery from 3, how we can separately mark 1 as
   recovered while 2 is not yet? The answer is elect 13 as new gen.
   Nobody ever gets ONLINE in such gens, it is just a marker that node is
   recovered enough.
 - Reply to vote request accepting just any clique conforming offer is sort of
   not enough, as simple example shows; with previous example,
   - 13 is elected, 2 in it
   - Then 345 unite again and write 10gb of data, 1 again deeply lagging;
   - Then 123 live again; 2 quickly recovers and proposes 123 while 1 shouldn't
     be proposed because another gen without it emerged since then.
   This particular example hardly might lead to electing deeply lagged node,
   but... generally, to confirm the rule 'only node can enable itself; if older
   gen where it is excluded appeared, only node must be able to add itself again',
   we can add simple if 'the only new node relative to curr gen must be vote initiator'
   to the vote accepting procedure.

Of course, this still doesn't give us iron guarantees of little recovery lag,
because during 'node learned lag is low and proposed to vote or it' ->
'majority agreed' -> 'node broadcasts new gen to at least one old gen member'
roundtrips theoretically arbitrary lag might appear, but I think this is fine
(at least because obviously generally unavoidable).


Elections themselves don't happen instantly, and immediately after their
beginning we still want to change current_gen, which shouldn't fire immediate
election restart. The simplest thing to do here is just have only one process
who performs campaigns, and it quits campaign iff it got replies from all nodes
whom it sent requests or there is suspicion of lost reply (DMQ reported
failure).


## Alternatives

'Promises' not to join generations up to some n seems elegant on the first
glance and resembles first phase of Paxos, however it a bit complicates choosing
when to propose new generation and with which members, as discussed above. One
alternative is don't give such promises, but collect last_online_in to determine
donor in rountrip *after* voting completed, i.e. ask it along with 'elections
finished' announce. That complicates process of switching into gen as each node
need determine donors in additional roundtrip after learning about gen existence
though. Yet another alternative is just recover from all gen members -- at least
one of them would be right donor, however it seems even harder and more
expensive.

It seems it is possible might get away without generations (as centralized
elections with logical clocks counter) at all, like
MtmPrimitiveCurrentMasksFixed.tla. I was going to create a better
non-primitive model of this, but... in this algorithm, whenever node n
learned that node m has disabled it, n must abort its own prepares and forbid
new ones until it recovers (pulling all origins) from m. The explanation is
roughly the following: once m has disabled n, applying its WAL in non-recovery
mode is generally unsafe until ParallelSafe where it is enabled again, as there
might lie xacts which were committed without being first prepared at n.
Similarly, non-aborting own xacts before recovery is also unsafe, because once
recovery starts n gets enabled and thus such xacts might be acknowledged
(and committed), though they lie on n before others which it would get during
recovery. This leads to inadequately many recovery sessions, and I've failed
to invent ways to relax this which would be easier than centralized enabled nodes
switching. There are some more things I like about generations:
 - They allow not to vote in paxos for xacts for which we don't have prepare.
   With >= 5 nodes this might lead to recovery deadlock, but in such deadlock one
   xact will always be aborted, and generations allow easily to say which one: if
   online node in some gen doesn't have prepare of older gen, this prepare will
   never be committed.
 - I suspect centralized enabled nodes might facilitate easier fair node
   addition/removal procedure, but this needs more thought.