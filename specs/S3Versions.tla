----------------------------- MODULE S3Versions -----------------------------
(***************************************************************************)
(* One key of an S3 bucket in internal/store: its current record          *)
(* (objects/<hash>.json), its noncurrent records (versions/<hash>/<id>),   *)
(* and its blobs. Writers hold the key lock; lifecycle expiry holds the    *)
(* exclusive bucket lock. Blob staging, readers, crashes, and recovery at  *)
(* Open interleave with them. Each step is one file commit, removal, or    *)
(* read.                                                                   *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS
    MaxIds,         \* blobs and delete markers the run may create
    MaxCrashes,     \* process restarts
    MaxOpen,        \* GetObjectVersion attempts (16 in objects.go:230)
    Locked,         \* TRUE: readers hold the key lock (the fix)
    SweepLocked,    \* TRUE: lifecycle expiry also holds the key lock (the fix)
    CommitFails,    \* TRUE: the objects/ commit in install may fail
    LookupReaders,  \* GetObjectVersion and HeadObjectVersion callers
    ListReaders     \* ListVersions callers

NULLV == 0                     \* the null version ID
CUR == -1                      \* GetObject and HeadObject: the current version
Vids == 0..MaxIds              \* an Enabled version ID is its Seq
Reader == LookupReaders \cup ListReaders
None == [id |-> 0, seq |-> 0, vid |-> NULLV, dm |-> FALSE]
Has(r) == r.id # 0
Blob(r) == IF r.dm THEN 0 ELSE r.id

VARIABLES
    status,     \* versioning: "Off" (never set), "Enabled", "Suspended"
    obj,        \* objects/<hash>.json, or None
    vers,       \* versions/<hash>/<vid>.json, or None
    blobs,      \* blob files of this key
    nid, sq,    \* last blob or marker ID, last Seq
    staged,     \* committed blobs that wait for the key lock
    phase,      \* "run", "boot" (after a crash), "repair" (recoverKeyVersions)
    crashes,
    wpc,        \* installer: "idle" or "busy"
    plan,       \* the installer's remaining file operations
    pendA,      \* the logical versions after the installer's operation
    A,          \* the logical versions that callers were told about
    lock,       \* key lock holder: "none", "w", or a reader
    rplan,      \* recovery promotion steps
    rrem,       \* recovery duplicate removals (version IDs)
    lk,         \* lookup reader state
    ls,         \* list reader state
    lalive,     \* ghost: the wanted version existed in every state of the read
    valive,     \* ghost: the version IDs that existed in every state of the list
    vsnap       \* ghost: the version ID sets that the key held during the list

files == <<status, obj, vers, blobs>>
vars == <<status, obj, vers, blobs, nid, sq, staged, phase, crashes, wpc, plan,
          pendA, A, lock, rplan, rrem, lk, ls, lalive, valive, vsnap>>

VersRecs == {vers[v] : v \in {u \in Vids : Has(vers[u])}}
FileRecs == VersRecs \cup (IF Has(obj) THEN {obj} ELSE {})
Present(v) == IF v = CUR THEN Has(obj) /\ ~obj.dm ELSE (Has(obj) /\ obj.vid = v) \/ Has(vers[v])
PresentSet == {v \in Vids : Present(v)}
PresentP(v) == IF v = CUR THEN Has(obj') /\ ~obj'.dm ELSE (Has(obj') /\ obj'.vid = v) \/ Has(vers'[v])
PresentSetP == {v \in Vids : PresentP(v)}
MaxSeq(S) == CHOOSE r \in S : \A q \in S : q.seq <= r.seq

\* noncurrent (versions.go:170): what readers list beside the current record.
Noncurrent(cur) ==
    {r \in VersRecs : r.seq # cur.seq /\ (r.vid # NULLV \/ cur.vid # NULLV)}
Logical == IF Has(obj) THEN {obj} \cup Noncurrent(obj) ELSE {}
LogicalVids == {r.vid : r \in Logical}

\* A file operation. A failable one runs the cleanup f when its commit fails.
Eff(k, v, r, b, fl, f) == [k |-> k, v |-> v, r |-> r, b |-> b, fl |-> fl, f |-> f]
SetObj(r) == Eff("obj", 0, r, 0, FALSE, <<>>)
SetVer(v, r) == Eff("vers", v, r, 0, FALSE, <<>>)
DelBlob(b) == Eff("blob", 0, None, b, FALSE, <<>>)
BlobDel(r) == IF Blob(r) # 0 THEN <<DelBlob(Blob(r))>> ELSE <<>>

Apply(e) ==
    CASE e.k = "obj"  -> obj' = e.r /\ UNCHANGED <<vers, blobs>>
      [] e.k = "vers" -> vers' = [vers EXCEPT ![e.v] = e.r] /\ UNCHANGED <<obj, blobs>>
      [] e.k = "blob" -> blobs' = blobs \ {e.b} /\ UNCHANGED <<obj, vers>>

\* install (versions.go:210). rec is new; old is the current record or None.
InstallPlan(old, rec) ==
    IF status = "Off" THEN
        \* versions.go:211-218: commit, then remove the old blob. After a failed
        \* commit, PutObject removes the new blob (objects.go:159).
        <<Eff("obj", 0, rec, 0, TRUE, BlobDel(rec))>> \o (IF Has(old) THEN BlobDel(old) ELSE <<>>)
    ELSE
        LET oldIsNull == Has(old) /\ old.vid = NULLV
            save == Has(old) /\ ~(status = "Suspended" /\ oldIsNull)
            nullAside == status = "Suspended" /\ ~oldIsNull /\ Has(vers[NULLV])
            replaced == IF Has(old) /\ status = "Suspended" /\ oldIsNull THEN old
                        ELSE IF nullAside THEN vers[NULLV] ELSE None
            \* versions.go:250-255: a failed commit removes the saved copy;
            \* PutObject then removes the new blob (objects.go:159).
            undo == (IF save THEN <<SetVer(old.vid, None)>> ELSE <<>>) \o BlobDel(rec)
        IN  (IF save THEN <<SetVer(old.vid, old)>> ELSE <<>>)     \* versions.go:235
            \o <<Eff("obj", 0, rec, 0, TRUE, undo)>>               \* versions.go:250
            \o (IF nullAside THEN <<SetVer(NULLV, None)>> ELSE <<>>) \* versions.go:257-258
            \o BlobDel(replaced)                                   \* versions.go:260-264

\* deleteVersion (versions.go:319), with the key lock held.
DelVerPlan(id) ==
    LET isCur == Has(obj) /\ obj.vid = id
        target == IF isCur THEN obj ELSE vers[id]
        older == {r \in Noncurrent(obj) : r.vid # obj.vid}
    IN  IF ~isCur /\ ~Has(vers[id]) THEN <<>>               \* versions.go:326-333
        ELSE IF isCur THEN
            (IF Has(vers[obj.vid]) THEN <<SetVer(obj.vid, None)>> ELSE <<>>)  \* :344
            \o (IF older = {} THEN <<SetObj(None)>>                           \* :350
                ELSE <<SetObj(MaxSeq(older)), SetVer(MaxSeq(older).vid, None)>>) \* :355-358
            \o BlobDel(target)                                                \* :363-364
        ELSE <<SetVer(id, None)>> \o BlobDel(target)                          \* :360, :363-364

NewRec(id, dm) ==
    [id |-> id, dm |-> dm,
     seq |-> IF status = "Off" THEN 0 ELSE sq + 1,
     vid |-> IF status = "Enabled" THEN sq + 1 ELSE NULLV]
NextSq == IF status = "Off" THEN sq ELSE sq + 1

\* The logical versions after an install or a permanent delete.
AbsInstall(rec) ==
    CASE status = "Off"       -> {rec}
      [] status = "Enabled"   -> A \cup {rec}
      [] status = "Suspended" -> {r \in A : r.vid # NULLV} \cup {rec}

Running == phase = "run"
Idle == wpc = "idle"

\* Take the key lock and read the files that fix the plan. An empty plan is a
\* no-op, such as deleting a missing version.
Begin(p, newA) ==
    /\ Running /\ Idle /\ lock = "none"
    /\ UNCHANGED <<obj, vers, blobs>>
    /\ IF p = <<>>
         THEN UNCHANGED <<wpc, lock, plan, pendA, A>>
         ELSE wpc' = "busy" /\ lock' = "w" /\ plan' = p /\ pendA' = newA /\ UNCHANGED A

\* An expiry without SweepLocked holds no key lock, and a reader may hold it.
Finish(rest, newA) ==
    IF rest = <<>>
      THEN /\ wpc' = "idle" /\ plan' = <<>> /\ A' = newA /\ pendA' = newA
           /\ lock' = IF lock = "w" THEN "none" ELSE lock
      ELSE plan' = rest /\ pendA' = newA /\ UNCHANGED <<wpc, lock, A>>

ReadersUnchanged == UNCHANGED <<lk, ls>>
RecoveryUnchanged == UNCHANGED <<rplan, rrem, crashes>>

-----------------------------------------------------------------------------
(* Installer. PutObject commits a blob under the shared bucket lock, then  *)
(* installs it under the key lock (objects.go:133-158).                    *)

StageBlob ==
    /\ Running /\ nid < MaxIds
    /\ nid' = nid + 1 /\ blobs' = blobs \cup {nid + 1} /\ staged' = staged \cup {nid + 1}
    /\ UNCHANGED <<status, obj, vers, sq, phase, wpc, plan, pendA, A, lock>>
    /\ ReadersUnchanged /\ RecoveryUnchanged

\* A failed write condition: PutObject removes its blob (objects.go:159).
DropBlob(b) ==
    /\ Running /\ b \in staged
    /\ blobs' = blobs \ {b} /\ staged' = staged \ {b}
    /\ UNCHANGED <<status, obj, vers, nid, sq, phase, wpc, plan, pendA, A, lock>>
    /\ ReadersUnchanged /\ RecoveryUnchanged

Put(b) ==
    /\ b \in staged
    /\ LET rec == NewRec(b, FALSE) IN Begin(InstallPlan(obj, rec), AbsInstall(rec))
    /\ staged' = staged \ {b} /\ sq' = NextSq
    /\ UNCHANGED <<status, nid, phase>> /\ ReadersUnchanged /\ RecoveryUnchanged

\* DeleteObject without a version ID (objects.go:262).
DelCur ==
    /\ IF status = "Off"
         THEN /\ Begin(IF Has(obj) /\ ~obj.dm                      \* objects.go:294
                        THEN <<SetObj(None)>> \o BlobDel(obj)      \* objects.go:303-308
                        ELSE <<>>, {})
              /\ UNCHANGED <<nid, sq>>
         ELSE /\ nid < MaxIds
              /\ LET m == NewRec(nid + 1, TRUE) IN Begin(InstallPlan(obj, m), AbsInstall(m))
              /\ nid' = nid + 1 /\ sq' = NextSq
    /\ UNCHANGED <<status, staged, phase>> /\ ReadersUnchanged /\ RecoveryUnchanged

\* Lifecycle expiry (lifecycle.go:101) holds the exclusive bucket lock, so
\* no writer and no staged blob is in flight. It skips delete markers.
Expire ==
    /\ Running /\ Idle /\ staged = {} /\ Has(obj) /\ ~obj.dm
    /\ SweepLocked => lock = "none"
    /\ status # "Off" => nid < MaxIds
    /\ LET m == NewRec(nid + 1, TRUE) IN
         IF status = "Off"
           THEN plan' = <<SetObj(None)>> \o BlobDel(obj) /\ pendA' = {}
           ELSE plan' = InstallPlan(obj, m) /\ pendA' = AbsInstall(m)
    /\ wpc' = "busy"
    /\ lock' = IF SweepLocked THEN "w" ELSE lock
    /\ nid' = IF status = "Off" THEN nid ELSE nid + 1
    /\ sq' = NextSq
    /\ UNCHANGED <<status, obj, vers, blobs, staged, phase, A>>
    /\ ReadersUnchanged /\ RecoveryUnchanged

DelVer(id) ==
    /\ Begin(DelVerPlan(id), {r \in A : r.vid # id})
    /\ UNCHANGED <<status, nid, sq, staged, phase>> /\ ReadersUnchanged /\ RecoveryUnchanged

Step ==
    /\ Running /\ wpc = "busy"
    /\ Apply(Head(plan))
    /\ Finish(Tail(plan), pendA)
    /\ UNCHANGED <<status, nid, sq, staged, phase>> /\ ReadersUnchanged /\ RecoveryUnchanged

\* The objects/ commit in install fails: nothing changes, the operation
\* reports an error, and its cleanup runs instead of the rest of the plan.
StepFail ==
    /\ CommitFails /\ Running /\ wpc = "busy" /\ Head(plan).fl
    /\ Finish(Head(plan).f, A)
    /\ UNCHANGED <<status, obj, vers, blobs, nid, sq, staged, phase>>
    /\ ReadersUnchanged /\ RecoveryUnchanged

\* PutBucketConfiguration holds the bucket lock exclusively (configuration.go:57),
\* so no installer and no staged blob is in flight.
SetStatus(s) ==
    /\ Running /\ Idle /\ staged = {}
    /\ status' = s
    /\ UNCHANGED <<obj, vers, blobs, nid, sq, staged, phase, wpc, plan, pendA, A, lock>>
    /\ ReadersUnchanged /\ RecoveryUnchanged

-----------------------------------------------------------------------------
(* Lookup readers: lookup (versions.go:101) and the blob open in           *)
(* GetObjectVersion (objects.go:213-226). With Locked, lookupLocked       *)
(* (versions.go:141) holds the key lock for the lookup but not the open.   *)

\* capped: the read gave up after MaxOpen blob opens.
LIdle == [pc |-> "idle", get |-> FALSE, want |-> NULLV, rec |-> None, last |-> 0,
          tries |-> 0, res |-> "none", capped |-> FALSE]

Release(r) == IF Locked /\ lock = r THEN lock' = "none" ELSE UNCHANGED lock
FilesSame == UNCHANGED <<status, obj, vers, blobs, nid, sq, staged, phase, wpc, plan, pendA, A>>

\* Found rec: Head returns it; Get opens its blob next.
Found(r, rec) ==
    IF lk[r].get /\ ~rec.dm
      THEN lk' = [lk EXCEPT ![r].pc = "open", ![r].rec = rec] /\ Release(r)
      ELSE lk' = [lk EXCEPT ![r].pc = "idle", ![r].res = "found", ![r].rec = rec] /\ Release(r)

Missing(r) == lk' = [lk EXCEPT ![r].pc = "idle", ![r].res = "missing"] /\ Release(r)

\* versions.go:102-115: read objects/. For the current version, a delete
\* marker is a miss (lookupObject, versions.go:133).
L1(r) ==
    /\ Running /\ lk[r].pc = "l1"
    /\ Locked => lock \in {"none", r}
    /\ IF ~Has(obj) THEN Missing(r)
       ELSE IF lk[r].want = CUR THEN (IF obj.dm THEN Missing(r) ELSE Found(r, obj))
       ELSE IF obj.vid = lk[r].want THEN Found(r, obj)
       ELSE lk' = [lk EXCEPT ![r].pc = "l2"] /\ (IF Locked THEN lock' = r ELSE UNCHANGED lock)
    /\ FilesSame /\ UNCHANGED ls /\ RecoveryUnchanged

LStart(r, get, want) ==
    /\ Running /\ lk[r].pc = "idle"
    /\ lk' = [lk EXCEPT ![r] = [LIdle EXCEPT !.pc = "l1", !.get = get, !.want = want]]
    /\ FilesSame /\ UNCHANGED <<ls, lock>> /\ RecoveryUnchanged

\* versions.go:116: read versions/<want>.
L2(r) ==
    /\ Running /\ lk[r].pc = "l2"
    /\ IF Has(vers[lk[r].want]) THEN Found(r, vers[lk[r].want])
       ELSE lk' = [lk EXCEPT ![r].pc = "l3"] /\ UNCHANGED lock
    /\ FilesSame /\ UNCHANGED ls /\ RecoveryUnchanged

\* versions.go:117-122: read objects/ again after a missing version file.
L3(r) ==
    /\ Running /\ lk[r].pc = "l3"
    /\ IF Has(obj) /\ obj.vid = lk[r].want THEN Found(r, obj) ELSE Missing(r)
    /\ FilesSame /\ UNCHANGED ls /\ RecoveryUnchanged

\* objects.go:218-225: open the blob, or look up again when a writer removed it.
LOpen(r) ==
    /\ Running /\ lk[r].pc = "open"
    /\ LET b == lk[r].rec.id IN
       IF b \in blobs THEN lk' = [lk EXCEPT ![r].pc = "idle", ![r].res = "found"]
       ELSE IF b = lk[r].last \/ lk[r].tries + 1 >= MaxOpen
         THEN lk' = [lk EXCEPT ![r].pc = "idle", ![r].res = "error", ![r].capped = (b # lk[r].last)]
         ELSE lk' = [lk EXCEPT ![r].pc = "l1", ![r].last = b, ![r].tries = @ + 1]
    /\ FilesSame /\ UNCHANGED <<ls, lock>> /\ RecoveryUnchanged

-----------------------------------------------------------------------------
(* List readers: ListVersions (versions.go:271) for this key. It reads     *)
(* objects/, then lists versions/, then reads each listed file.            *)

VIdle == [pc |-> "idle", cur |-> None, names |-> {}, out |-> {}, ran |-> FALSE]

VFinish(r, s) == ls' = [ls EXCEPT ![r] = [s EXCEPT !.pc = "idle", !.ran = TRUE]] /\ Release(r)

\* readAllObjects (objects.go:338) finds the key. With Locked, keyVersions
\* (versions.go:304) reads it again under the key lock (VLock).
VStart(r) ==
    /\ Running /\ ls[r].pc = "idle"
    /\ IF ~Has(obj) THEN VFinish(r, VIdle)
       ELSE IF Locked THEN ls' = [ls EXCEPT ![r] = [VIdle EXCEPT !.pc = "lock"]] /\ UNCHANGED lock
       ELSE ls' = [ls EXCEPT ![r] = [VIdle EXCEPT !.pc = "dir", !.cur = obj, !.out = {obj.vid}]]
            /\ UNCHANGED lock
    /\ FilesSame /\ UNCHANGED lk /\ RecoveryUnchanged

VLock(r) ==
    /\ Running /\ Locked /\ ls[r].pc = "lock" /\ lock = "none"
    /\ IF ~Has(obj) THEN VFinish(r, VIdle) /\ UNCHANGED lock
       ELSE ls' = [ls EXCEPT ![r] = [VIdle EXCEPT !.pc = "dir", !.cur = obj, !.out = {obj.vid}]]
            /\ lock' = r
    /\ FilesSame /\ UNCHANGED lk /\ RecoveryUnchanged

\* noncurrent (versions.go:172): ReadDir.
VDir(r) ==
    /\ Running /\ ls[r].pc = "dir"
    /\ LET names == {v \in Vids : Has(vers[v])} IN
       IF names = {} THEN VFinish(r, ls[r])
       ELSE ls' = [ls EXCEPT ![r].pc = "read", ![r].names = names] /\ UNCHANGED lock
    /\ FilesSame /\ UNCHANGED lk /\ RecoveryUnchanged

\* versions.go:179-191: read one listed file; a removed one is skipped.
VRead(r, v) ==
    /\ Running /\ ls[r].pc = "read" /\ v \in ls[r].names
    /\ LET s == ls[r]
           keep == Has(vers[v]) /\ vers[v].seq # s.cur.seq /\ (v # NULLV \/ s.cur.vid # NULLV)
           s2 == [s EXCEPT !.names = @ \ {v}, !.out = IF keep THEN @ \cup {v} ELSE @]
       IN IF s2.names = {} THEN VFinish(r, s2)
          ELSE ls' = [ls EXCEPT ![r] = s2] /\ UNCHANGED lock
    /\ FilesSame /\ UNCHANGED lk /\ RecoveryUnchanged

-----------------------------------------------------------------------------
(* Crash and recovery (store.go:63-83). A crash stops every operation;    *)
(* Open then runs recoverVersions and removeOrphanBlobs on the files.      *)

Crash ==
    /\ crashes < MaxCrashes
    /\ phase' = "boot" /\ crashes' = crashes + 1
    /\ wpc' = "idle" /\ plan' = <<>> /\ lock' = "none" /\ staged' = {}
    /\ rplan' = <<>> /\ rrem' = {}
    /\ lk' = [r \in LookupReaders |-> LIdle] /\ ls' = [r \in ListReaders |-> VIdle]
    /\ UNCHANGED <<status, obj, vers, blobs, nid, sq, pendA, A>>

\* recoverKeyVersions (versions.go:397) reads every file first.
RepairBegin ==
    /\ phase = "boot" /\ VersRecs # {}
    /\ LET top == MaxSeq(VersRecs)
           cur == IF Has(obj) THEN obj ELSE top
           rest == IF Has(obj) THEN VersRecs ELSE VersRecs \ {top}
       IN /\ rplan' = IF Has(obj) THEN <<>> ELSE <<SetObj(top), SetVer(top.vid, None)>>  \* :417-424
          /\ rrem' = {r.vid : r \in {q \in rest : q.seq = cur.seq \/ (q.vid = NULLV /\ cur.vid = NULLV)}} \* :428-433
    /\ phase' = "repair"
    /\ UNCHANGED <<status, obj, vers, blobs, nid, sq, staged, crashes, wpc, plan, pendA, A, lock>>
    /\ ReadersUnchanged

RepairStep ==
    /\ phase = "repair" /\ rplan # <<>>
    /\ Apply(Head(rplan)) /\ rplan' = Tail(rplan)
    /\ UNCHANGED <<status, nid, sq, staged, phase, crashes, wpc, plan, pendA, A, lock, rrem>>
    /\ ReadersUnchanged

RepairRemove(v) ==
    /\ phase = "repair" /\ rplan = <<>> /\ v \in rrem
    /\ vers' = [vers EXCEPT ![v] = None] /\ rrem' = rrem \ {v}
    /\ UNCHANGED <<status, obj, blobs, nid, sq, staged, phase, crashes, wpc, plan, pendA, A, lock, rplan>>
    /\ ReadersUnchanged

\* removeOrphanBlobs (store.go:200) runs after recoverVersions. Open then
\* returns: the interrupted operation took effect entirely or not at all.
Orphans ==
    /\ \/ phase = "boot" /\ VersRecs = {}
       \/ phase = "repair" /\ rplan = <<>> /\ rrem = {}
    /\ blobs' = blobs \cap {Blob(r) : r \in FileRecs}
    /\ phase' = "run"
    /\ A' = IF Logical = pendA THEN pendA ELSE A
    /\ pendA' = A'
    /\ UNCHANGED <<status, obj, vers, nid, sq, staged, crashes, wpc, plan, lock, rplan, rrem>>
    /\ ReadersUnchanged

-----------------------------------------------------------------------------

Init ==
    /\ status = "Off" /\ obj = None /\ vers = [v \in Vids |-> None] /\ blobs = {}
    /\ nid = 0 /\ sq = 0 /\ staged = {}
    /\ phase = "run" /\ crashes = 0
    /\ wpc = "idle" /\ plan = <<>> /\ pendA = {} /\ A = {} /\ lock = "none"
    /\ rplan = <<>> /\ rrem = {}
    /\ lk = [r \in LookupReaders |-> LIdle] /\ ls = [r \in ListReaders |-> VIdle]
    /\ lalive = [r \in LookupReaders |-> TRUE] /\ valive = [r \in ListReaders |-> {}]
    /\ vsnap = [r \in ListReaders |-> {}]

Core ==
    \/ StageBlob \/ \E b \in staged : DropBlob(b) \/ Put(b)
    \/ DelCur \/ Expire \/ \E id \in Vids : DelVer(id)
    \/ Step \/ StepFail
    \/ \E s \in {"Enabled", "Suspended"} : SetStatus(s)
    \/ \E r \in LookupReaders :
         \/ \E get \in BOOLEAN, w \in Vids \cup {CUR} : LStart(r, get, w)
         \/ L1(r) \/ L2(r) \/ L3(r) \/ LOpen(r)
    \/ \E r \in ListReaders : VStart(r) \/ VLock(r) \/ VDir(r) \/ \E v \in Vids : VRead(r, v)
    \/ Crash \/ RepairBegin \/ RepairStep \/ Orphans
    \/ \E v \in rrem : RepairRemove(v)

\* The ghosts track existence from the state where a read starts to the
\* state where it returns, at the file level.
Ghosts ==
    /\ lalive' = [r \in LookupReaders |->
         IF lk'[r].pc = "idle" /\ lk[r].pc = "idle" THEN lalive[r]
         ELSE IF lk[r].pc = "idle" THEN PresentP(lk'[r].want)
         ELSE lalive[r] /\ PresentP(lk'[r].want)]
    /\ valive' = [r \in ListReaders |->
         IF ls[r].pc = "idle" /\ ls'[r] = ls[r] THEN valive[r]
         ELSE IF ls[r].pc = "idle" THEN PresentSetP
         ELSE valive[r] \cap PresentSetP]
    /\ vsnap' = [r \in ListReaders |->
         IF ls[r].pc = "idle" /\ ls'[r] = ls[r] THEN vsnap[r]
         ELSE IF ls[r].pc = "idle" THEN {LogicalVids'}
         ELSE vsnap[r] \cup {LogicalVids'}]

Next == Core /\ Ghosts

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

Quiescent == Running /\ Idle

TypeOK ==
    /\ status \in {"Off", "Enabled", "Suspended"}
    /\ phase \in {"run", "boot", "repair"}
    /\ wpc \in {"idle", "busy"}
    /\ blobs \subseteq 1..MaxIds /\ staged \subseteq blobs
    /\ Idle => plan = <<>>
    /\ Quiescent => pendA = A

\* A version that exists for the whole read is found, unless the read gave
\* up after MaxOpen overwrites removed the blobs it opened.
LookupSound ==
    \A r \in LookupReaders :
      lk[r].pc = "idle" /\ (lk[r].res = "missing" \/ (lk[r].res = "error" /\ ~lk[r].capped)) => ~lalive[r]
\* The known limit: a read of an existing version gives up after MaxOpen
\* overwrites. Expected to fail when MaxIds >= MaxOpen.
LookupNoGiveUp ==
    \A r \in LookupReaders : lk[r].pc = "idle" /\ lk[r].res = "error" => ~lalive[r]

\* A listing includes every version that exists for the whole listing.
ListSound ==
    \A r \in ListReaders : ls[r].pc = "idle" /\ ls[r].ran => valive[r] \subseteq ls[r].out
\* A listing shows the key's versions as they were in one state.
ListAtomic ==
    \A r \in ListReaders : ls[r].pc = "idle" /\ ls[r].ran => ls[r].out \in vsnap[r]

\* AGENTS.md: a key with any version always has an objects/ file.
KeyHasCurrent == VersRecs # {} => Has(obj)
\* Every record file has its blob.
RecordsHaveBlobs == \A r \in FileRecs : ~r.dm => r.id \in blobs
\* At rest: one null version, no stale copy of the current record, the
\* current record is newest, the files hold exactly what callers were told,
\* and no blob is orphaned.
AtRest ==
    Quiescent =>
      /\ ~(Has(obj) /\ obj.vid = NULLV /\ Has(vers[NULLV]))
      /\ Has(obj) => ~Has(vers[obj.vid])
      /\ \A r \in VersRecs : r.seq < obj.seq
      /\ FileRecs = A /\ Logical = A
      /\ blobs = ({Blob(r) : r \in FileRecs} \ {0}) \cup staged
=============================================================================
