% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_sqlite_engine).

-behavior(couch_db_engine).

-export([
    exists/1,

    delete/3,
    delete_compaction_files/3,

    init/2,
    terminate/2,
    handle_db_updater_call/2,
    handle_db_updater_info/2,

    incref/1,
    decref/1,
    monitored_by/1,

    last_activity/1,

    get_compacted_seq/1,
    get_del_doc_count/1,
    get_disk_version/1,
    get_doc_count/1,
    get_epochs/1,
    get_last_purged/1,
    get_purge_seq/1,
    get_revs_limit/1,
    get_security/1,
    get_size_info/1,
    get_update_seq/1,
    get_uuid/1,

    set_revs_limit/2,
    set_security/2,

    open_docs/2,
    open_local_docs/2,
    read_doc_body/2,

    serialize_doc/2,
    write_doc_body/2,
    write_doc_infos/4,

    commit_data/1,

    open_write_stream/2,
    open_read_stream/2,
    is_active_stream/2,

    fold_docs/4,
    fold_local_docs/4,
    fold_changes/5,
    count_changes_since/2,

    start_compaction/4,
    finish_compaction/4
]).

-include("couch_sqlite_engine.hrl").


% This is called by couch_server to determine which
% engine should be used for the given database. DbPath
% is calculated based on the DbName and the configured
% extension for a given engine. The first engine to
% return true is the engine that will be used for the
% database.
exists(DbPath) ->
    filelib:is_file(DbPath).

% This is called by couch_server to delete a database. It
% is called from inside the couch_server process which
% means that the storage engine does not have to guarantee
% its own consistency checks when executing in this
% context. Although since this is executed in the context
% of couch_server it should return relatively quickly.
delete(RootDir, DbPath, DelOpts) ->
    %% TODO Call through db_updater to close DB first
    couch_file:delete(RootDir, DbPath, DelOpts).

% This function can be called from multiple contexts. It
% will either be called just before a call to delete/3 above
% or when a compaction is cancelled which executes in the
% context of a couch_db_updater process. It is intended to
% remove any temporary files used during compaction that
% may be used to recover from a failed compaction swap.
delete_compaction_files(_RootDir, _DbPath, _DelOpts) ->
    ok.

% This is called from the couch_db_updater:init/1 context. As
% such this means that it is guaranteed to only have one process
% executing for a given DbPath argument (ie, opening a given
% database is guaranteed to only happen in a single process).
% However, multiple process may be trying to open different
% databases concurrently so if a database requires a shared
% resource that will require concurrency control at the storage
% engine layer.
%
% The returned DbHandle should be a term that can be freely
% copied between processes and accessed concurrently. However
% its guaranteed that the handle will only ever be mutated
% in a single threaded context (ie, within the couch_db_updater
% process).
init(DbPath, DbOpenOpts) ->
    Create = lists:member(create, DbOpenOpts),
    Exists = ?MODULE:exists(DbPath),
    case {Create, Exists} of
        {false, false} ->
            throw({not_found, no_db_file});
        {true, true} ->
            case lists:member(overwrite, DbOpenOpts) of
                true ->
                    RootDir = filename:dirname(DbPath),
                    DelOpts = [sync, {context, delete}],
                    ?MODULE:delete(RootDir, DbPath, DelOpts);
                false ->
                    throw({error, eexist})
            end;
        _ -> ok
    end,
    case esqlite3:open(DbPath) of
        {ok, Ref} ->
            St = #st{filepath = DbPath, ref = Ref},
            init_state(St, DbOpenOpts);
        {error, Error} ->
            throw(Error)
    end.

% This is called in the context of couch_db_updater:terminate/2
% and as such has the same properties for init/2. It's guaranteed
% to be consistent for a given database but may be called by many
% databases concurrently.
terminate(_Reason, #st{} = St) ->
    esqlite3:close(St#st.ref),
    ok.

init_state(#st{ref = Ref} = St, Opts) ->
    %% prepare working tables
    ok = esqlite3:exec("begin;", Ref),
    lists:foreach(fun(Q) -> ok = esqlite3:exec(Q, Ref) end, [
        "create table if not exists meta(key text primary key, value blob);",
        "create table if not exists id(key text primary key, value blob);",
        "create table if not exists seq(key text primary key, value blob);",
        "create table if not exists local(key text primary key, value blob);",
        "create table if not exists docs(key text primary key, value blob);"
    ]),
    ok = esqlite3:exec("commit;", Ref),

    %% maybe update security
    {ok, MetaQ} = esqlite3:prepare("insert into meta values(?1, ?2)", Ref),
    ok = esqlite3:exec("select 1 from meta where key='security';", Ref),
    case esqlite3:changes(Ref) of
        {ok, 1} -> ok;
        {ok, 0} ->
            DSO = couch_util:get_value(default_security_object, Opts, []),
            esqlite3:bind(MetaQ, ["security", term_to_binary(DSO)]),
            esqlite3:step(MetaQ)
    end,

    %% read or set meta
    case esqlite3:q("select value from meta where key='meta'", Ref) of
        [] ->
            DefaultMeta = [
                {disk_version, 1},
                {revs_limit, 1000},
                {uuid, couch_uuids:random()},
                {doc_count, 0},
                {del_doc_count, 0},
                {update_seq, 0},
                {epochs, [{node(), 0}]}
            ],
            esqlite3:bind(MetaQ, ["meta", term_to_binary(DefaultMeta)]),
            esqlite3:step(MetaQ),
            {ok, St#st{meta = DefaultMeta}};
        [{MetaBin}] ->
            Meta = binary_to_term(MetaBin),
            {ok, St#st{meta = Meta}}
    end.

% This is called in the context of couch_db_updater:handle_call/3
% for any message that is unknown. It can be used to handle messages
% from asynchronous processes like the engine's compactor if it has one.
handle_db_updater_call(_Msg, #st{} = St) ->
    {reply, ok, St}.

% This is called in the context of couch_db_updater:handle_info/2
% and has the same properties as handle_call/3.
handle_db_updater_info(_Msg, #st{} = St) ->
    {noreply, St}.

% These functions are called by any process opening or closing
% a database. As such they need to be able to handle being
% called concurrently. For example, the legacy engine uses these
% to add monitors to the main engine process.
incref(#st{} = St) ->
    {ok, St}.

decref(#st{} = _St) ->
    ok.

monitored_by(#st{} = _St) ->
    [self()].

% This is called in the context of couch_db_updater:handle_info/2
% and should return the timestamp of the last activity of
% the database. If a storage has no notion of activity or the
% value would be hard to report its ok to just return the
% result of os:timestamp/0 as this will just disable idle
% databases from automatically closing.
last_activity(#st{} = _St) ->
    os:timestamp().

% All of the get_* functions may be called from many
% processes concurrently.

% The database should make a note of the update sequence when it
% was last compacted. If the database doesn't need compacting it
% can just hard code a return value of 0.
get_compacted_seq(#st{} = _St) ->
    0.

% The number of documents in the database which have all leaf
% revisions marked as deleted.
get_del_doc_count(#st{meta = Meta}) ->
    {del_doc_count, DelDocCount} = lists:keyfind(del_doc_count, 1, Meta),
    DelDocCount.

% This number is reported in the database info properties and
% as such can be any JSON value.
get_disk_version(#st{meta = Meta}) ->
    {disk_version, DiskVersion} = lists:keyfind(disk_version, 1, Meta),
    DiskVersion.

% The number of documents in the database that have one or more
% leaf revisions not marked as deleted.
get_doc_count(#st{meta = Meta}) ->
    {doc_count, DocCount} = lists:keyfind(doc_count, 1, Meta),
    DocCount.

% The epochs track which node owned the database starting at
% a given update sequence. Each time a database is opened it
% should look at the epochs. If the most recent entry is not
% for the current node it should add an entry that will be
% written the next time a write is performed. An entry is
% simply a {node(), CurrentUpdateSeq} tuple.
get_epochs(#st{meta = Meta}) ->
    {epochs, Epochs} = lists:keyfind(epochs, 1, Meta),
    Epochs.

% Get the last purge request performed.
get_last_purged(#st{} = _St) ->
    [].

% Get the current purge sequence. This should be incremented
% for every purge operation.
get_purge_seq(#st{} = _St) ->
    0.

% Get the revision limit. This should just return the last
% value that was passed to set_revs_limit/2.
get_revs_limit(#st{meta = Meta}) ->
    {revs_limit, RevsLimit} = lists:keyfind(revs_limit, 1, Meta),
    RevsLimit.

% Get the current security properties. This should just return
% the last value that was passed to set_security/2.
get_security(#st{ref = Ref}) ->
    Q = "select value from meta where key='security'",
    [{SecPropsBin}] = esqlite3:q(Q, Ref),
    binary_to_term(SecPropsBin).

% This information is displayed in the database info poperties. It
% should just be a list of {Name::atom(), Size::non_neg_integer()}
% tuples that will then be combined across shards. Currently,
% various modules expect there to at least be values for:
%
%   file     - Number of bytes on disk
%
%   active   - Theoretical minimum number of bytes to store this db on disk
%              which is used to guide decisions on compaction
%
%   external - Number of bytes that would be required to represent the
%              contents outside of the database (for capacity and backup
%              planning)
get_size_info(#st{} = _St) ->
    [{file, 0}, {active, 0}, {external, 0}].

% The current update sequence of the database. The update
% sequence should be incrememnted for every revision added to
% the database.
get_update_seq(#st{meta = Meta}) ->
    {update_seq, UpdateSeq} = lists:keyfind(update_seq, 1, Meta),
    UpdateSeq.

% Whenever a database is created it should generate a
% persistent UUID for identification in case the shard should
% ever need to be moved between nodes in a cluster.
get_uuid(#st{meta = Meta}) ->
    {uuid, UUID} = lists:keyfind(uuid, 1, Meta),
    UUID.

% These functions are only called by couch_db_updater and
% as such are guaranteed to be single threaded calls. The
% database should simply store these values somewhere so
% they can be returned by the corresponding get_* calls.

set_revs_limit(#st{ref = Ref, meta = Meta0} = St, RevsLimit) ->
    Meta = lists:keyreplace(revs_limit, 1, Meta0, {revs_limit, RevsLimit}),
    Q = "update meta set value = ?1 where key = 'meta';",
    {ok, Stmt} = esqlite3:prepare(Q, Ref),
    esqlite3:bind(Stmt, [term_to_binary(Meta)]),
    esqlite3:step(Stmt),
    {ok, St#st{meta = Meta}}.

set_security(#st{ref = Ref} = St, SecProps) ->
    Q = "update meta set value = ?1 where key = 'security';",
    {ok, Stmt} = esqlite3:prepare(Q, Ref),
    esqlite3:bind(Stmt, [term_to_binary(SecProps)]),
    esqlite3:step(Stmt),
    {ok, St}.

% This function will be called by many processes concurrently.
% It should return a #full_doc_info{} record or not_found for
% every provided DocId in the order those DocId's appear in
% the input.
%
% Traditionally this function will only return documents that
% were present in the database when the DbHandle was retrieved
% from couch_server. It is currently unknown what would break
% if a storage engine deviated from that property.
open_docs(#st{} = _St, _DocIds) ->
    %% #full_doc_info{}
    not_found.

% This function will be called by many processes concurrently.
% It should return a #doc{} record or not_found for every
% provided DocId in the order they appear in the input.
%
% The same caveats around database snapshots from open_docs
% apply to this function (although this function is called
% rather less frequently so it may not be as big of an
% issue).
open_local_docs(#st{} = _St, _DocIds) ->
    %% #doc{}
    not_found.

% This function will be called from many contexts concurrently.
% The provided RawDoc is a #doc{} record that has its body
% value set to the body value returned from write_doc_body/2.
%
% This API exists so that storage engines can store document
% bodies externally from the #full_doc_info{} record (which
% is the traditional approach and is recommended).
read_doc_body(#st{} = _St, RawDoc) ->
    RawDoc.

% This function is called concurrently by any client process
% that is writing a document. It should accept a #doc{}
% record and return a #doc{} record with a mutated body it
% wishes to have written to disk by write_doc_body/2.
%
% This API exists so that storage engines can compress
% document bodies in parallel by client processes rather
% than forcing all compression to occur single threaded
% in the context of the couch_db_updater process.
serialize_doc(#st{} = _St, Doc) ->
    Doc.

% This function is called in the context of a couch_db_updater
% which means its single threaded for the given DbHandle.
%
% The returned #doc{} record should have its Body set to a value
% that will be stored in the #full_doc_info{} record's revision
% tree leaves which is passed to read_doc_body/2 above when
% a client wishes to read a document.
%
% The BytesWritten return value is used to determine the number
% of active bytes in the database which can is used to make
% a determination of when to compact this database.
write_doc_body(#st{} = _St, Doc) ->
    {ok, Doc, 0}.

% This function is called from the context of couch_db_updater
% and as such is guaranteed single threaded for the given
% DbHandle.
%
% This is probably the most complicated function in the entire
% API due to a few subtle behavior requirements required by
% CouchDB's storage model.
%
% The Pairs argument is a list of pairs (2-tuples) of
% #full_doc_info{} records. The first element of the pair is
% the #full_doc_info{} that exists on disk. The second element
% is the new version that should be written to disk. There are
% three basic cases that should be followed:
%
%     1. {not_found, #full_doc_info{}} - A new document was created
%     2. {#full_doc_info{}, #full_doc_info{}} - A document was updated
%     3. {#full_doc_info{}, not_found} - A document was purged completely
%
% Number one and two are fairly straight forward as long as proper
% accounting for moving entries in the udpate sequence are accounted
% for. However, case 3 you'll notice is "purged completely" which
% means it needs to be removed from the database including the
% update sequence. Also, for engines that are not using append
% only storage like the legacy engine, case 2 can be the result of
% a purge so special care will be needed to see which revisions
% should be removed.
%
% The LocalDocs variable is applied separately. Its important to
% note for new storage engine authors that these documents are
% separate because they should *not* be included as part of the
% changes index for the database.
%
% The PurgedDocIdRevs is the list of Ids and Revisions that were
% purged during this update. While its not guaranteed by the API,
% currently there will never be purge changes comingled with
% standard updates.
%
% Traditionally an invocation of write_doc_infos should be all
% or nothing in so much that if an error occurs (or the VM dies)
% then the database doesn't retain any of the changes. However
% as long as a storage engine maintains consistency this should
% not be an issue as it has never been a guarantee and the
% batches are non-deterministic (from the point of view of the
% client).
write_doc_infos(#st{} = St, _Pairs, _LocalDocs, _PurgedDocIdRevs) ->
    {ok, St}.

% This function is called in the context of couch_db_udpater and
% as such is single threaded for any given DbHandle.
%
% This call is made periodically to ensure that the database has
% stored all updates on stable storage. (ie, here is where you fsync).
commit_data(#st{} = St) ->
    {ok, St}.

% This function is called by multiple processes concurrently.
%
% This function along with open_read_stream are part of the
% attachments API. For the time being I'm leaving these mostly
% undocumented. There are implementations of this in both the
% legacy btree engine as well as the alternative engine
% implementations for the curious, however this is a part of the
% API for which I'd like feed back.
%
% Currently an engine can elect to not implement these API's
% by throwing the atom not_supported.
open_write_stream(#st{} = _St, _Options) ->
    {ok, self()}.

% See the documentation for open_write_stream
open_read_stream(#st{} = _St, _StreamDiskInfo) ->
    ReadStreamState = [],
    {ok, {?MODULE, ReadStreamState}}.

% See the documentation for open_write_stream
is_active_stream(#st{} = _St, _ReadStreamState) ->
    true.

% This funciton is called by many processes concurrently.
%
% This function is called to fold over the documents in
% the database sorted by the raw byte collation order of
% the document id. For each document id, the supplied user
% function should be invoked with the first argument set
% to the #full_doc_info{} record and the second argument
% set to the current user supplied accumulator. The return
% value of the user function is a 2-tuple of {Go, NewUserAcc}.
% The NewUserAcc value should then replace the current
% user accumulator. If Go is the atom ok, iteration over
% documents should continue. If Go is the atom stop, then
% iteration should halt and the return value should be
% {ok, NewUserAcc}.
%
% Possible options to this function include:
%
%     1. start_key - Start iteration at the provided key or
%        or just after if the key doesn't exist
%     2. end_key - Stop iteration prior to visiting the provided
%        key
%     3. end_key_gt - Stop iteration just after the provided key
%     4. dir - The atom fwd or rev. This is to be able to iterate
%        over documents in reverse order. The logic for comparing
%        start_key, end_key, and end_key_gt are then reversed (ie,
%        when rev, start_key should be greater than end_key if the
%        user wishes to see results)
%     5. include_reductions - This is a hack for _all_docs since
%        it currently relies on reductions to count an offset. This
%        is a terrible hack that will need to be addressed by the
%        API in the future. If this option is present the supplied
%        user function expects three arguments, where the first
%        argument is a #full_doc_info{} record, the second argument
%        is the current list of reductions to the left of the current
%        document, and the third argument is the current user
%        accumulator. The return value from the user function is
%        unaffected. However the final return value of the function
%        should include the final total reductions as the second
%        element of a 3-tuple. Like I said, this is a hack.
%     6. include_deleted - By default deleted documents are not
%        included in fold_docs calls. However in some special
%        cases we do want to see them (as of now, just in couch_changes
%        during the design document changes optimization)
%
% Historically, if a process calls this function repeatedly it
% would see the same results returned even if there were concurrent
% updates happening. However there doesn't seem to be any instance of
% that actually happening so a storage engine that includes new results
% between invocations shouldn't have any issues.
fold_docs(#st{} = _St, _UserFold, _UserAcc, _DocFoldOpts) ->
    LastUserAcc = [],
    {ok, LastUserAcc}.

% This function may be called by many processes concurrently.
%
% This should behave exactly the same as fold_docs/4 except that it
% should only return local documents and the first argument to the
% user function is a #doc{} record, not a #full_doc_info{}.
fold_local_docs(#st{} = _St, _UserFold, _UserAcc, _DocFoldOpts) ->
    LastUserAcc = [],
    {ok, LastUserAcc}.

% This function may be called by many processes concurrently.
%
% This function is called to fold over the documents (not local
% documents) in order of their most recent update. Each document
% in the database should have exactly one entry in this sequence.
% If a document is updated during a call to this funciton it should
% not be included twice as that will probably lead to Very Bad Things.
%
% This should behave similarly to fold_docs/4 in that the supplied
% user function should be invoked with a #full_doc_info{} record
% as the first arugment and the current user accumulator as the
% second argument. The same semantics for the return value from the
% user function should be handled as in fold_docs/4.
%
% The StartSeq parameter indicates where the fold should start
% *after*. As in, if a change with a value of StartSeq exists in the
% database it should not be included in the fold.
%
% The only option currently supported by the API is the `dir`
% option that should behave the same as for fold_docs.
fold_changes(#st{} = _St, _StartSeq, _UserFold, _UserAcc, _ChangesFoldOpts) ->
    LastUserAcc = [],
    {ok, LastUserAcc}.

% This function may be called by many processes concurrently.
%
% This function is called to count the number of documents changed
% since the given UpdateSeq (ie, not including the possible change
% at exactly UpdateSeq). It is currently only used internally to
% provide a status update in a replication's _active_tasks entry
% to indicate how many documents are left to be processed.
%
% This is a fairly difficult thing to support in engine's that don't
% behave exactly like a tree with efficient support for counting rows
% between keys. As such returning 0 or even just the difference between
% the current update sequence is possibly the best some storage engines
% can provide. This may lead to some confusion when interpreting the
% _active_tasks entry if the storage engine isn't accounted for by the
% client.
count_changes_since(#st{} = _St, _UpdateSeq) ->
    0.

% This function is called in the context of couch_db_updater and as
% such is guaranteed to be single threaded for the given DbHandle.
%
% If a storage engine requires compaction this is a trigger to start
% it off. However a storage engine can do whatever it wants here. As
% this is fairly engine specific there's not a lot guidance that is
% generally applicable.
%
% When compaction is finished the compactor should use
% gen_server:cast/2 to send a {compact_done, CompactEngine, CompactInfo}
% message to the Parent pid provided. Currently CompactEngine
% must be the same engine that started the compaction and CompactInfo
% is an arbitrary term that's passed to finish_compaction/4.
start_compaction(#st{} = St, _DbName, _Options, _Parent) ->
    {ok, St, self()}.

% This function is called in the context of couch_db_udpater and as
% such is guarnateed to be single threaded for the given DbHandle.
%
% Same as for start_compaction, this will be extremely specific to
% any given storage engine.
%
% The split in the API here is so that if the storage engine needs
% to update the DbHandle state of the couch_db_updater it can as
% finish_compaction/4 is called in the context of the couch_db_updater.
finish_compaction(#st{} = St, _DbName, _Options, _CompactInfo) ->
    {ok, St, self()}.
