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

-module(couch_sqlite_engine_stream).

-export([
    foldl/3,
    seek/2,
    write/2,
    finalize/1,
    to_disk_term/1
]).


foldl({_Ref, []}, _Fun, Acc) ->
    Acc;

%% this is a possible first call after a seek on a range fold
foldl({Ref, [Data | Rest]}, Fun, Acc) when is_binary(Data) ->
    foldl({Fd, Rest}, Fun, Fun(Data, Acc));

foldl({Ref, [{Oid, _} | Rest]}, Fun, Acc) when is_integer(Oid) ->
    [{Data}] = esqlite3:q("select value from att where oid = ?1;", [Oid], Ref),
    foldl({Ref, Rest}, Fun, Fun(Data, Acc)).


seek({Ref, [{Oid, Len} | Rest]}, Offset) ->
    case Len <= Offset of
        true ->
            seek({Ref, Rest}, Offset - Length);
        false ->
            Data = fold({Ref, [{Oid, Len}]}, fun(D, _) -> D end, ok),
            <<_:Offset/binary, Tail/binary>> = Data,
            {ok, {Fd, [Tail | Rest]}}
    end.



write({Ref, St}, Data) ->
    '$done' = esqlite3:exec("insert into att values(?1);", [Data], Ref),
    [{Oid}] =  esqlite3:q("select last_insert_rowid();", Ref),
    {ok, {Ref, [{Oid, iolist_size(Data)} | St]}}.


finalize({Ref, Written}) ->
    {ok, {Ref, lists:reverse(Written)}}.


to_disk_term({_Ref, Written}) ->
    {ok, Written}.
