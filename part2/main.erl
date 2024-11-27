-module(main).
-import(lists, [member/2, nth/2, nthtail/2, append/2, delete/2, reverse/1]).
-import(crypto, [hash/2]).
-import(hashing, [run/2, hash_key/1, read_keys/2, hash_keys/2]).
-import(saving, [create_folder/1, save_into_file/3]).
-record(node, {id, successor, predecessor, keys = []}).
-export([launch/1, launch/6, init/6, init/7, loop/7, speak_all/1, reliable_speak_all/1, create_ring/1]).

loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable) ->
    receive
        {speak} ->
            case {Predecessor, Successor} of
                {{_, PredecessorId, _}, {_, SuccessorId, _}} ->
                    io:format("~w,~w,~w,~p~n", [Id, SuccessorId, PredecessorId, maps:keys(Map)]),
                    loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
            end;
        {reliableSpeak, SenderPid} ->
            case {Predecessor, Successor} of
                {{_, PredecessorId, _}, {_, SuccessorId, _}} ->
                    io:format("~w,~w,~w,~p~n", [Id, SuccessorId, PredecessorId, maps:keys(Map)]),
                    loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
            end,
            SenderPid ! {ackSpeak};
        {whois, SenderPid} ->
            SenderPid ! {self(), Id},
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {addKey, Key} ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {getKey, Key} ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {delKey, Key} ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {findRange, Key} ->
            FoundNode = maps:fold(fun (NodeKey, NodePid, Acc) ->
                if
                    Key > NodeKey ->
                        Acc;
                    true ->
                        Dist = NodeKey - Key,

                        case Acc of
                            none ->
                                {Dist, NodeKey, NodePid};
                            {BestDist, _, _} ->
                                if
                                    Dist < BestDist ->
                                        {Dist, NodeKey, NodePid};
                                    true ->
                                        Acc
                                end
                        end
                end
            end, none, FingerTable),

            case FoundNode of
                none ->
                    case Successor of
                        {_, _, SuccessorPid} ->
                            SuccessorPid ! {discover, Key, self()}
                    end;
                {_, NodeKey, NodePid} ->
                    NodePid ! {findRange, Key}
            end,

            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {discover, Key, AskerPid} ->
            case {Predecessor, Successor} of
                {{_, PredecessorId, _}, {_, _, SuccessorPid}} ->
                    if
                        (Key >= PredecessorId andalso Key < Id) orelse (Id < PredecessorId andalso (Key >= PredecessorId orelse Key < Id)) ->
                            case maps:is_key(Key, Map) of
                                true ->
                                    AskerPid ! {itsme, Id, self()},
                                    loop(Index, Id, Successor, Predecessor, maps:remove(Key, Map), DirName, FingerTable)
                            end
                    end,
                    SuccessorPid ! {discover, Key, AskerPid},
                    loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
            end,
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {addKeyNaive, Key, Value} ->
            case {Predecessor, Successor} of
                {{_, PredecessorId, _}, {_, _, SuccessorPid}} ->
                    if
                        Key >= PredecessorId, Key < Id ->
                            loop(Index, Id, Successor, Predecessor, maps:put(Key, Value, Map), DirName, FingerTable);
                        Id < PredecessorId, Key >= PredecessorId orelse Key < Id ->
                            loop(Index, Id, Successor, Predecessor, maps:put(Key, Value, Map), DirName, FingerTable);
                        true ->
                            SuccessorPid ! {addKey, Key, Value},
                            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
                    end
            end;
        {getKeyNaive, Key} ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {delKeyNaive, Key} ->
            case {Predecessor, Successor} of
                {{_, PredecessorId, _}, {_, _, SuccessorPid}} ->
                    if
                        Key >= PredecessorId, Key < Id ->
                            case maps:is_key(Key, Map) of
                                true ->
                                    loop(Index, Id, Successor, Predecessor, maps:remove(Key, Map), DirName, FingerTable);
                                false ->
                                    loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
                            end;
                        Id < PredecessorId, Key >= PredecessorId orelse Key < Id ->
                            case maps:is_key(Key, Map) of
                                true ->
                                    loop(Index, Id, Successor, Predecessor, maps:remove(Key, Map), DirName, FingerTable);
                                false ->
                                    loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
                            end;
                        true ->
                            SuccessorPid ! {delKey, Key},
                            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
                    end
            end;
        {findRangeNaive, Key} ->
            case {Predecessor, Successor} of
                {{_, PredecessorId, _}, {_, _, SuccessorPid}} ->
                    if
                        Key >= PredecessorId, Key < Id ->
                            io:format("Key '~w' found in range of node ~w ~n", [Key, Id]);
                        Id < PredecessorId, Key >= PredecessorId orelse Key < Id ->
                            io:format("Key '~w' found in range of node ~w ~n", [Key, Id]);
                        true ->
                            SuccessorPid ! {findRange, Key}
                    end
            end,
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {successor, NewSuccessor} ->
            io:format("New successor for ~w (~p) ~p~n", [Id, self(), NewSuccessor]),
            loop(Index, Id, NewSuccessor, Predecessor, Map, DirName, FingerTable);
        {predecessor, NewPredecessor} ->
            io:format("New predecessor for ~w (~p) ~p~n", [Id, self(), NewPredecessor]),
            loop(Index, Id, Successor, NewPredecessor, Map, DirName, FingerTable);
        _ ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
    after 1000 ->
        FileNameInArray = io_lib:format("~p.csv", [Index]),
        FileNameInString = lists:flatten(FileNameInArray),

        case {Predecessor, Successor} of
            {{_, PredecessorId, _}, {_, SuccessorId, _}} ->
                ContentInArray = io_lib:format("~s,~s,~s,~s", [
                    hashing:hash_to_hex(Id),
                    hashing:hash_to_hex(SuccessorId),
                    hashing:hash_to_hex(PredecessorId),
                    lists:foldl(fun (Key, Output) -> 
                        case Output of
                            "" -> hashing:hash_to_hex(Key); % integer_to_list(Key);
                            _ -> Output ++ "|" ++ hashing:hash_to_hex(Key) % integer_to_list(Key)
                        end
                    end, "", maps:keys(Map))
                ])
        end,

        ContentInString = lists:flatten(ContentInArray),
        saving:save_into_file(DirName, FileNameInString, ContentInString, true),
        loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
    end.

init(Index, Id, Successor, Predecessor, Keys, DirName, Map) ->
    case Keys of
        [] ->
            loop(Index, Id, {Id, self()}, {Id, self()}, Map, DirName, #{});
        [Key | Tail] ->
            init(Index, Id, Successor, Predecessor, Tail, DirName, maps:put(Key, default, Map))
    end.

init(Index, Id, Successor, Predecessor, Keys, DirName) ->
    init(Index, Id, Successor, Predecessor, Keys, DirName, #{}).

launch(Index, NodesList, Pids, First, Predecessor, DirName) ->
    case NodesList of
        [] ->
            Pids;
        [Head] ->
            case Head of
                #node{id = Id, keys = Keys} ->
                    case {First, Predecessor} of
                        {none, none} ->
                            Pid = spawn(?MODULE, init, [Index, Id, none, none, Keys, DirName]),
                            lists:append([Pid], Pids);
                        {{FirstIndex, FirstId, FirstPid}, {PredecessorIndex, PredecessorId, PredecessorPid}} ->
                            Pid = spawn(?MODULE, init, [Index, Id, none, none, Keys, DirName]),
                            Pid ! {predecessor, {PredecessorIndex, PredecessorId, PredecessorPid}},
                            PredecessorPid ! {successor, {Index, Id, Pid}},
                            FirstPid ! {predecessor, {Index, Id, Pid}},
                            Pid ! {successor, {FirstIndex, FirstId, FirstPid}},
                            lists:append([Pid], Pids)
                    end
            end;
        [Head | Tail] ->
            case Head of
                #node{id = Id, keys = Keys} ->
                    case Predecessor of
                        none ->
                            Pid = spawn(?MODULE, init, [Index, Id, none, none, Keys, DirName]),
                            launch(Index + 1, Tail, lists:append([Pid], Pids), {Index, Id, Pid}, {Index, Id, Pid}, DirName);
                            % launch({Id, Pid}, Pid, Tail, lists:append([Pid], Pids));
                        {PredecessorIndex, PredecessorId, PredecessorPid} ->
                            Pid = spawn(?MODULE, init, [Index, Id, none, none, Keys, DirName]),
                            Pid ! {predecessor, {PredecessorIndex, PredecessorId, PredecessorPid}},
                            PredecessorPid ! {successor, {Index, Id, Pid}},
                            launch(Index + 1, Tail, lists:append([Pid], Pids), First, {Index, Id, Pid}, DirName)
                            % launch(FirstNode, Pid, Tail, lists:append([Pid], Pids))
                    end
            end
    end.



-spec create_ring(list(integer())) -> list(#node{}).
create_ring(NodeIds) ->
    SortedIds = lists:sort(lists:map(fun hashing:hash_key/1, NodeIds)),
    AllKeys = lists:sort(hashing:hash_keys("keys.csv", 65000)),
    Len = length(SortedIds),
    lists:map(fun(Index) ->
        Id = lists:nth(Index, SortedIds),
        PredIndex = (Index - 2 + Len) rem Len + 1,
        PredecessorId = lists:nth(PredIndex, SortedIds),
        Keys = [KId || KId <- AllKeys, (KId > PredecessorId andalso KId < Id) orelse (Id < PredecessorId andalso (KId >= PredecessorId orelse KId < Id))],
        #node{id = Id, keys = Keys}
    end, lists:seq(1, Len)).

launch(N) ->
    DirNameInArray = io_lib:format("nth_~p/", [N]),
    DirNameInString = lists:flatten(DirNameInArray),
    saving:create_folder(DirNameInString, true),
    RawIds = lists:seq(0, N - 1),
    NodesList = create_ring(RawIds),
    Pids = launch(0, NodesList, [], none, none, DirNameInString),
    Pids.

speak_all(Pids) ->
    case Pids of
        [] -> skip;
        [Pid | Tail] ->
            Pid ! {speak},
            speak_all(Tail)
    end.

reliable_speak_all(Pids) ->
    case Pids of
        [] -> skip;
        [Pid | Tail] ->
            Pid ! {reliableSpeak, self()},
            receive
                {ackSpeak} ->
                    speak_all(Tail)
            after 1000 ->
                io:format("No answers from ~p~n", [Pid]),
                speak_all(Tail)
            end
    end.