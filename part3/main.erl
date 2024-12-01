-module(main).
-import(lists, [member/2, nth/2, nthtail/2, append/2, delete/2, reverse/1]).
-import(crypto, [hash/2]).
-import(hashing, [run/2, hash_key/1, read_keys/2, hash_keys/2]).
-import(saving, [create_folder/1, save_into_file/3]).
-record(node, {id, successor, predecessor, keys = []}).
-export([launch/1, launch/9, init/7, init/8, loop/7, request_fingers/4, speak_all/1, speak_finger_table_all/1, recv_from/0, update_fingers_all/1, create_ring/1, create_fingerTables/4]).

request_fingers(Index, Id, Successor, FingerTable) ->
    case {Successor, FingerTable} of
        {{_, _, SuccessorPid}, {Size, Age, _}} ->
            case Successor of
                {_, _, SuccessorPid} ->
                    lists:foreach(fun(I) ->
                        % whoisfinger, OriginalDistance, CurrentDistance, Size, Age, SenderPid
                        Distance = math:pow(2, I), Size,
                        SuccessorPid ! {whoisfinger, Distance, Distance - 1, Size, Age, {Index, Id, self()}}
                    end, lists:seq(0, trunc(math:log(Size) / math:log(2)) - 1))
            end
    end.

loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable) ->
    receive
        {speak} ->
            case {Predecessor, Successor} of
                {{PredecessorIndex, _, _}, {SuccessorIndex, _, _}} ->
                    io:format("Node[~w]: ~p, succ[~w], pred[~w]~nFingerTable[~w]: ~p~n", [Index, self(), SuccessorIndex, PredecessorIndex, Index, FingerTable]),
                    loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
            end;
        {whois, SenderPid} ->
            SenderPid ! {self(), Id},
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {whoisfinger, OriginalDistance, CurrentDistance, Size, Age, Sender} ->
            case {Sender, Successor, Predecessor, FingerTable} of
                {{SenderIndex, SenderId, SenderPid}, {SuccessorIndex, SuccessorId, SuccessorPid}, {ThisSize, ThisAge, ThisFingerMap}} ->
                    if
                        CurrentDistance == 0 ->
                            io:format("Node[~p]: answers iamfinger to ~p at distance ~p and remaining distance is ~p~n", [
                                Index,
                                SenderIndex,
                                OriginalDistance,
                                CurrentDistance
                            ]),
                            SenderPid ! {iamfinger, OriginalDistance, Size, Age, {Index, Id, self()}};
                        Age =< ThisAge ->
                            {FoundOtherIndex, FoundOtherId, FoundOtherPid, FoundOtherDistance} = maps:fold(
                                fun (OtherId, {OtherIndex, OtherPid, OtherDistance}, {FoundOtherIndex, FoundOtherId, FoundOtherPid, FoundOtherDistance}) ->
                                    if
                                        OtherDistance =< CurrentDistance andalso OtherDistance > FoundOtherDistance ->
                                            {OtherIndex, OtherId, OtherPid, OtherDistance};
                                        true ->
                                            {FoundOtherIndex, FoundOtherId, FoundOtherPid, FoundOtherDistance}
                                    end
                                end,
                                {SuccessorIndex, SuccessorId, SuccessorPid, 1},
                                ThisFingerMap
                            ),
                            io:format("Node[~p]: passes whoisfinger to ~p at distance ~p and remaining distance is ~p~n", [
                                Index,
                                FoundOtherIndex,
                                OriginalDistance,
                                CurrentDistance
                            ]),
                            FoundOtherPid ! {whoisfinger, OriginalDistance, CurrentDistance - FoundOtherDistance, Size, Age, Sender};
                        true ->
                            io:format("Node[~p]: passes whoisfinger to successor ~p at distance ~p and remaining distance is ~p~n", [
                                Index,
                                SuccessorIndex,
                                OriginalDistance,
                                CurrentDistance
                            ]),
                            SuccessorPid ! {whoisfinger, OriginalDistance, CurrentDistance - 1, Size, Age, Sender},
                            loop(Index, Id, Successor, Predecessor, Map, DirName, {ThisSize, Age, #{}})
                    end
            end,

            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {iamfinger, OriginalDistance, Size, Age, Sender} ->
            case {Sender, FingerTable} of
                {{SenderIndex, SenderId, SenderPid}, {ThisSize, ThisAge, ThisFingerMap}} ->
                    if
                        ThisAge =< Age ->
                            io:format("Node ~w has found finder ~w at distance ~p (Age: ~p)~n", [Index, SenderIndex, OriginalDistance, Age]),
                            loop(Index, Id, Successor, Predecessor, Map, DirName, {ThisSize, ThisAge, maps:put(SenderId, {SenderIndex, SenderPid, OriginalDistance}, ThisFingerMap)});
                        true ->
                            io:format("Node ~w rejects finder ~w at distance ~p (Age: ~p)~n", [Index, SenderIndex, OriginalDistance, Age]),
                            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
                    end
            end;
        {updateFingers} ->
            case FingerTable of
                {ThisSize, ThisAge, ThisFingerMap} ->
                    io:format("Force node ~w to update fingers~n", [Index]),
                    request_fingers(Index, Id, Successor, {ThisSize, ThisAge + 1, ThisFingerMap}),
                    loop(Index, Id, Successor, Predecessor, Map, DirName, {ThisSize, ThisAge + 1, #{}})
            end;
        {fullUpdateFingers, StopIndex} ->
            case {Predecessor, FingerTable} of
                {{PredecessorIndex, _, PredecessorPid}, {ThisSize, ThisAge, ThisFingerMap}} ->
                    io:format("Force node ~w to update fingers~n", [Index]),
                    request_fingers(Index, Id, Successor, {ThisSize, ThisAge + 1, ThisFingerMap}),

                    if
                        StopIndex == Index -> PredecessorPid ! {updateFingers};
                        true ->
                            io:format("Ask update fingers from ~w to ~w~n", [Index, PredecessorIndex]),
                            PredecessorPid ! {fullUpdateFingers, StopIndex}
                    end,
                    loop(Index, Id, Successor, Predecessor, Map, DirName, {ThisSize, ThisAge + 1, #{}})
            end;
        {fullUpdateFingers} ->
            case {Successor, Predecessor} of {{SuccessorIndex, _, _}, {_, _, PredecessorPid}} ->
                PredecessorPid ! {fullUpdateFingers, SuccessorIndex},
                loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
            end;
        {ring, Sender, Order} ->
            case Sender of {SenderIndex, _, _} ->
                if
                    SenderIndex == Index -> io:format("~p~n", [Index]);
                    true ->
                        io:format("~p -> ", [Index]),
                        if
                            Order == successor ->
                                case Successor of {_, _, SuccessorPid} ->
                                    SuccessorPid ! {ring, Sender, Order}
                                end;
                            true ->
                                case Predecessor of {_, _, PredecessorPid} ->
                                    PredecessorPid ! {ring, Sender, Order}
                                end
                        end
                end
            end,
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {startRing, Order} ->
            io:format("~n"),
            if
                Order == successor ->
                    case Successor of {_, _, SuccessorPid} ->
                        SuccessorPid ! {ring, {Index, Id, self()}, Order}
                    end;
                true ->
                    case Predecessor of {_, _, PredecessorPid} ->
                        PredecessorPid ! {ring, {Index, Id, self()}, Order}
                    end
            end,
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {startRing} ->
            self() ! {startRing, successor},
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {addKey, Key} ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {getKey, Key} ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {delKey, Key} ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {findRangeInternal, ContactsList, SenderPid, Key, Action} ->
            case {Predecessor, Successor} of
                {{_, PredecessorId, PredecessorPid}, {_, _, SuccessorPid}} ->
                    if
                        (Key >= PredecessorId andalso Key < Id) orelse (Id < PredecessorId andalso (Key >= PredecessorId orelse Key < Id)) ->
                            case Action of
                                {add, Value} ->
                                    io:format("Node[~w]: Add key '~s' with value '~w'~n", [Index, lists:flatten(hashing:hash_to_hex(Key)), Value]),
                                    loop(Index, Id, Successor, Predecessor, maps:put(Key, Value, Map), DirName, FingerTable);
                                {del} ->
                                    io:format("Node[~w]: Del key '~s'~n", [Index, lists:flatten(hashing:hash_to_hex(Key))]),
                                    case maps:is_key(Key, Map) of
                                        true ->
                                            loop(Index, Id, Successor, Predecessor, maps:remove(Key, Map), DirName, FingerTable);
                                        false ->
                                            skip
                                    end;
                                {get, ThisSenderPid} ->
                                    io:format("Node[~w]: Get key '~s'~n", [Index, lists:flatten(hashing:hash_to_hex(Key))]),
                                    ThisSenderPid ! {Index, Id, self(), maps:get(Key, Map, none)};
                                {addNode, NewIndex} ->
                                    case FingerTable of {Size, Age, _} ->
                                        NewId = Key,
                                        Keys = maps:keys(Map),
                                        SortedKeys = lists:sort(Keys),
                                        {OtherMap, NewMap} = lists:foldl(
                                            fun (ThisKey, {OtherMap, NewMap}) ->
                                                if
                                                    ThisKey =< NewId ->
                                                        {OtherMap, maps:put(
                                                            ThisKey,
                                                            maps:get(ThisKey, Map, default),
                                                            NewMap
                                                        )};
                                                    true ->
                                                        {maps:put(
                                                            ThisKey,
                                                            maps:get(ThisKey, Map, default),
                                                            OtherMap
                                                        ), NewMap}
                                                end
                                            end,
                                            {#{}, #{}},
                                            SortedKeys
                                        ),
                                        NewPid = spawn(?MODULE, loop, [NewIndex, NewId, {Index, Id, self()}, Predecessor, NewMap, DirName, {Size, Age + 1, #{}}]),
                                        PredecessorPid ! {successor, {NewIndex, NewId, NewPid}},
                                        % self() ! {fullUpdateFingers},
                                        loop(Index, Id, Successor, {NewIndex, NewId, NewPid}, OtherMap, DirName, {Size, Age + 1, #{}})
                                    end;
                                {prout} ->
                                    io:format("PROUT~n");
                                _ ->
                                    io:format("Key '~s' found in range of node ~w (~p)~n", [lists:flatten(hashing:hash_to_hex(Key)), Index, Action])
                            end,
                            SenderPid ! {findRangeBilan, ContactsList, Key};
                        true ->
                            case {Successor, FingerTable} of {{SuccessorIndex, SuccessorId, SuccessorPid}, {Size, Age, FingerMap}} ->
                                BestNodeKey = maps:fold(
                                    fun (NodeKey, Value, BestNodeKey) ->
                                        if
                                            Key =< NodeKey andalso (NodeKey - Key < BestNodeKey - Key orelse BestNodeKey < Key) ->
                                                NodeKey;
                                            true ->
                                                BestNodeKey
                                        end
                                    end,
                                    SuccessorId,
                                    FingerMap
                                ),

                                case maps:get(BestNodeKey, FingerMap) of {BestIndex, BestPid, BestDistance} ->
                                    io:format("~w asks key to ~w at distance ~w~n", [Index, BestIndex, BestDistance]),
                                    BestPid ! {findRangeInternal, [Id | ContactsList], SenderPid, Key, Action}
                                    % if
                                    %     BestPid == self() ->
                                    %         io:format("ALECT : Node ~w has itself in its fingerTable. Then pass to successor ~w~n", [Index, SuccessorIndex]),
                                    %         SuccessorPid ! {findRangeInternal, [Id | ContactsList], SenderPid, Key, Action};
                                    %     true ->
                                    %         BestPid ! {findRangeInternal, [Id | ContactsList], SenderPid, Key, Action}
                                    % end
                                end
                            end
                    end
            end,
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {findRange, HashedKey, Action} ->
            self() ! {findRangeInternal, [], self(), erlang:list_to_integer(HashedKey, 16), Action},
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {findRangeBilan, ContactsList, Key} ->
            FileNameInArray = io_lib:format("~p_queries.csv", [Index]),
            FileNameInString = lists:flatten(FileNameInArray),

            case {Predecessor, Successor} of
                {{_, PredecessorId, _}, {_, SuccessorId, _}} ->
                    ContentInArray = io_lib:format("~s,~s~n", [
                        hashing:hash_to_hex(Key),
                        lists:foldl(fun (Id, String) -> 
                            case String of
                                "" -> hashing:hash_to_hex(Id); % integer_to_list(Key);
                                _ -> String ++ "|" ++ hashing:hash_to_hex(Id) % integer_to_list(Key)
                            end
                        end, "", ContactsList)
                    ])
            end,

            ContentInString = lists:flatten(ContentInArray),
            saving:save_into_file(DirName, FileNameInString, ContentInString, true, append),

            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {successor, NewSuccessor} ->
            io:format("New successor for ~w (~p) ~p~n", [Id, self(), NewSuccessor]),
            loop(Index, Id, NewSuccessor, Predecessor, Map, DirName, FingerTable);
        {predecessor, NewPredecessor} ->
            io:format("New predecessor for ~w (~p) ~p~n", [Id, self(), NewPredecessor]),
            loop(Index, Id, Successor, NewPredecessor, Map, DirName, FingerTable);
        {fingerTable, NewFingerTable} ->
            io:format("New fingerTable for ~w (~p) ~p~n", [Id, self(), NewFingerTable]),
            loop(Index, Id, Successor, Predecessor, Map, DirName, NewFingerTable);
        {addNode, Index} ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        _ ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
    after 5000 ->
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

        % self() ! {updateFingers},

        loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
    end.

init(Index, Id, Successor, Predecessor, Keys, DirName, Map, Size) ->
    case Keys of
        [] ->
            loop(Index, Id, {Id, self()}, {Id, self()}, Map, DirName, {Size, 0, #{}});
        [Key | Tail] ->
            init(Index, Id, Successor, Predecessor, Tail, DirName, maps:put(Key, default, Map), Size)
    end.

init(Index, Id, Successor, Predecessor, Keys, DirName, Size) ->
    init(Index, Id, Successor, Predecessor, Keys, DirName, #{}, Size).

launch(Index, NodesList, Indexs, Ids, Pids, First, Predecessor, DirName, Size) ->
    case NodesList of
        [] ->
            Pids;
        [Head] ->
            case Head of
                #node{id = Id, keys = Keys} ->
                    case {First, Predecessor} of
                        {none, none} ->
                            Pid = spawn(?MODULE, init, [Index, Id, none, none, Keys, DirName, Size]),
                            {lists:append([Index], Indexs), lists:append([Id], Ids), lists:append([Pid], Pids)};
                        {{FirstIndex, FirstId, FirstPid}, {PredecessorIndex, PredecessorId, PredecessorPid}} ->
                            Pid = spawn(?MODULE, init, [Index, Id, none, none, Keys, DirName, Size]),
                            Pid ! {predecessor, {PredecessorIndex, PredecessorId, PredecessorPid}},
                            PredecessorPid ! {successor, {Index, Id, Pid}},
                            FirstPid ! {predecessor, {Index, Id, Pid}},
                            Pid ! {successor, {FirstIndex, FirstId, FirstPid}},
                            {lists:append([Index], Indexs), lists:append([Id], Ids), lists:append([Pid], Pids)}
                    end
            end;
        [Head | Tail] ->
            case Head of
                #node{id = Id, keys = Keys} ->
                    case Predecessor of
                        none ->
                            Pid = spawn(?MODULE, init, [Index, Id, none, none, Keys, DirName, Size]),
                            launch(Index + 1, Tail,
                            lists:append([Index], Indexs), lists:append([Id], Ids), lists:append([Pid], Pids),
                            {Index, Id, Pid}, {Index, Id, Pid}, DirName, Size);
                            % launch({Id, Pid}, Pid, Tail, lists:append([Pid], Pids));
                        {PredecessorIndex, PredecessorId, PredecessorPid} ->
                            Pid = spawn(?MODULE, init, [Index, Id, none, none, Keys, DirName, Size]),
                            Pid ! {predecessor, {PredecessorIndex, PredecessorId, PredecessorPid}},
                            PredecessorPid ! {successor, {Index, Id, Pid}},
                            launch(Index + 1, Tail,
                            lists:append([Index], Indexs), lists:append([Id], Ids), lists:append([Pid], Pids),
                            First, {Index, Id, Pid}, DirName, Size)
                            % launch(FirstNode, Pid, Tail, lists:append([Pid], Pids))
                    end
            end
    end.

create_ring(NodeIds) ->
    SortedIds = lists:sort(lists:map(fun hashing:hash_key/1, NodeIds)),
    AllKeys = lists:sort(hashing:hash_keys("keys.csv", 65)),
    Len = length(SortedIds),
    lists:map(fun(Index) ->
        Id = lists:nth(Index, SortedIds),
        PredIndex = (Index - 2 + Len) rem Len + 1,
        PredecessorId = lists:nth(PredIndex, SortedIds),
        Keys = [KId || KId <- AllKeys, (KId > PredecessorId andalso KId < Id) orelse (Id < PredecessorId andalso (KId >= PredecessorId orelse KId < Id))],
        #node{id = Id, keys = Keys}
    end, lists:seq(1, Len)).

create_fingerTables(Indexs, Ids, Pids, N) ->
    M = trunc(math:log(N) / math:log(2)),
    Jumps = lists:seq(0, M - 1),
    lists:foreach(
        fun (Index) ->
            FingerMap = lists:foldl(
                fun (Jump, Map) ->
                    Distance = trunc(math:pow(2, Jump)),
                    NodeAtJumpIndex = (Index + Distance) rem N,
                    maps:put(
                        lists:nth(NodeAtJumpIndex + 1, Ids),
                        {NodeAtJumpIndex, lists:nth(NodeAtJumpIndex + 1, Pids), Distance},
                        Map
                    )
                end,
                #{},
                Jumps
            ),
            lists:nth(Index + 1, Pids) ! {fingerTable, {N, 0, FingerMap}}
        end,
        Indexs
    ).

launch(N) ->
    DirNameInArray = io_lib:format("nth_~p/", [N]),
    DirNameInString = lists:flatten(DirNameInArray),
    saving:create_folder(DirNameInString, true),
    RawIds = lists:seq(0, N - 1),
    NodesList = create_ring(RawIds),
    {Indexs, Ids, Pids} = launch(0, NodesList, [], [], [], none, none, DirNameInString, N),
    create_fingerTables(lists:reverse(Indexs), lists:reverse(Ids), lists:reverse(Pids), N),
    Pids.

speak_all(Pids) ->
    case Pids of
        [] -> skip;
        [Pid | Tail] ->
            Pid ! {speak},
            speak_all(Tail)
    end.

recv_from() ->
    receive
        Message ->
            io:format("~p~n", [Message])
    after 1000 ->
        io:format("No reponse~n")
    end.

speak_finger_table_all(Pids) ->
    case Pids of
        [] -> skip;
        [Pid | Tail] ->
            Pid ! {speakFingerTable},
            speak_all(Tail)
    end.

update_fingers_all(Pids) ->
    case Pids of
        [] -> skip;
        [Pid | Tail] ->
            Pid ! {updateFingers},
            speak_all(Tail)
    end.