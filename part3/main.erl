-module(main).
-import(lists, [member/2, nth/2, nthtail/2, append/2, delete/2, reverse/1]).
-import(crypto, [hash/2]).
-import(hashing, [run/2, hash_key/1, read_keys/2, hash_keys/2]).
-import(saving, [create_folder/1, save_into_file/3]).
-record(node, {index, id, keys = []}).

% Ring launching functions
-export([launch/1, launch/2, test/0, test2/0, test3/0, test4/0]).
% Print ring functions
-export([speak_all/1, speak_finger_table_all/1, show_ring/1]).
% Functions proper to a node/process
-export([init/7, init/8, loop/7, request_fingers/4]).
% Functions to interact with nodes
-export([recv_from/0, update_fingers_all/1]).
% Specifically to use the distributed hashmap
-export([add_node/2, get_key/2, add_key/3, del_key/2]).

request_fingers(Index, Id, Successor, FingerTable) ->
    case {Successor, FingerTable} of
        {{_, _, SuccessorPid}, {Size, Age, _}} ->
            case Successor of
                {_, _, SuccessorPid} ->
                    lists:foreach(fun(I) ->
                        % whoisfinger, OriginalDistance, CurrentDistance, Size, Age, SenderPid
                        Distance = math:pow(2, I), Size,
                        SuccessorPid ! {whoisfinger, Distance, Distance - 1, Size, Age, {Index, Id, self()}, 0}
                    end, lists:seq(0, trunc(math:log(Size) / math:log(2)) - 1))
            end
    end.

loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable) ->
    case FingerTable of {_, _, FingerMap} ->
        if
            FingerMap == #{} ->
                io:format("CLEAR[~p]:~p~n", [Index, FingerTable]);
            true ->
                skip
        end
    end,
    receive
        {speak} ->
            case {Predecessor, Successor} of
                {{PredecessorIndex, _, _}, {SuccessorIndex, _, _}} ->
                    io:format("Node[~w]: ~p, ~p, succ[~w], pred[~w]~nFingerTable[~w]: ~p~n", [Index, Id, self(), SuccessorIndex, PredecessorIndex, Index, FingerTable]),
                    loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
            end;
        {whois, SenderPid} ->
            SenderPid ! {self(), Id},
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {whoisfinger, OriginalDistance, CurrentDistance, Size, Age, Sender, NumberOfHops} ->
            case {Sender, Successor, FingerTable} of
                {{SenderIndex, SenderId, SenderPid}, {SuccessorIndex, SuccessorId, SuccessorPid}, {ThisSize, ThisAge, ThisFingerMap}} ->
                    if
                        CurrentDistance == 0 ->
                            io:format("Node[~p]: answers iamfinger[~p:~p:~p] to ~p at distance ~p and remaining distance is ~p~n", [
                                Index,
                                SenderIndex,
                                Age,
                                OriginalDistance,
                                SenderIndex,
                                OriginalDistance,
                                CurrentDistance
                            ]),
                            SenderPid ! {iamfinger, OriginalDistance, Size, Age, {Index, Id, self()}, NumberOfHops + 1},
                            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
                        Age =< ThisAge ->
                            {FoundOtherIndex, FoundOtherId, FoundOtherPid, FoundOtherDistance} = maps:fold(
                                fun (OtherId, {OtherIndex, OtherPid, OtherDistance, _}, {FoundOtherIndex, FoundOtherId, FoundOtherPid, FoundOtherDistance}) ->
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
                            % io:format("Node[~p]: passes whoisfinger[~p:~p] to ~p at distance ~p and remaining distance is ~p~n", [
                            %     Index,
                            %     SenderIndex,
                            %     OriginalDistance,
                            %     FoundOtherIndex,
                            %     OriginalDistance,
                            %     CurrentDistance
                            % ]),
                            FoundOtherPid ! {whoisfinger, OriginalDistance, CurrentDistance - FoundOtherDistance, Size, Age, Sender, NumberOfHops + 1},
                            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
                        true ->
                            % io:format("Node[~p]: passes whoisfinger[~p:~p] to successor ~p at distance ~p and remaining distance is ~p~n", [
                            %     Index,
                            %     SenderIndex,
                            %     OriginalDistance,
                            %     SuccessorIndex,
                            %     OriginalDistance,
                            %     CurrentDistance
                            % ]),
                            SuccessorPid ! {whoisfinger, OriginalDistance, CurrentDistance - 1, Size, Age, Sender, NumberOfHops + 1},
                            loop(Index, Id, Successor, Predecessor, Map, DirName, {Size, Age, #{}})
                    end
            end;
        {iamfinger, OriginalDistance, Size, Age, Sender, NumberOfHops} ->
            case {Sender, FingerTable} of
                {{SenderIndex, SenderId, SenderPid}, {ThisSize, ThisAge, ThisFingerMap}} ->
                    if
                        ThisAge == Age ->
                            M = {ThisSize, ThisAge, maps:put(SenderId, {SenderIndex, SenderPid, OriginalDistance, NumberOfHops}, ThisFingerMap)},
                            io:format("Node ~w has found finder ~w at distance ~p (Age: ~p, NumberOfHops: ~p)~nNewMap: ~p~n", [Index, SenderIndex, OriginalDistance, Age, NumberOfHops, M]),
                            % io:format("~p~n", [maps:put(SenderId, {SenderIndex, SenderPid, OriginalDistance, NumberOfHops}, ThisFingerMap)]),
                            loop(Index, Id, Successor, Predecessor, Map, DirName, M);
                        true ->
                            io:format("Node ~w rejects finder ~w at distance ~p (ThisAge: ~p, Age: ~p, NumberOfHops: ~p)~n", [Index, SenderIndex, OriginalDistance, ThisAge, Age, NumberOfHops]),
                            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
                    end
            end;
        {fullUpdateFingers, StopIndex, Age, Size} ->
            % io:format("~n~nALERT %%%%%%%%%%%%%%%%%%%%% ~w vs ~w~n~n", [Index, StopIndex]),
            case {Predecessor, FingerTable} of
                {{PredecessorIndex, _, PredecessorPid}, {ThisSize, ThisAge, ThisFingerMap}} ->
                    % io:format("Force node ~w to update fingers~n", [Index]),
                    request_fingers(Index, Id, Successor, {Size, Age, #{}}),

                    if
                        StopIndex == Index -> skip;
                        true -> PredecessorPid ! {fullUpdateFingers, StopIndex, Age, Size}
                    end,
                    loop(Index, Id, Successor, Predecessor, Map, DirName, {Size, Age, #{}})
            end;
        {fullUpdateFingers, Age, Size} ->
            case {Successor, Predecessor} of {{SuccessorIndex, _, _}, {_, _, PredecessorPid}} ->
                PredecessorPid ! {fullUpdateFingers, Index, Age, Size},
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
        {findRangeInternal, ContactsList, SenderPid, Key, Action} ->
            case {Predecessor, Successor} of
                {{_, PredecessorId, PredecessorPid}, {_, _, SuccessorPid}} ->
                    if
                        (Key >= PredecessorId andalso Key < Id) orelse (Id < PredecessorId andalso (Key >= PredecessorId orelse Key < Id)) ->
                            case Action of
                                {add, Value, ThisSenderPid} ->
                                    io:format("Node[~w]: Add key '~s' with value '~w'~n", [Index, lists:flatten(hashing:hash_to_hex(Key)), Value]),
                                    ThisSenderPid ! {Index, Id, self(), ok},
                                    SenderPid ! {findRangeBilan, ContactsList, Key},
                                    loop(Index, Id, Successor, Predecessor, maps:put(Key, Value, Map), DirName, FingerTable);
                                {del, ThisSenderPid} ->
                                    io:format("Node[~w]: Del key '~s'~n", [Index, lists:flatten(hashing:hash_to_hex(Key))]),
                                    SenderPid ! {findRangeBilan, ContactsList, Key},
                                    case maps:is_key(Key, Map) of
                                        true ->
                                            ThisSenderPid ! {Index, Id, self(), ok},
                                            loop(Index, Id, Successor, Predecessor, maps:remove(Key, Map), DirName, FingerTable);
                                        false ->
                                            ThisSenderPid ! {Index, Id, self(), notfound},
                                            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
                                    end;
                                {get, ThisSenderPid} ->
                                    io:format("Node[~w]: Get key '~s'~n", [Index, lists:flatten(hashing:hash_to_hex(Key))]),
                                    ThisSenderPid ! {Index, Id, self(), maps:get(Key, Map, none)},
                                    SenderPid ! {findRangeBilan, ContactsList, Key},
                                    loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
                                {addNode, NewIndex, ThisSenderPid} ->
                                    io:format("Node[~w]: Add node with index ~w and id '~s'~n", [Index, NewIndex, lists:flatten(hashing:hash_to_hex(Key))]),
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
                                        NewPid = spawn(?MODULE, loop, [NewIndex, NewId, {Index, Id, self()}, Predecessor, NewMap, DirName, {Size + 1, Age + 1, #{}}]),
                                        PredecessorPid ! {successor, {NewIndex, NewId, NewPid}},
                                        % self() ! {fullUpdateFingers},
                                        ThisSenderPid ! {Index, Id, self(), {PredecessorPid, NewPid}},
                                        % timer:sleep(500),
                                        self() ! {fullUpdateFingers, Age + 1, Size + 1},
                                        loop(Index, Id, Successor, {NewIndex, NewId, NewPid}, OtherMap, DirName, FingerTable)
                                    end;
                                {prout} ->
                                    io:format("PROUT~n");
                                _ ->
                                    io:format("Key '~s' found in range of node ~w (~p)~n", [lists:flatten(hashing:hash_to_hex(Key)), Index, Action])
                            end;
                        true ->
                            case {Successor, FingerTable} of {{SuccessorIndex, SuccessorId, SuccessorPid}, {Size, Age, FingerMap}} ->
                                BestNodeKey = maps:fold(
                                    fun (NodeKey, Value, BestNodeKey) ->
                                        case erlang:abs(NodeKey - Key) < erlang:abs(BestNodeKey - Key) of
                                            %math:abs(NodeKey - Key) < math:abs(BestNodeKey - Key) -> %orelse BestNodeKey < Key) ->
                                            true ->
                                                NodeKey;
                                            false ->
                                                BestNodeKey
                                        end
                                    end,
                                    SuccessorId,
                                    FingerMap
                                ),

                                case maps:get(BestNodeKey, FingerMap, {SuccessorIndex, SuccessorPid, 1, none}) of {BestIndex, BestPid, BestDistance, _} ->
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
            % case NewSuccessor of {NewSuccessorIndex, _, _} ->
            %     io:format("Node[~w]: New successor ~w~n", [Index, NewSuccessorIndex])
            % end,
            loop(Index, Id, NewSuccessor, Predecessor, Map, DirName, FingerTable);
        {predecessor, NewPredecessor} ->
            % case NewPredecessor of {NewPredecessorIndex, _, _} ->
            %     io:format("Node[~w]: New predecessor ~w~n", [Index, NewPredecessorIndex])
            % end,
            loop(Index, Id, Successor, NewPredecessor, Map, DirName, FingerTable);
        {fingerTable, NewFingerTable} ->
            % io:format("New fingerTable for ~w (~p) ~p~n", [Id, self(), NewFingerTable]),
            loop(Index, Id, Successor, Predecessor, Map, DirName, NewFingerTable);
        {addNode, Index} ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        _ ->
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
    after 5000 ->
        case {Predecessor, Successor} of
            {{_, PredecessorId, _}, {_, SuccessorId, _}} ->
                FileNameInArray = io_lib:format("~p.csv", [Index]),
                FileNameInString = lists:flatten(FileNameInArray),
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
                ]),
                ContentInString = lists:flatten(ContentInArray),
                saving:save_into_file(DirName, FileNameInString, ContentInString, true),

                FileNameInArray2 = io_lib:format("~p_fingerTable.csv", [Index]),
                FileNameInString2 = lists:flatten(FileNameInArray2),
                ContentInArray2 = io_lib:format("~p", [
                    FingerTable
                ]),
                ContentInString2 = lists:flatten(ContentInArray2),
                saving:save_into_file(DirName, FileNameInString2, ContentInString2, true);
            _ ->
                io:format("Node[~w]: Invalid successor (~p) or predecessor (~p)~n", [Index, Successor, Predecessor])
        end,
    

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

launch(N, Debug) ->
    % Create the directory
    DirNameInArray = io_lib:format("nth_~p/", [N]),
    DirNameInString = lists:flatten(DirNameInArray),
    saving:create_folder(DirNameInString, true),

    % Create the ring
    OrderedIndexs = lists:seq(0, N - 1),
    Identifiants = lists:map(
        fun(I) ->
            case Debug of
                true -> {I, I * 1000000000000000000};
                _ -> {I, hashing:hash_key(I)}
            end
        end,
        OrderedIndexs
    ),
    SortedIdentifians = lists:sort(
        fun (X, Y) ->
            case {X, Y} of {{IndexX, HashedIndexX}, {IndexY, HashedIndexY}} ->
                HashedIndexX < HashedIndexY
            end
        end,
        Identifiants
    ),
    io:format("~p~n", [SortedIdentifians]),
    Length = erlang:length(SortedIdentifians),
    AllKeys = lists:sort(hashing:hash_keys("keys.csv", 65)),
    RingList = lists:map(
        fun(I) ->
            case lists:nth(I + 1, SortedIdentifians) of {Index, HashedIndex} ->
                Id = HashedIndex,
                case lists:nth(((I - 1) + Length) rem Length + 1, SortedIdentifians) of {PreIndex, PreHashedIndex} ->
                    Keys = [KId || KId <- AllKeys, (KId > PreHashedIndex andalso KId < Id) orelse (Id < PreHashedIndex andalso (KId >= PreHashedIndex orelse KId < Id))],
                    #node{index = Index, id = Id, keys = Keys}
                end
            end
        end,
        OrderedIndexs
    ),

    % According to the ring description inits the nodes
    {Indexs, Ids, Pids} = lists:foldl(
        fun (I, {Indexs, Ids, Pids}) ->
            case lists:nth(I, RingList) of #node{index = Index, id = Id, keys = Keys} ->
                Pid = spawn(?MODULE, init, [Index, Id, none, none, Keys, DirNameInString, N]),
                {Indexs ++ [Index], Ids ++ [Id], Pids ++ [Pid]}
            end
        end,
        {[], [], []},
        lists:seq(1, erlang:length(RingList))
    ),
    Length = erlang:length(RingList),
    M = trunc(math:log(N) / math:log(2)),
    Jumps = lists:seq(0, M - 1),
    Distances = lists:map(
        fun (Jump) ->
            trunc(math:pow(2, Jump))
        end,
        Jumps
    ),

    % Add corresponding successor, predecessor and fingerTable to each node
    lists:foreach(
        fun (I) ->
            case {lists:nth(I + 1, Indexs), lists:nth(I + 1, Ids), lists:nth(I + 1, Pids)} of {Index, Id, Pid} ->
                PredecessorI = (I - 1 + Length) rem Length,
                SuccessorI = (I + 1 + Length) rem Length,
                {PredecessorIndex, PredecessorId, PredecessorPid} = {lists:nth(PredecessorI + 1, Indexs), lists:nth(PredecessorI + 1, Ids), lists:nth(PredecessorI + 1, Pids)},
                {SuccessorIndex, SuccessorId, SuccessorPid} = {lists:nth(SuccessorI + 1, Indexs), lists:nth(SuccessorI + 1, Ids), lists:nth(SuccessorI + 1, Pids)},
                Pid ! {predecessor, {PredecessorIndex, PredecessorId, PredecessorPid}},
                Pid ! {successor, {SuccessorIndex, SuccessorId, SuccessorPid}},

                FingerMap = lists:foldl(
                    fun (Distance, Map) ->
                        OtherI = (I + Distance + Length) rem Length,
                        {OtherIndex, OtherId, OtherPid} = {lists:nth(OtherI + 1, Indexs), lists:nth(OtherI + 1, Ids), lists:nth(OtherI + 1, Pids)},
                        maps:put(OtherId, {OtherIndex, OtherPid, Distance, 0}, Map)
                    end,
                    #{},
                    Distances
                ),
                Pid ! {fingerTable, {N, 0, FingerMap}}
            end
        end,
        lists:seq(0, erlang:length(RingList) - 1)
    ),
    Pids.

launch(N) ->
    launch(N, false).

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

add_node(NewIndex, Pids) ->
    NewId = hashing:hash_key(1024),
    Length = erlang:length(Pids),
    I = 1 + rand:uniform(Length - 1),
    Pid = lists:nth(I, Pids),
    HashedId = lists:flatten(hashing:hash_to_hex(NewId)),
    Pid ! {findRange, HashedId, {addNode, NewIndex, self()}},
    receive
        {Index, _, _, {PredecessorPid, NewPid}} ->
            io:format("Node[~w] has answered ok ~p~n", [Index, NewPid]),
            NewPids = lists:foldl(
                fun (Pid, NewPids) ->
                    if
                        Pid == PredecessorPid ->
                            [Pid, NewPid] ++ NewPids;
                        true ->
                            [Pid] ++ NewPids
                    end
                end,
                [],
                Pids
            ),
            NewPids
    after 5000 ->
        io:format("No responses....~n"),
        Pids
    end.

show_ring(Pids) ->
    Length = erlang:length(Pids),
    I = 1 + rand:uniform(Length - 1),
    Pid = lists:nth(I, Pids),
    %Pid ! {startRing}.
    Pid ! {startRing, predecessor}.

get_key(Key, Pids) ->
    Length = erlang:length(Pids),
    I = 1 + rand:uniform(Length - 1),
    Pid = lists:nth(I, Pids),
    Pid ! {findRange, Key, {get, self()}},
    receive
        {Index, _, _, Value} ->
            io:format("Node[~w] has answered with value ~p~n", [Index, Value]),
            Value
    after 5000 ->
        io:format("No responses....~n"),
        none
    end.

add_key(Key, Value, Pids) ->
    Length = erlang:length(Pids),
    I = 1 + rand:uniform(Length - 1),
    Pid = lists:nth(I, Pids),
    Pid ! {findRange, Key, {add, Value, self()}},
    receive
        {Index, _, _, Value} ->
            io:format("Node[~w] has answered with value ~p~n", [Index, Value]),
            Value
    after 5000 ->
        io:format("No responses....~n"),
        none
    end.

del_key(Key, Pids) ->
    Length = erlang:length(Pids),
    I = 1 + rand:uniform(Length - 1),
    Pid = lists:nth(I, Pids),
    Pid ! {findRange, Key, {del, self()}},
    receive
        {Index, _, _, Value} ->
            io:format("Node[~w] has answered with value ~p~n", [Index, Value]),
            Value
    after 5000 ->
        io:format("No responses....~n"),
        none
    end.

test() ->
    Pids = launch(16, true),
    timer:sleep(500),
    lists:nth(1, Pids) ! {speak},
    timer:sleep(500),
    io:format("==================== Key is at distance 1~n"),
    lists:nth(1, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(1)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 4~n"),
    lists:nth(1, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(3000000000000000001)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 8~n"),
    lists:nth(1, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(7000000000000000001)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 15~n"),
    lists:nth(1, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(14000000000000000001)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 5~n"),
    lists:nth(1, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(4000000000000000001)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 7~n"),
    lists:nth(1, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(6000000000000000001)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 13~n"),
    lists:nth(1, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(12000000000000000001)), {get, self()}},
    recv_from().

test2() ->
    Pids = launch(16, true),
    timer:sleep(500),
    lists:nth(7, Pids) ! {speak},
    timer:sleep(500),
    io:format("==================== Key is at distance 1~n"),
    lists:nth(7, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(6000000000000000001)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 4~n"),
    lists:nth(7, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(9000000000000000001)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 8~n"),
    lists:nth(7, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(13000000000000000001)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 15~n"),
    lists:nth(7, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(4000000000000000001)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 5~n"),
    lists:nth(7, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(10000000000000000001)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 7~n"),
    lists:nth(7, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(12000000000000000001)), {get, self()}},
    timer:sleep(500),
    io:format("==================== Key is at distance 13~n"),
    lists:nth(7, Pids) ! {findRange, lists:flatten(hashing:hash_to_hex(2000000000000000001)), {get, self()}},
    recv_from().

test3() ->
    Pids = launch(16, true),
    timer:sleep(500),
    Pids2 = main:add_node(1024, Pids),
    io:format("========================~n"),
    timer:sleep(2000),
    speak_all(Pids2),
    io:format("========================~n"),
    timer:sleep(500),
    io:format("Old pids : ~p~n", [Pids]),
    io:format("New pids : ~p~n", [Pids2]),
    Pids2.

test4() ->
    Pids = launch(16, true),
    timer:sleep(500),
    Pids2 = main:add_node(128, main:add_node(256, main:add_node(2048, main:add_node(1024, main:add_node(24, Pids))))),
    io:format("========================~n"),
    timer:sleep(500),
    %lists:nth(1, Pids2) ! {fullUpdateFingers, 10},
    timer:sleep(2000),
    speak_all(Pids2),
    io:format("========================~n"),
    timer:sleep(500),
    io:format("Old pids : ~p~n", [Pids]),
    io:format("New pids : ~p~n", [Pids2]),
    Pids2.