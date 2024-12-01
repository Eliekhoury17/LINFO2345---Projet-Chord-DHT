-module(main).
-import(lists, [member/2, nth/2, nthtail/2, append/2, delete/2, reverse/1]).
-import(crypto, [hash/2]).
-import(hashing, [run/2, hash_key/1, read_keys/2, hash_keys/2]).
-import(saving, [create_folder/1, save_into_file/3]).
-record(node, {id, successor, predecessor, keys = []}).
-export([launch/1, launch/9, init/7, init/8, loop/7, speak_all/1, create_ring/1, create_fingerTables/4]).

loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable) ->
    receive
        {speak} ->
            case {Predecessor, Successor} of
                {{PredecessorIndex, _, _}, {SuccessorIndex, _, _}} ->
                    io:format("Node[~w]: succ[~w], pred[~w]~nFingerTable[~w]: ~p~n", [Index, SuccessorIndex, PredecessorIndex, Index, FingerTable]),
                    loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable)
            end;
        {whois, SenderPid} ->
            SenderPid ! {self(), Id},
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {findRangeInternal, Key} ->
            case {Predecessor, Successor} of
                {{_, PredecessorId, _}, {_, _, SuccessorPid}} ->
                    if
                        (Key >= PredecessorId andalso Key < Id) orelse (Id < PredecessorId andalso (Key >= PredecessorId orelse Key < Id)) ->
                            io:format("Key '~w' found in range of node ~w ~n", [Key, Index]);
                        true ->
                            case {Successor, FingerTable} of {{_, SuccessorId, _}, {Size, Age, FingerMap}} ->
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
                                    BestPid ! {findRangeInternal, Key}
                                end
                            end
                    end
            end,
            loop(Index, Id, Successor, Predecessor, Map, DirName, FingerTable);
        {findRange, HashedKey} ->
            self() ! {findRangeInternal, erlang:list_to_integer(HashedKey, 16)},
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
            lists:nth(N - Index, Pids) ! {fingerTable, {N, 0, FingerMap}}
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
    create_fingerTables(Indexs, Ids, Pids, N),
    Pids.

speak_all(Pids) ->
    case Pids of
        [] -> skip;
        [Pid | Tail] ->
            Pid ! {speak},
            speak_all(Tail)
    end.