-module(main).
-import(lists, [member/2, nth/2, nthtail/2, append/2, delete/2, reverse/1]).
-import(crypto, [hash/2]).
-import(hashing, [run/2, hash_key/1, read_keys/2, hash_keys/2]).
-record(node, {id, successor, predecessor, keys = []}).
-export([launch/1, launch/4, loop/4, speak_all/1, reliable_speak_all/1, create_ring/1]).

loop(Id, Successor, Predecessor, Keys) ->
    receive
        {speak} ->
            case {Successor, Predecessor} of
                {{SuccessorId, _}, {PredecessorId, _}} ->
                    io:format("~w,~w,~w,~p~n", [Id, SuccessorId, PredecessorId, Keys]),
                    loop(Id, Successor, Predecessor, Keys)
            end;
        {reliableSpeak, SenderPid} ->
            case {Successor, Predecessor} of
                {{SuccessorId, _}, {PredecessorId, _}} ->
                    io:format("~w,~w,~w,~p~n", [Id, SuccessorId, PredecessorId, Keys]),
                    loop(Id, Successor, Predecessor, Keys)
            end,
            SenderPid ! {ackSpeak};
        {whois, SenderPid} ->
            SenderPid ! {self(), Id},
            loop(Id, Successor, Predecessor, Keys);
        {addKey, Key} ->
            loop(Id, Successor, Predecessor, Keys);
        {getKey, Key} ->
            case {Predecessor, Successor} of
                {{PredecessorId, _}, {_, SuccessorPid}} ->
                    if
                        Key >= PredecessorId, Key < Id ->
                            io:format("Key '~w' found in node ~w ~n", [Key, Id]);
                        Id < PredecessorId, Key >= PredecessorId orelse Key < Id ->
                            io:format("Key '~w' found in node ~w ~n", [Key, Id]);
                        true ->
                            SuccessorPid ! {getKey, Key}
                    end
            end,
            loop(Id, Successor, Predecessor, Keys);
        {delKey, Key} ->
            loop(Id, Successor, Predecessor, Keys);
        {successor, NewSuccessor} ->
            io:format("New successor for ~w (~p) ~p~n", [Id, self(), NewSuccessor]),
            loop(Id, NewSuccessor, Predecessor, Keys);
        {predecessor, NewPredecessor} ->
            io:format("New predecessor for ~w (~p) ~p~n", [Id, self(), NewPredecessor]),
            loop(Id, Successor, NewPredecessor, Keys);
        Other ->
            io:format("What the fuck ?! ~p~n", [Other]),
            loop(Id, Successor, Predecessor, Keys)
    % after 1000 ->
    %     io:format("Process ~w (~p) says 'I'm alive'~n", [Id, Pid]),
    %     io:format("Successor ~p~n", [Successor]),
    %     io:format("Predecessor ~p~n", [Predecessor]),
    %     io:format("Keys ~p~n", [Keys]),
    %     loop(Id, Successor, Predecessor, Keys)
    end.

launch(NodesList, Pids, First, Predecessor) ->
    case NodesList of
        [] ->
            Pids;
        [Head] ->
            case Head of
                #node{id = Id, keys = Keys} ->
                    case {First, Predecessor} of
                        {none, none} ->
                            Pid = spawn(?MODULE, loop, [Id, none, none, Keys]),
                            lists:append([Pid], Pids);
                        {{FirstId, FirstPid}, {PredecessorId, PredecessorPid}} ->
                            Pid = spawn(?MODULE, loop, [Id, none, none, Keys]),
                            Pid ! {predecessor, {PredecessorId, PredecessorPid}},
                            PredecessorPid ! {successor, {Id, Pid}},
                            FirstPid ! {predecessor, {Id, Pid}},
                            Pid ! {successor, {FirstId, FirstPid}},
                            lists:append([Pid], Pids)
                    end
            end;
        [Head | Tail] ->
            case Head of
                #node{id = Id, keys = Keys} ->
                    case Predecessor of
                        none ->
                            Pid = spawn(?MODULE, loop, [Id, none, none, Keys]),
                            launch(Tail, lists:append([Pid], Pids), {Id, Pid}, {Id, Pid});
                            % launch({Id, Pid}, Pid, Tail, lists:append([Pid], Pids));
                        {PredecessorId, PredecessorPid} ->
                            Pid = spawn(?MODULE, loop, [Id, none, none, Keys]),
                            Pid ! {predecessor, {PredecessorId, PredecessorPid}},
                            PredecessorPid ! {successor, {Id, Pid}},
                            launch(Tail, lists:append([Pid], Pids), First, {Id, Pid})
                            % launch(FirstNode, Pid, Tail, lists:append([Pid], Pids))
                    end
            end
    end.

-spec create_ring(list(integer())) -> list(#node{}).
create_ring(NodeIds) ->
    SortedIds = lists:sort(lists:map(fun hashing:hash_key/1, NodeIds)),
    Len = length(SortedIds),
    lists:map(fun(Index) ->
        Id = lists:nth(Index, SortedIds),
        #node{id = Id}
    end, lists:seq(1, Len)).

launch(N) ->
    RawIds = lists:seq(0, N - 1),
    NodesList = create_ring(RawIds),
    Pids = launch(NodesList, [], none, none),
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