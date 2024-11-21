-module(main).
-import(lists, [member/2, nth/2, nthtail/2, append/2, delete/2, reverse/1]).
-import(crypto, [hash/2]).
-import(node_logic, [create_node/1, set_successor/2, set_predecessor/2, add_keys/2, create_ring/1]).
-record(node, {id, successor, predecessor, keys = []}).
-export([launch/1, launch/5, loop/4, speak_all/2]).

loop(Id, Successor, Predecessor, Keys) ->
    Pid = self(),
    % io:fwrite("Node ~w started~n", [Id]),
    % io:format("My PID is ~p~n", [Pid]),

    receive
        {Action, Argument} ->
            case Action of
                speak ->
                    io:format("Process ~w (~p) speaks ~p~nSuccessor ~p~nPredecessor ~p~nKeys ~p~n", [Id, Pid, Argument, Successor, Predecessor, Keys]),
                    loop(Id, Successor, Predecessor, Keys);
                whois ->
                    Argument ! {self(), Id},
                    loop(Id, Successor, Predecessor, Keys);
                addKey ->
                    loop(Id, Successor, Predecessor, lists:append([Argument], Keys));
                delKey ->
                    loop(Id, Successor, Predecessor, lists:delete(Argument, Keys));
                addKeys ->
                    loop(Id, Successor, Predecessor, lists:append(Argument, Keys));
                delKeys ->
                    loop(Id, Successor, Predecessor, lists:filter(fun(X) -> not lists:member(X, Argument) end, Keys));
                replaceKeys ->
                    loop(Id, Successor, Predecessor, Keys);
                successor ->
                    io:format("New successor for ~w (~p) ~p~n", [Id, self(), Argument]),
                    loop(Id, Argument, Predecessor, Keys);
                predecessor ->
                    io:format("New predecessor for ~w (~p) ~p~n", [Id, self(), Argument]),
                    loop(Id, Successor, Argument, Keys);
                _ ->
                    loop(Id, Successor, Predecessor, Keys)
            end;
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

% launch(N, Predecessor) ->
%     Pid = spawn(?MODULE, loop, [N - 1, Predecessor, 0]),
%     Predecessor ! {self(), successor, Pid},

%     if
%         N == 0 ->
%             Pid;
%         true ->
%             launch(N - 1, Pid)
%     end.

launch(K, FirstNode, PredecessorPid, NodesList, Pids) ->
    case NodesList of
        [] ->
            Pids;
        [Head] ->
            case Head of
                #node{id = Id, successor = Successor, predecessor = Predecessor, keys = Keys} ->
                    io:format("Id: ~p, Successor: ~p, Predecessor: ~p, Keys: ~p~n", [Id, Successor, Predecessor, Keys]),
                    case FirstNode of
                        {_, FirstNodePid} ->
                            Pid = spawn(?MODULE, loop, [K, none, none, Keys]),
                            Pid ! {predecessor, {Predecessor, PredecessorPid}},
                            Pid ! {successor, FirstNode},
                            FirstNodePid ! {predecessor, {Id, Pid}},
                            PredecessorPid ! {successor, {Id, Pid}},
                            lists:append([Pid], Pids)
                    end
            end;
        [Head | Tail] ->
            case Head of
                #node{id = Id, successor = Successor, predecessor = Predecessor, keys = Keys} ->
                    io:format("Id: ~p, Successor: ~p, Predecessor: ~p, Keys: ~p~n", [Id, Successor, Predecessor, Keys]),
                    if
                        PredecessorPid == none ->
                            Pid = spawn(?MODULE, loop, [K, none, none, Keys]),
                            launch(K + 1, {Id, Pid}, Pid, Tail, lists:append([Pid], Pids));
                        true ->
                            Pid = spawn(?MODULE, loop, [K, none, none, Keys]),
                            Pid ! {predecessor, {Predecessor, PredecessorPid}},
                            PredecessorPid ! {successor, {Id, Pid}},
                            launch(K + 1, FirstNode, Pid, Tail, lists:append([Pid], Pids))
                    end;
                _ ->
                    throw({error, "Invalid entry in ring nodes list"})
            end
    end.


launch(N) ->
    % RawIds = lists:seq(0, N - 1),
    % NodesList = node_logic:create_ring(RawIds),
    % launch(0, none, none, NodesList).
    Node0 = #node{id = 0, successor = 1, predecessor = 3, keys = []},
    Node1 = #node{id = 1, successor = 2, predecessor = 0, keys = []},
    Node2 = #node{id = 2, successor = 3, predecessor = 1, keys = []},
    Node3 = #node{id = 3, successor = 0, predecessor = 2, keys = []},
    Pids = launch(0, none, none, [Node0, Node1, Node2, Node3], []),
    Pids.

    % if
    %     N == 0 ->
    %         ok;
    %     true ->
    %         Pid = spawn(?MODULE, loop, [N - 1, 0, 0]),
    %         LPid = launch(N - 1, Pid),
    %         Pid ! {self(), predeccessor, LPid},
    %         LPid ! {self(), successor, Pid}
    % end.

speak_all(Pids, Argument) ->
    case Pids of
        [] -> skip;
        [Pid | Tail] ->
            Pid ! {speak, Argument},
            speak_all(Tail, Argument)
    end.

% -module(helloworld). 
% -export([start/0, call/2]). 

% call(Arg1, Arg2) -> 
%    io:format("~p ~p~n", [Arg1, Arg2]). 
% start() -> 
%    Pid = spawn(?MODULE, call, ["hello", "process"]), 
%    io:fwrite("~p",[Pid]).