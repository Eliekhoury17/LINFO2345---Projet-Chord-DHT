-module(main).
-import(lists, [member/2, nth/2, nthtail/2, append/2, delete/2, reverse/1]).
-import(crypto, [hash/2]).
-export([launch/1, launch/2, loop/4]).
-record(node, {id, successor, predecessor, keys}).


hash_key(Key) ->
    <<Hash:16, _Rest/binary>> = crypto:hash(sha, Key),
    Hash.

loop(Id, Successor, Predecessor, Keys) ->
    io:fwrite("Node ~w ~p started~n", [Id, hash_key(integer_to_list(Id))]),

    receive
        {Pid, Action, Argument} ->
            case Action of
                whois ->
                    Pid ! {self(), Id},
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
                    loop(Id, Argument, Predecessor, Keys);
                predeccessor ->
                    loop(Id, Successor, Argument, Keys);
                Other ->
                    loop(Id, Successor, Predecessor, Keys)
            end;
        Other ->
            loop(Id, Successor, Predecessor, Keys)
    end.

launch(N, Predecessor) ->
    Pid = spawn(?MODULE, loop, [N - 1, Predecessor, 0]),
    Predecessor ! {self(), successor, Pid},

    if
        N == 0 ->
            Pid;
        true ->
            launch(N - 1, Pid)
    end.

launch(N) ->
    if
        N == 0 ->
            ok;
        true ->
            Pid = spawn(?MODULE, loop, [N - 1, 0, 0]),
            LPid = launch(N - 1, Pid),
            Pid ! {self(), predeccessor, LPid},
            LPid ! {self(), successor, Pid}
    end.

% -module(helloworld). 
% -export([start/0, call/2]). 

% call(Arg1, Arg2) -> 
%    io:format("~p ~p~n", [Arg1, Arg2]). 
% start() -> 
%    Pid = spawn(?MODULE, call, ["hello", "process"]), 
%    io:fwrite("~p",[Pid]).