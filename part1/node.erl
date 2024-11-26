-module(node).
-import(lists, [member/2, nth/2, nthtail/2, append/2, delete/2, reverse/1]).
-import(crypto, [hash/2]).
-import(hashing, [run/2, hash_key/1, read_keys/2, hash_keys/2]).
-export([init/6, init/7, loop/6]).

loop(Index, Id, Successor, Predecessor, Map, DirName) ->
    receive
        {successor, NewSuccessor} ->
            io:format("New successor for ~w (~p) ~p~n", [Id, self(), NewSuccessor]),
            loop(Index, Id, NewSuccessor, Predecessor, Map, DirName);
        {predecessor, NewPredecessor} ->
            io:format("New predecessor for ~w (~p) ~p~n", [Id, self(), NewPredecessor]),
            loop(Index, Id, Successor, NewPredecessor, Map, DirName);
        _ ->
            loop(Index, Id, Successor, Predecessor, Map, DirName)
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
        loop(Index, Id, Successor, Predecessor, Map, DirName)
    end.

init(Index, Id, Successor, Predecessor, Keys, DirName, Map) ->
    case Keys of
        [] ->
            loop(Index, Id, {Id, self()}, {Id, self()}, Map, DirName);
        [Key | Tail] ->
            init(Index, Id, Successor, Predecessor, Tail, DirName, maps:put(Key, default, Map))
    end.

init(Index, Id, Successor, Predecessor, Keys, DirName) ->
    init(Index, Id, Successor, Predecessor, Keys, DirName, #{}).