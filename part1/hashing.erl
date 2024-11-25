-module(hashing).
-import(lists, [member/2, nth/2, nthtail/2, append/2, delete/2, reverse/1]).
-import(crypto, [hash/2]).
-export([run/2, hash_key/1, read_keys/2, hash_keys/2]).

hash_key(Key) when is_integer(Key) ->
    hash_key(integer_to_list(Key));

hash_key(Key) when is_list(Key) ->
    <<Hash:16, _/binary>> = crypto:hash(sha, Key),
    Hash;

hash_key(Key) when is_binary(Key) ->
    hash_key(binary_to_list(Key)).


read_keys(File, MaxLines) ->
    {ok, Binary} = file:read_file(File),
    Lines = binary:split(Binary, <<"\n">>, [global]),
    FilteredLines = lists:filter(fun(Line) -> Line =/= <<>> end, Lines),
    lists:sublist(FilteredLines, min(length(FilteredLines), MaxLines)).

hash_keys(File, MaxLines) ->
    Keys = read_keys(File, MaxLines),
    lists:map(fun(Key) -> {binary_to_list(Key), hash_key(Key)} end, Keys).

run(File, MaxLines) ->
    KeyHashes = hash_keys(File, MaxLines),
    lists:foreach(fun({Key, Hash}) ->
        io:format("Key: ~s, Hash: ~p~n", [Key, Hash])
    end, KeyHashes).