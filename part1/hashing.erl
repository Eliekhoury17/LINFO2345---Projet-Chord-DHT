-module(hashing).
-import(lists, [member/2, nth/2, nthtail/2, append/2, delete/2, reverse/1]).
-import(crypto, [hash/2]).
-export([run/2, hash_key/1, hash_to_hex/1, read_keys/2, hash_keys/2]).

hash_key(Int) ->
    Bin = integer_to_binary(Int),
    Hash = crypto:hash(sha, Bin),
    HashInt = binary:decode_unsigned(Hash),
    TruncatedHash = HashInt rem (1 bsl 16),
    TruncatedHash.

hash_to_hex(HashInt) ->
    String = integer_to_list(HashInt, 16),
    LoweredString = string:to_lower(String),
    LoweredString.

read_keys(File, MaxLines) ->
    {ok, Binary} = file:read_file(File),
    Lines = binary:split(Binary, <<"\n">>, [global]),
    FilteredLines = lists:filter(fun(Line) -> Line =/= <<>> end, Lines),
    lists:sublist(FilteredLines, min(length(FilteredLines), MaxLines)).

hash_keys(File, MaxLines) ->
    Keys = read_keys(File, MaxLines),
    lists:map(fun(Key) ->
        {Int, _} = string:to_integer(Key),
        HashHex = hash_key(Int),
        HashHex
    end, Keys).

run(File, MaxLines) ->
    KeyHashes = hash_keys(File, MaxLines),
    lists:foreach(fun({Key, Hash}) ->
        io:format("Key: ~s, Hash: ~p~n", [Key, Hash])
    end, KeyHashes).