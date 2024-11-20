-module(part1).
-import(lists, [member/2, nth/2, nthtail/2, append/2, delete/2, reverse/1]).
-import(crypto, [hash/2]).
-record(node, {id, successor, predecessor, keys}).


hash_key(Key) ->
    <<Hash:16, _Rest/binary>> = crypto:hash(sha, Key),
    Hash.

read_keys(File, MaxLines) ->
    {ok, Binary} = file:read_file(File),
    % Split into lines
    Lines = binary:split(Binary, <<"\n">>, [global]),
    % Convert each line to a string and filter out empty lines
    FilteredLines = lists:filter(fun(Line) -> Line =/= <<>> end, Lines),
    % Take only the specified number of lines
    lists:sublist(FilteredLines, min(length(FilteredLines), MaxLines)).

hash_keys(File, MaxLines) ->
    Keys = read_keys(File, MaxLines),
    % Map each key to its hash
    lists:map(fun(Key) -> {binary_to_list(Key), hash_key(Key)} end, Keys).

run(File, MaxLines) ->
    KeyHashes = hash_keys(File, MaxLines),
    % Print key and hash
    lists:foreach(fun({Key, Hash}) ->
        io:format("Key: ~s, Hash: ~p~n", [Key, Hash])
    end, KeyHashes).