-module(node_logic).
-record(node, {id, successor, predecessor, keys = []}).
-import(lists, [sort/1, map/2, zip/2, nth/2, seq/2, first/2]).
-import(hashing, [run/2, hash_key/1, read_keys/2, hash_keys/2]).
-export([create_node/1, set_successor/2, set_predecessor/2, add_keys/2, create_ring/1, run_assignment/3]).

-spec create_node(integer()) -> #node{}.
create_node(Id) ->
    #node{id = Id, successor = undefined, predecessor = undefined}.


-spec set_successor(#node{}, integer()) -> #node{}.
set_successor(Node, SuccessorId) ->
    Node#node{successor = SuccessorId}.


-spec set_predecessor(#node{}, integer()) -> #node{}.
set_predecessor(Node, PredecessorId) ->
    Node#node{predecessor = PredecessorId}.


-spec add_keys(#node{}, list()) -> #node{}.
add_keys(Node, Key) ->
    Node#node{keys = [Key | Node#node.keys]}.

-spec create_ring(list(integer())) -> list(#node{}).
create_ring(NodeIds) ->
    SortedIds = lists:sort(lists:map(fun hashing:hash_key/1, NodeIds)),
    Len = length(SortedIds),
    lists:map(fun(Index) ->
        Id = lists:nth(Index, SortedIds),
        PredIndex = (Index - 2 + Len) rem Len + 1,
        SuccIndex = Index rem Len + 1,
        Predecessor = lists:nth(PredIndex, SortedIds),
        Successor = lists:nth(SuccIndex, SortedIds),
        Node = create_node(Id),
        Node1 = set_predecessor(Node, Predecessor),
        set_successor(Node1, Successor)
    end, lists:seq(1, Len)).


-spec run_assignment(list(integer()), string(), integer()) -> list(#node{}).
run_assignment(NodeIds, File, MaxLines) ->
    Nodes = lists:sort(create_ring(NodeIds)),
    KeyHashes = lists:sort(hashing:hash_keys(File, MaxLines)),
    assign_keys_to_nodes(Nodes, KeyHashes,0,0).

-spec assign_keys_to_nodes(list(#node{}), list({list(), integer()}), integer(), integer()) -> list(#node{}).
assign_keys_to_nodes(Nodes, KeyHashes, N, K) ->
    LenNodes = length(Nodes),
    LenKeyHashes = length(KeyHashes),
    case {N >= LenNodes, K >= LenKeyHashes} of
        {true, _} -> Nodes;
        {_, true} -> Nodes;
        _ ->
            Node = lists:nth(N + 1, Nodes),
            {Key, Hash} = lists:nth(K+1, KeyHashes),
            case {Node#node.predecessor < Hash, Hash < Node#node.id} of
                {true, true} ->
                    UpdatedNode = add_keys(Node, Hash),
                    UpdatedNodes = lists:sublist(Nodes, N) ++ [UpdatedNode] ++ lists:nthtail(N + 1, Nodes),
                    assign_keys_to_nodes(UpdatedNodes, KeyHashes, N, K + 1);
                _ ->
                    assign_keys_to_nodes(Nodes, KeyHashes, N + 1, K)
            end
    end.


    

    


