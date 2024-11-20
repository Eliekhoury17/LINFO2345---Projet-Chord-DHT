-module(node_logic).
-record(node, {id, successor, predecessor, keys = []}).
-import(lists, [sort/1, map/2, zip/2, nth/2, seq/2]).
-export([create_node/1, set_successor/2, set_predecessor/2, add_keys/2, create_ring/1]).

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
add_keys(Node, KeyList) ->
    Node#node{keys = Node#node.keys ++ KeyList}.


-spec create_ring(list(integer())) -> list(#node{}).
create_ring(NodeIds) ->
    SortedIds = lists:sort(NodeIds),
    IndexedIds = lists:zip(lists:seq(1, length(SortedIds)), SortedIds),
    lists:map(fun({Index, Id}) ->
        Predecessor = if Index == 1 -> lists:nth(length(SortedIds), SortedIds);
                        true -> lists:nth(Index - 1, SortedIds) end,
        Successor = if
            Index == length(SortedIds) -> lists:first(SortedIds);
            true -> lists:nth(Index + 1, SortedIds)
        end,
        Node = create_node(Id),
        Node = set_predecessor(Node, Predecessor),
        set_successor(Node, Successor)
    end, IndexedIds).
