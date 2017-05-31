-module(aworset_utils).

-export([map_get/3, map_member/2, map_put/3, map_delete/2, map_fold/3]).
-export([set_member/2, set_put/2, set_delete/2, set_fold/3, set_subtract/2]).

map_get(Map, Key, Default) when is_map(Map) ->
    maps:get(Key, Map, Default);
map_get(Ets, Key, Default) ->
    case ets:lookup(Ets, Key) of
        [] -> Default;
        [{_, Value}] -> Value
    end.

map_put(Map, Key, Value) when is_map(Map) ->
    maps:put(Key, Value, Map);
map_put(Ets, Key, Value) ->
    ets:insert(Ets, {Key, Value}),
    Ets.

map_delete(Map, Key) when is_map(Map) ->
    maps:remove(Key, Map);
map_delete(Ets, Key) ->
    ets:delete(Ets, Key).

map_fold(Map, Acc0, Fun) when is_map(Map) ->
    maps:fold(Fun, Acc0, Map);
map_fold(Ets, Acc0, Fun) ->
    ets:foldl(fun({Key, Value}, Acc) -> Fun(Key, Value, Acc) end,
              Acc0, Ets).

map_member(Map, Key) when is_map(Map) ->
    maps:is_key(Key, Map);
map_member(Ets, Key) ->
    [] =/= ets:lookup(Ets, Key).

set_member(Ordset, Object) when is_list(Ordset) ->
    ordsets:is_element(Object, Ordset);
set_member(Ets, Object) ->
    [] =/= ets:lookup(Ets, Object).

set_put(Ordset, Object) when is_list(Ordset) ->
    ordsets:add_element(Object, Ordset);
set_put(Ets, Object) ->
    ets:insert(Ets, {Object}),
    Ets.

set_delete(Ordset, Object) when is_list(Ordset) ->
    ordsets:del_element(Object, Ordset);
set_delete(Ets, Object) ->
    ets:delete_object(Ets, Object),
    Ets.

set_fold(Ordset, Acc0, Fun) when is_list(Ordset) ->
    ordsets:fold(Fun, Acc0, Ordset);
set_fold(Ets, Acc0, Fun) ->
    ets:foldl(Fun, Acc0, Ets).

set_subtract(OrdsetA, OrdsetB) when is_list(OrdsetA), is_list(OrdsetB) ->
    ordsets:subtract(OrdsetA, OrdsetB);
set_subtract(Ets, OrdsetB) when is_list(OrdsetB) ->
    [ets:delete(Ets, Object) || Object <- OrdsetB],
    Ets.

