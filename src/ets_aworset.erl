-module(ets_aworset).

-record(ets_aworset, {
    state :: integer(),
    causal_context :: arwset_causal_context:causal_context(),
    references :: integer(),
    view :: integer(),
    counters :: integer()
}).

-record(ets_aworset_delta, {
    state :: #{dot() => term()},
    causal_context :: arwset_causal_context:causal_context_delta()
}).

-type actor() :: arwset_causal_context:actor().
-type dot() :: arwset_causal_context:dot().
-type aworset() :: #ets_aworset{}.
-type aworset_delta() :: #ets_aworset_delta{}.
-type object() :: term().

-export([new/0, lookup/2, join/2, insert/3, delete/2]).

-spec new() -> aworset().
new() ->
    State = ets:new(state, [public, {read_concurrency, true},
                            {write_concurrency, true}]),
    View = ets:new(view, [public, {read_concurrency, true}]),
    Counters = ets:new(counters, [public, {write_concurrency, true}]),
    References = ets:new(references, [bag, public, {read_concurrency, true}]),
    CausalContext = aworset_causal_context:new(),
    #ets_aworset{state = State, view = View, references = References,
                 causal_context = CausalContext, counters = Counters}.

-spec lookup(aworset(), Key :: term()) -> term().
lookup(#ets_aworset{view = View}, Key) ->
    ets:lookup(View, Key).

-spec join(aworset(), aworset_delta()) -> aworset();
          (aworset_delta(), aworset_delta()) -> aworset_delta().
join(This = #ets_aworset{state = State, causal_context = CC},
     _Other = #ets_aworset_delta{state = OState, causal_context = OCC}) ->
    {ToAdd, ToDelete, NewCC} = do_join(State, CC, OState, OCC),
    [insert_dot(This, Dot, Value) || {Dot, Value} <- ToAdd],
    [delete_dot(This, Dot, Value) || {Dot, Value} <- ToDelete],
    This#ets_aworset{causal_context = NewCC};
join(This = #ets_aworset_delta{state = State, causal_context = CC},
     _Other = #ets_aworset_delta{state = OState, causal_context = OCC}) ->
    {ToAdd, ToDelete, NewCC} = do_join(State, CC, OState, OCC),
    NewState0 = lists:foldl(fun({D, _}, S) -> aworset_utils:map_delete(S, D) end,
                            State, ToDelete),
    NewState = lists:foldl(fun({D, V}, S) -> aworset_utils:map_put(S, D, V) end,
                           NewState0, ToAdd),
    This#ets_aworset_delta{state = NewState, causal_context = NewCC}.

do_join(State, CC, OState, OCC) ->
    %% TODO: We're looking through the whole ETS here - this is obviously
    %% going to be slow when the state is big. An optimization to consider
    %% here is sending tombstones in deltas. This looks to be a simple
    %% tradeoff between network usage (explicitly marking deleted elements
    %% as opposed to sometimes folding them into compact causal context)
    %% and processing power (looking through the whole state on each delta).
    %% The tombstones and adds should cancel each other when joining deltas
    %% that happen to contain both.
    ToDelete =
        aworset_utils:map_fold(
          State, [],
          fun(Dot, Value, Acc) ->
                  TheyKnowBetter = (not aworset_utils:map_member(OState, Dot))
                      andalso aworset_causal_context:dotin(OCC, Dot),
                  case TheyKnowBetter of
                      false -> Acc;
                      true -> [{Dot, Value} | Acc]
                  end
          end),

    ToAdd =
        aworset_utils:map_fold(
          OState, [],
          fun(Dot, Value, Acc) ->
                  TheyKnow = (not aworset_utils:map_member(State, Dot))
                      andalso (not aworset_causal_context:dotin(CC, Dot)),
                  case TheyKnow of
                      false -> Acc;
                      true -> [{Dot, Value} | Acc]
                  end
          end),

    NewCC = aworset_causal_context:join(CC, OCC),
    {ToAdd, ToDelete, NewCC}.

-spec insert(aworset(), Object :: term(), actor()) -> aworset_delta().
insert(This = #ets_aworset{causal_context = CC}, Object, Actor) ->
    Dot = aworset_causal_context:makedot(CC, Actor),
    insert_dot(This, Dot, Object),
    make_delta(Dot, Object).

-spec insert_dot(aworset(), dot(), Object :: term()) -> any().
insert_dot(#ets_aworset{state = State, references = References, view = View,
                        counters = Counters},
           Dot, Object) ->
    ets:insert(State, {Dot, Object}),
    ets:insert(References, {Object, Dot}),
    case ets:update_counter(Counters, Object, {2, 1}, {[], 0}) of
        1 -> ets:insert(View, Object);
        _ -> ok
    end.

-spec delete_dot(aworset(), dot(), Object :: term()) -> any().
delete_dot(#ets_aworset{state = State, references = References, view = View,
                        counters = Counters},
           Dot, Object) ->
    ets:delete(State, Dot),
    ets:delete_object(References, {Object, Dot}),
    case ets:update_counter(Counters, Object, {2, -1}) of
        0 ->
            ets:delete(Counters, {counter, Object}),
            ets:delete_object(View, Object);
        _ ->
            ok
    end.

-spec delete(aworset(), Object :: term()) -> aworset_delta().
delete(This = #ets_aworset{references = References}, Object) ->
    Dots = [D || {_, D} <- ets:lookup(References, Object)],
    DeltaCC1 =
        lists:foldl(
          fun(Dot, DeltaCC0) ->
                  delete_dot(This, Dot, Object),
                  aworset_causal_context:insert_dot_nocompact(DeltaCC0, Dot)
          end,
          aworset_causal_context:new_delta(),
          Dots),
    DeltaCC = aworset_causal_context:compact(DeltaCC1),
    #ets_aworset_delta{state = #{}, causal_context = DeltaCC}.

-spec make_delta(dot(), Object :: term()) -> aworset_delta().
make_delta(Dot, Object) ->
    CC0 = aworset_causal_context:new_delta(),
    CC = aworset_causal_context:insert_dot(CC0, Dot),
    #ets_aworset_delta{state = maps:from_list([{Dot, Object}]),
                       causal_context = CC}.

