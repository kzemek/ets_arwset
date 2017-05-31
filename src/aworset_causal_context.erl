-module(aworset_causal_context).

-record(aworset_causal_context, {
    is_delta = false :: boolean(),
    compact :: integer() | #{actor() => counter()},
    cloud :: integer() | ordsets:ordset(dot())
}).

-type actor() :: term().
-type counter() :: non_neg_integer().
-type dot() :: {actor(), counter()}.
-type causal_context() :: #aworset_causal_context{is_delta :: false}.
-type causal_context_delta() :: #aworset_causal_context{is_delta :: true}.
-type causal_context_any() :: #aworset_causal_context{}.

-export_types([actor/0, counter/0, dot/0, causal_context/0,
               causal_context_delta/0, causal_context_any/0]).
-export([new/0, new_delta/0, dotin/2, join/2, insert_dot/2, insert_dot_nocompact/2,
         compact/1, makedot/2]).

-spec new() -> causal_context().
new() ->
    Compact = ets:new(compact, [public]),
    Cloud = ets:new(cloud, [public, ordered_set]),
    #aworset_causal_context{compact = Compact, cloud = Cloud}.

-spec new_delta() -> causal_context_delta().
new_delta() ->
    #aworset_causal_context{is_delta = true, compact = #{}, cloud = ordsets:new()}.

-spec dotin(causal_context_any(), dot()) -> boolean().
dotin(#aworset_causal_context{compact = CC, cloud = DC}, {Actor, Counter} = Dot) ->
    case aworset_utils:map_get(CC, Actor, -1) of
        C when C >= Counter -> true;
        _ -> aworset_utils:set_member(DC, Dot)
    end.

-spec compact(causal_context()) -> causal_context();
             (causal_context_delta()) -> causal_context_delta().
compact(State = #aworset_causal_context{compact = CC, cloud = DC}) ->
    {NewCC, ReverseRemoveFromDC} =
        aworset_utils:set_fold(
          DC, {CC, []},
          fun({Actor, Counter} = Dot, {OldCC, OldRemoveDC}) ->
                  case aworset_utils:map_get(OldCC, Actor, 0) of
                      C when C =:= Counter - 1 ->
                          OldCC1 = aworset_utils:map_put(OldCC, Actor, Counter),
                          {OldCC1, [Dot | OldRemoveDC]};
                      _ ->
                          {OldCC, OldRemoveDC}
                  end
          end),

    RemoveFromDC = lists:reverse(ReverseRemoveFromDC),
    NewDC = aworset_utils:set_subtract(DC, RemoveFromDC),
    State#aworset_causal_context{compact = NewCC, cloud = NewDC}.

-spec makedot(causal_context(), actor()) -> dot().
makedot(#aworset_causal_context{compact = CC}, Actor) ->
    NewCounter = ets:update_counter(CC, Actor, {2, 1}, {Actor, 0}),
    {Actor, NewCounter}.

-spec insert_dot(causal_context(), dot()) -> causal_context();
                 (causal_context_delta(), dot()) -> causal_context_delta().
insert_dot(State, Dot) ->
    State1 = insert_dot_nocompact(State, Dot),
    compact(State1).

-spec insert_dot_nocompact(causal_context(), dot()) -> causal_context();
                          (causal_context_delta(), dot()) -> causal_context_delta().
insert_dot_nocompact(State = #aworset_causal_context{cloud = DC}, Dot) ->
    DC1 = aworset_utils:set_put(DC, Dot),
    State#aworset_causal_context{cloud = DC1}.

-spec join(This :: causal_context(), Other :: causal_context_delta()) ->
                  causal_context();
          (This :: causal_context_delta(), Other :: causal_context_delta()) ->
                  causal_context_delta().
join(This = #aworset_causal_context{is_delta = TDelta, compact = TCC},
     #aworset_causal_context{is_delta = ODelta, compact = OCC, cloud = ODC})
  when TDelta orelse ODelta ->
    NewTCC = aworset_utils:map_fold(
               OCC, TCC,
               fun(OActor, OCounter, OldTCC) ->
                       case aworset_utils:map_get(OldTCC, OActor, -1) of
                           Counter when Counter > OCounter -> OldTCC;
                           Counter -> aworset_utils:map_put(OldTCC, OActor, Counter)
                       end
               end),

    NewState0 = This#aworset_causal_context{compact = NewTCC},
    NewState1 = aworset_utils:set_fold(
                  ODC, NewState0, fun(Dot, T) -> insert_dot_nocompact(T, Dot) end),
    compact(NewState1).

