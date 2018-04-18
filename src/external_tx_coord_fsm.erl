%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% @doc The coordinator for a given external transaction.

-module(external_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DC_META_UTIL, mock_partition_fsm).
-define(DC_UTIL, mock_partition_fsm).
-define(VECTORCLOCK, mock_partition_fsm).
-define(LOG_UTIL, mock_partition_fsm).
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(CLOCKSI_DOWNSTREAM, mock_partition_fsm).
-define(LOGGING_VNODE, mock_partition_fsm).
-define(PROMETHEUS_GAUGE, mock_partition_fsm).
-define(PROMETHEUS_COUNTER, mock_partition_fsm).

-else.
-define(DC_META_UTIL, dc_meta_data_utilities).
-define(DC_UTIL, dc_utilities).
-define(VECTORCLOCK, vectorclock).
-define(LOG_UTIL, log_utilities).
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(CLOCKSI_DOWNSTREAM, clocksi_downstream).
-define(LOGGING_VNODE, logging_vnode).
-define(PROMETHEUS_GAUGE, prometheus_gauge).
-define(PROMETHEUS_COUNTER, prometheus_counter).
-endif.


%% API
-export([start_link/3]).

%% Callbacks
-export([init/1,
    code_change/4,
    handle_event/3,
    handle_info/3,
    handle_sync_event/4,
    terminate/3,
    stop/1]).

%% States
-export([compute_snapshot/2,
         check_stable/2,
         reply/2]).

-record(state,
        {from, client_clock, properties, time}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pid(), clock_time() | ignore, txn_properties()) -> {ok, pid()}.
start_link(From, Clientclock, Properties) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock, Properties], []).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid, stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock, Properties]) ->
    {ok, compute_snapshot, init_state(From, ClientClock, Properties), 0}.

init_state(From, ClientClock, Properties) ->
    #state {
        client_clock = ClientClock,
        from = From,
        properties = Properties
    }.

compute_snapshot(timeout, S0=#state{client_clock=_ClientClock, properties=Properties}) ->
    Now = dc_utilities:now_microsec() - ?OLD_SS_MICROSEC,
    Extra = antidote:get_txn_property(error_sync, Properties),
    DcId = ?DC_META_UTIL:get_my_dc_id(),
    %Max = case dit:find(DcId, ClientClock) of
    %        {ok, LastClock} ->
    %            max(LastClock, Now);
    %        error ->
    %            Now
    %end,
    %Value = Max + Time,
    Value = Now + Extra,
    {ok, VecSnapshotTime} = ?DC_UTIL:get_stable_snapshot(),
    {ok, Min} = get_min(dict:to_list(VecSnapshotTime), DcId, infinity),
    case Min of
        infinity ->
            {next_state, reply, S0#state{time=Value}, 0};
        Smaller when Smaller < Value ->
            lager:info("Gotta wait ~p, the vector looked like ~p", [round((Value-Min)/1000), dict:to_list(VecSnapshotTime)]),
            {next_state, check_stable, S0#state{time=Value}, round((Value-Min)/1000)};
        _Larger ->
            {next_state, reply, S0#state{time=Value}, 0}
    end.

check_stable(timeout, S0=#state{time=Time}) ->
    DcId = ?DC_META_UTIL:get_my_dc_id(),
    {ok, VecSnapshotTime} = ?DC_UTIL:get_stable_snapshot(),
    case check(dict:to_list(VecSnapshotTime), Time, DcId) of
        true ->
            {next_state, reply, S0, 0};
        false ->
            lager:info("Extra wait of ~p, the vector looked like ~p", [?RETRY_CHECK_STABLE, dict:to_list(VecSnapshotTime)]),
            {next_state, check_stable, S0, ?RETRY_CHECK_STABLE}
    end.

reply(timeout, S0=#state{from=From}) ->
    Reply = {ok, ready},
    case is_pid(From) of
        false ->
            gen_fsm:reply(From, Reply);
        true ->
            From ! Reply
    end,
    {stop, normal, S0}.

%% =============================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(stop, _From, _StateName, StateData) ->
    {stop, normal, ok, StateData};

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%Internal operations
check([], _Value, _MyId) ->
    true;

check([{DcId, Clock}|Rest], Value, MyId) ->
    case DcId of
        MyId ->
            check(Rest, Value, MyId);
        _ ->
            case Clock < Value of
                true ->
                    false;
                false ->
                    check(Rest, Value, MyId)
            end
    end.

get_min([], _MyId, Min) ->
    {ok, Min};

get_min([{DcId, Clock}|Rest], MyId, Min) ->
    case DcId of
        MyId ->
            get_min(Rest, MyId, Min);
        _ ->
            get_min(Rest, MyId, min(Min, Clock))
    end.
    
-ifdef(TEST).

ckeck_test() ->
    R1 = check([], 7899, dc1),
    ?assertMatch(true, R1),
    R2 = check([{dc1,45},{dc2,42},{dc3,30}], 30, dc1),
    ?assertMatch(true, R2),
    R3 = check([{dc1,45},{dc2,42},{dc3,29}], 30, dc1),
    ?assertMatch(false, R3).

get_min_test() ->
    R1 = get_min([], dc1, infinity),
    ?assertMatch({ok, 0}, R1),
    R2 = get_min([{dc1,23}, {dc2,34}, {dc3,12}], dc1, infinity),
    ?assertMatch({ok, 12}, R2),
    R3 = get_min([{dc1,23}, {dc2,34}, {dc3,12}], dc3, infinity),
    ?assertMatch({ok, 23}, R3).

-endif.
