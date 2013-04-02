-module(test_resource).

-behaviour(gen_pool_resource).
%% API
-export([start_link/1,
	 status/1,
	 execute/2]).

-export([move/2,
	 '$get_traces'/0,
	 '$get_rollback_count'/1]).

-export([initializing/3,
	 online/2,
	 online/3,
	 offline/2,
	 offline/3]).

%% gen_fsm callbacks
-export([init/1, 
	 handle_event/3,
	 handle_sync_event/4, 
	 handle_info/3, 
	 terminate/3, 
	 code_change/4]).

-record(state, { pool_pid, ttl, transaction_pid, rollback_count = 0 }).


-include("../include/gen_pool.hrl").

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------

start_link(Configuration) ->
  gen_fsm:start_link(?MODULE, [Configuration], []).

%%--------------------------------------------------------------------

status(Connection) ->
  gen_fsm:sync_send_event(Connection,{command,external,status}).

%%--------------------------------------------------------------------

transaction(Connection,start) ->
  gen_fsm:sync_send_event(Connection,{command,external,{transaction,start,serializable}});

transaction(Connection,rollback) ->
  gen_fsm:sync_send_event(Connection,{command,external,{transaction,rollback}});

transaction(Connection,commit) ->
  gen_fsm:sync_send_event(Connection,{command,external,{transaction,commit}}).

transaction(Connection,start,_IsolationLevel) ->
  transaction(Connection,start).

%%--------------------------------------------------------------------

execute(Connection,{simple_query,QueryString,Parameters}) ->
  gen_fsm:sync_send_event(Connection,{command,external,{simple_query,QueryString,Parameters}}).

%%--------------------------------------------------------------------

move(PoolPid,StateName) ->
  gen_fsm:sync_send_event(PoolPid,{move,StateName}).

'$get_rollback_count'(Pid) ->
  gen_fsm:sync_send_event(Pid,get_rollback_count).

'$get_traces'() ->
  [].

%%====================================================================
%% gen_fsm callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, StateName, State} |
%%                         {ok, StateName, State, Timeout} |
%%                         ignore                              |
%%                         {stop, StopReason}                   
%% Description:Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/3,4, this function is called by the new process to 
%% initialize. 
%%--------------------------------------------------------------------
init([Configuration]) ->
  PoolPid    = Configuration#pool.pool_resource_manager_pid,
  Behaviour  = Configuration#pool.resource_behaviour,
  TimeToLive = case Behaviour of
		 undefined ->
		   undefined;
		 permanent ->
		   undefined;
		 temporary ->
		   pool_config:fget(Configuration,temporary_idle_ttl)
	       end,
  gen_pool_resource_manager:'$notify_resource_state'(PoolPid,started),

  {ok, initializing, #state{ pool_pid = PoolPid, ttl = TimeToLive }}.

%%--------------------------------------------------------------------

initializing({move,NextState}, _From, State) ->
  gen_pool_resource_manager:'$notify_resource_state'(State#state.pool_pid,NextState),
  case State#state.ttl of
    undefined ->
      io:format("initializing...\n"),
      {reply,ok,NextState,State};
    TTL ->
      io:format("initializing...\n"),
      io:format("** Test connection time to live: ~p\n",[TTL]),
      {reply,ok,NextState,State,TTL}
  end.

%%--------------------------------------------------------------------

online({command,external,status},_From,StateData) ->
  {reply,connected,online,StateData};

online({move,NextState}, _From, State) ->
  gen_pool_resource_manager:'$notify_resource_state'(State#state.pool_pid,NextState),
  case State#state.ttl of
    undefined ->
      {reply,ok,NextState,State};
    TTL ->
      io:format("** Test connection time to live: ~p\n",[TTL]),
      {reply,ok,NextState,State,TTL}
  end.


online(timeout,State) ->
  io:format("Timeout on ttl, stopping\n"),
  {stop,normal,State}.

offline(timeout,State) ->
  io:format("Timeout on ttl, stopping\n"),
  {stop,normal,State}.

offline({command,external,status},_From,StateData) ->
  {reply,connected,offline,StateData}.


%%--------------------------------------------------------------------

handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.

%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
  Reply = ok,
  {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
  {next_state, StateName, State}.

%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
  ok.

%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

goto(NextState,StateData) ->
  {next_state,NextState,StateData}.

%%--------------------------------------------------------------------
