%%% @author Rudolph van Graan <rvg@wyemac.local>
%%% @copyright (C) 2013, Rudolph van Graan
%%% @doc
%%%
%%% @end
%%% Created : 15 Mar 2013 by Rudolph van Graan <rvg@wyemac.local>

-module(gen_pool_manager).


%%--------------------------------------------------------------------
-behaviour(gen_server).
%%--------------------------------------------------------------------
-define(CALLTIMEOUT,infinity).
%%--------------------------------------------------------------------
-include("../include/gen_pool.hrl").
%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-export([start_link/1,
	 start/1,
	 start/2,
	 start_link/2,
	 stop/1,
	 status/1,
	 acquire/1,
	 acquire/2,
	 get_allocated_resource/0,
	 get_dialect/1,
	 release/0,
	 is_allocated/2,
	 get_pending_allocation_count/1,
	 get_pending_allocations/1,
	 get_configuration/1]).
%%--------------------------------------------------------------------
%% API (internal)
%%--------------------------------------------------------------------
-export(['$expire_resource_allocation'/2,
	 '$get_resource_pool'/1,
	 '$get_resource'/2,
	 '$release_resource'/2,
	 '$notify_resource_state'/2]).
%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
-export([init/1, 
	 handle_call/3, 
	 handle_cast/2, 
	 handle_info/2,
	 terminate/2, 
	 code_change/3]).
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Configuration
%% name:                 string() The sqldb pool name is used to identify a group/pool of connections
%%                                of the same type and startup configuration.
%% connection_pool_pid:  pid()    The connection pool pid
%% pending_allocations:  term()   ets table keeping track of pending connection allocations
%%--------------------------------------------------------------------
-record(state, {name,
		type,
		debug,
		owner,
		pool_type,
		allocation_timeout,
		own_resource_pool,
		resource_pool_pid,
		pending_allocations}).

%%====================================================================
%% API
%%====================================================================

%%--------------------------------------------------------------------
%% Function: start_link(Configuration) -> {ok,Pid} | ignore | {error,Error}
%% Configuration: #gen_pool_configuration
%% 
%% Start a pool and start a connection pool automatically
%%--------------------------------------------------------------------

start_link(Config = #pool{}) ->
    Owner = self(), 
    gen_server:start_link(?MODULE, [Owner,Config], []).

start(Config = #pool{}) ->
    Owner = self(), 
    gen_server:start(?MODULE, [Owner,Config], []).

%%--------------------------------------------------------------------
%% Function: start_link(Configuration,ResourcePoolPid) -> {ok,Pid} | ignore | {error,Error}
%% ConnectionPool = Pid()
%% Configuration  = sqldb_configuration()
%%
%% Start a Pool for another running connection pool
%%--------------------------------------------------------------------

start_link(Config = #pool{} ,ResourcePool) ->
    Owner = self(), 
    gen_server:start_link(?MODULE, [Owner,Config,ResourcePool], []).

start(Config = #pool{},ResourcePool) ->
    Owner = self(), 
    gen_server:start(?MODULE, [Owner,Config,ResourcePool], []).

%%--------------------------------------------------------------------

stop(Pool) ->
    call_pool(Pool,stop,5000).

%%--------------------------------------------------------------------

status(PoolName) ->
    call_pool(PoolName,status,5000).

%%--------------------------------------------------------------------

acquire(PoolName) ->
    acquire(PoolName,?CALLTIMEOUT).

acquire(PoolName,Timeout) ->
    acquire(PoolName,{get_resource,Timeout},infinity).

%%--------------------------------------------------------------------

acquire(PoolName,Command,Timeout) ->
    do_check_already_allocated_resource(),
    case call_pool(PoolName,Command,Timeout) of
	{ok,Connection} ->
	    do_store_resource(PoolName,Connection);      
	{error,Reason} ->
	    throw(Reason)
    end.

%%--------------------------------------------------------------------

get_allocated_resource() ->
    case do_get_allocated_resource() of
	undefined ->
	    throw(no_resource);
	{_PoolName,Resource} ->
	    Resource
    end.

%%--------------------------------------------------------------------
%% Function: release() -> ok
%%--------------------------------------------------------------------

release() ->
    case do_get_allocated_resource() of
	undefined ->
	    ok;
	{PoolName,Resource} ->
	    do_clear_resource(),
	    call_pool(PoolName,{release_resource,Resource},?CALLTIMEOUT)
    end.

%%--------------------------------------------------------------------

get_configuration(PoolName) ->
    call_pool(PoolName,get_configuration,?CALLTIMEOUT).

%%--------------------------------------------------------------------

get_pending_allocation_count(Pool) ->
    call_pool(Pool,get_pending_allocation_count,?CALLTIMEOUT).

get_pending_allocations(Pool) ->
    call_pool(Pool,get_pending_allocations,?CALLTIMEOUT).

%%--------------------------------------------------------------------

is_allocated(Pool,{_Type,Pid} = Resource) when is_pid(Pid)->
    call_pool(Pool,{is_allocated,Resource},?CALLTIMEOUT);
is_allocated(_PoolName,_Resource) ->
    exit(badarg).


get_dialect(PoolName) ->
    call_pool(PoolName,get_dialect,?CALLTIMEOUT).

%%====================================================================
%% Resource Management API (Internal)
%%====================================================================

'$expire_resource_allocation'(PoolPid,CallerPid) ->
    ok = gen_server:call(PoolPid,{expire_resource_allocation,CallerPid},?CALLTIMEOUT).

%%--------------------------------------------------------------------

'$get_resource'(PoolName,Timeout) ->
    call_pool(PoolName,{get_resource,Timeout},?CALLTIMEOUT).

%%--------------------------------------------------------------------

'$notify_resource_state'(PoolPid,State) ->
    gen_server:cast(PoolPid,{notify_resource_state,State}).

%%--------------------------------------------------------------------
%% Returns the PID of the connection pool managed by this pool

'$get_resource_pool'(PoolName) ->
    call_pool(PoolName,get_resource_pool,?CALLTIMEOUT).

'$release_resource'(PoolName,{_Type,Pid} = Resource) when is_pid(Pid)->
    call_pool(PoolName,{release_connection,Resource},?CALLTIMEOUT).

%%====================================================================

call_pool(Name,Message,Timeout) ->
    try
	case proc_reg:where({pool,Name}) of
	    undefined ->
		throw(unknown_pool);
	    Pid ->
		gen_server:call(Pid,Message,Timeout)
	end
    catch
	throw:unknown_pool ->
	    %% Hack 
	    error_logger:warning_report([{reason,"Pool does not exist"},				   
					 {name,  Name},
					 {message,Message},
					 {stack,erlang:get_stacktrace()}
					]),
	    throw(unknown_pool)
    end.



%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
init([Owner,Config = #pool{}]) ->
    {ok,ResourcePool} = gen_pool_resource_manager:start_link(Config),
    %%  log(status,"Connection pool ~p started",[ConnectionPool],#state{debug=true}),
    Config2 = Config#pool{resource_pool_pid = ResourcePool},
    init([Owner,Config2,ResourcePool,true]);

init([Owner,Configuration,ResourcePool]) ->
    init([Owner,Configuration,ResourcePool,_OwnTheResourcePool = false]);
init([Owner,Configuration,ResourcePool,OwnTheResourcePool]) ->
    process_flag(trap_exit,true),
    %%  log(status,"Connection pool ~p started",[ConnectionPool],#state{debug=true}),

    MyPid = self(),

    Name = Configuration#pool.name, 
    true = proc_reg:reg({pool,Name},MyPid),

    F = fun(State) ->
		gen_pool_manager:'$notify_resource_state'(MyPid,State)
	end,
    %%  log(status,"Registering completion for connection pool ~p",[ConnectionPool],#state{debug=true}),
    ok = gen_pool_resource_manager:'$register_completion'(ResourcePool,F),

    %%  log(status,"Started",[],#state{debug=true}),
 
    {ok, #state{ name                = Name,
		 own_resource_pool   = OwnTheResourcePool,
		 pool_type           = Configuration#pool.pool_type,
		 allocation_timeout  = Configuration#pool.allocation_timeout,
		 debug               = Configuration#pool.debug,
		 resource_pool_pid   = ResourcePool,
		 owner               = Owner,
		 pending_allocations = ets:new(pending_allocations, [ordered_set,protected]) }}.

%%--------------------------------------------------------------------
handle_call(get_configuration, _From, State) ->
    {ok,Configuration} = gen_pool_resource_manager:get_configuration(State#state.resource_pool_pid),
    {reply,{ok,Configuration},State};

handle_call(get_connection_pool, _From, State) ->
    {reply,{ok,State#state.resource_pool_pid},State};

handle_call(get_resource, From, State) ->
    do_get_resource(From,State#state.allocation_timeout,State);

handle_call({get_connection,Timeout}, From, State) ->
    do_get_resource(From,Timeout,State);

%%--------------------------------------------------------------------

handle_call({release_resource,Resource}, From, State) ->
    do_release_resource(Resource,From,State);

%%--------------------------------------------------------------------

handle_call({is_allocated,Resource}, From, State) ->
    do_is_allocated(Resource,From,State);

%%--------------------------------------------------------------------

handle_call({expire_resource_allocation,CallerPid}, _From, State) ->
    do_expire_resource_allocation(CallerPid,State),
    {reply,ok,State};


handle_call(get_pending_allocation_count,_From,State) ->
    Size = ets:info(State#state.pending_allocations,size),
    {reply,Size,State};

handle_call(get_pending_allocations,_From,State) ->
    PendingAllocations = ets:tab2list(State#state.pending_allocations),
    {reply,PendingAllocations,State};

%%--------------------------------------------------------------------

handle_call(stop, From, State) ->
    %% TODO return control to waiting functions
    gen_server:reply(From,ok),
    {stop,normal,State}.

%%--------------------------------------------------------------------

handle_cast({notify_resource_state,State}, StateData) ->
    case State of
	online ->
	    do_complete_pending_request(StateData);
	_OtherState ->
	    {noreply,StateData}
    end.

%%--------------------------------------------------------------------
handle_info({'EXIT',PID,Reason}, State) when PID =:= State#state.resource_pool_pid->
    error_logger:error_report([{error,"POOL - Resource Pool process died"},
			       {module,?MODULE},
			       {connection_pool_pid,PID},
			       {reason,Reason},
			       {state,State}]),
    {stop,normal,State};
handle_info({'EXIT',State = #state{owner=_Owner},shutdown}, State) ->
    {stop,normal,State};
handle_info({'EXIT',PID,_Reason}, State) ->
    do_expire_resource_allocation(PID,State),
    {noreply,State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%%--------------------------------------------------------------------

terminate(Reason, State) when State#state.own_resource_pool == true->
    io:format("[POOL] [~s] ~p pool ~s shutting down with reason ~w\n",["Pool",self(),State#state.name,Reason]),
    ConnPool = State#state.resource_pool_pid,
    Mon = erlang:monitor(process,ConnPool),
    exit(ConnPool,shutdown),
    receive
	{'DOWN', Mon, _, _, _} ->
	    ok
    end,
    io:format("[POOL] [~s] ~p pool ~s shut down\n",["Pool",self(),State#state.name]),

    log(status,"terminate reason: ~p",[Reason],State);
terminate(Reason, State) ->
    io:format("[POOL] [~s] ~p pool ~s shutting down with reason ~w (Resource pool is not owned!)\n",["Pool",self(),State#state.name,Reason]),
    log(status,"terminate reason: ~p",[Reason],State).

%%--------------------------------------------------------------------

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

do_get_resource(CallerFrom,Timeout,State) ->
    {ClientPid,_Ref} = CallerFrom,
    case { gen_pool_resource_manager:get_resource(State#state.resource_pool_pid,ClientPid), has_pending_allocations(State) } of
	{{error,no_available_resource},_HasPending} ->
	    log(error,"[~p] no available resource",[State#state.name],State),
	    {reply,{error,no_available_resource},State};

	{{error,resource_pending},_HasPending} ->
	    do_pend_allocation_request(CallerFrom,Timeout,State);

	{{ok,Resource},_HasPending = false} ->
	    log(info,"[~p] resource allocated for ~p",[State#state.name,CallerFrom],State),
	    gen_server:reply(CallerFrom,{ok,{State#state.type,Resource}}),      
	    {noreply,State};

	{{ok,Resource},_HasPending = true} ->
	    do_pend_allocation_request(CallerFrom,Timeout,State),      
	    do_complete_pending_request(Resource,State)

    end.

%%--------------------------------------------------------------------

do_release_resource({_Type,Resource},From,State) ->
    log(info,"[~p] releasing resource from ~p",[State#state.name,From],State),

    ok = gen_pool_resource_manager:release_resource(State#state.resource_pool_pid,Resource),  
    gen_server:reply(From,ok),
    do_complete_pending_request(State).

%%--------------------------------------------------------------------

do_is_allocated(Resource,_From,State) ->
    Result = gen_pool_resource_manager:is_online(State#state.resource_pool_pid, Resource),
    {reply,Result,State}.

%%--------------------------------------------------------------------

do_pend_allocation_request(CallerFrom = {FromPid,_FromRef},infinity,State) ->
    link(FromPid),
    true = ets:insert_new(State#state.pending_allocations,{FromPid,CallerFrom,{date(),time()}}),
    log(info,"[~p] pending resource allocation for ~p (count: ~p)",[State#state.name,CallerFrom,get_pending_count(State)],State),
    {noreply,State};

do_pend_allocation_request(CallerFrom = {FromPid,_FromRef},Timeout,State) ->
    {ok,_ExpiryTimerRef} = fixtimer:apply_after(Timeout,?MODULE,'$expire_connection_allocation',[self(),FromPid]),
    link(FromPid),
    true = ets:insert_new(State#state.pending_allocations,{FromPid,CallerFrom,{date(),time()}}),
    log(info,"[~p] pending connection allocation for ~p (count: ~p)",[State#state.name,CallerFrom,get_pending_count(State)],State),
    {noreply,State}.

%%--------------------------------------------------------------------

do_expire_resource_allocation(CallerPid,State) ->
    case ets:lookup(State#state.pending_allocations,CallerPid) of
	[] ->
	    ok;
	[{FromPid,CallerFrom,_TS}] ->
	    true = ets:delete(State#state.pending_allocations,FromPid),
	    log(info,"[~p] expiring resource allocation for ~p (count: ~p)",[State#state.name,CallerFrom,get_pending_count(State)],State),
	    gen_server:reply(CallerFrom,{error,?R_RESOURCE_UNAVAILABLE}),
	    ok
    end.

%%--------------------------------------------------------------------

do_complete_pending_request(State) ->
    case ets_select(State#state.pending_allocations,[{{'$1','$2','$3'},[],['$_']}],1) of
	[] ->
	    log(info,"no pending requests to complete",[],State),
	    ok; 
	[{FromPid,CallerFrom,_TS}] ->
	    case gen_pool_resource_manager:get_connection(State#state.resource_pool_pid) of
		{ok,Resource} ->
		    log(info,"[~p] completing pending resource allocation for ~p (pending count: ~p)",[State#state.name,CallerFrom,get_pending_count(State)],State),
		    true = ets:delete(State#state.pending_allocations,FromPid),   
		    gen_server:reply(CallerFrom,{ok,{State#state.type,Resource}});
		{error,?R_RESOURCE_PENDING} ->
		    ok;
		{error,?R_RESOURCE_UNAVAILABLE} ->
		    ok
	    end
    end,
    {noreply,State}.

%%--------------------------------------------------------------------

do_complete_pending_request(Connection,State) ->
    [{FromPid,CallerFrom,_TS}] = ets_select(State#state.pending_allocations,[{{'$1','$2','$3'},[],['$_']}],1),
    true = ets:delete(State#state.pending_allocations,FromPid),
    log(info,"[~p] completing pending resource allocation for ~p (pending count: ~p)",[State#state.name,CallerFrom,get_pending_count(State)],State),
    gen_server:reply(CallerFrom,{ok,{State#state.type,Connection}}),
    {noreply,State}.

%%--------------------------------------------------------------------

%%--------------------------------------------------------------------

ets_select(Table,MatchSpec,Count) ->
    case ets:select(Table,MatchSpec,Count) of
	'$end_of_table' ->
	    [];
	{Results,_C} ->
	    Results
    end.

%%--------------------------------------------------------------------

has_pending_allocations(State) ->
    get_pending_count(State) > 0.

%%--------------------------------------------------------------------

get_pending_count(State) ->
    ets:info(State#state.pending_allocations,size).

%%--------------------------------------------------------------------

log(Type,Message,Parameters,State) ->
    debug(Type,Message,Parameters,State).

%%--------------------------------------------------------------------

debug(Type,Message,Parameters,State) when State#state.debug == true ->
    io:format("[POOL] [~p] ~p "++Message++"\n",[Type,self()|Parameters]);
debug(_Type,_Message,_Parameters,_State) ->
    ok.

%%--------------------------------------------------------------------

do_check_already_allocated_resource() ->
    case gdb:in_transaction() of
	true  -> throw(?E_MNESIA_TRANSACTION_ACTIVE);
	false -> ok
    end,
    case do_get_allocated_resource() of
	undefined ->
	    ok;
	_Else ->
	    throw(?E_RESOURCE_ALREADY_ALLOCATED)
    end.

do_get_allocated_resource() ->
    get(?POOL_RESOURCE).

do_store_resource(PoolName,Resource) ->
    put(?POOL_RESOURCE,{PoolName,Resource}),
    ok.

do_clear_resource() ->
    put(?POOL_RESOURCE,undefined),
    ok.

%%--------------------------------------------------------------------
