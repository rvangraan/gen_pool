%%% @author Rudolph van Graan <rvg@wyemac.local>
%%% @copyright (C) 2013, Rudolph van Graan
%%% @doc
%%% This is 
%%% @end
%%% Created : 15 Mar 2013 by Rudolph van Graan <rvg@wyemac.local>

-module(gen_pool_resource_manager).

%%--------------------------------------------------------------------

-behaviour(gen_server).

-include("../include/gen_pool.hrl").
-define(STATE_UNDEFINED,2).
-define(STATE_UP,1).
-define(STATE_DOWN,0).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-export([start_link/1,
	 start/1,
	 get_resource/1,
	 get_resource/2,
	 get_configuration/1,
	 release_resource/2,
	 release_resource/3,
	 resource_count/1,
	 is_online/2]).

%%--------------------------------------------------------------------
%% Resource API

-export([resource_change_state/2]).

%%--------------------------------------------------------------------

-export(['$notify_resource_state'/2,
	 '$get_all_resources'/1,
	 '$register_completion'/2,
	 '$monitor_resource_client'/3]).

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

-record(state, { configuration,
		 min_pool_size,
		 max_pool_size,
		 behaviour_mod,
		 resource_sup,
		 notify_completion,
		 debug = false,
		 resource_pool_pid,
		 behaviour_state,
		 online_count = 0,
		 resources_table = [] }).

%%====================================================================
%% API
%%====================================================================

where(GroupName) when is_atom(GroupName) ->
    gproc:where({n,l,{pool_manager, GroupName}});
where(Pid) when is_pid(Pid) ->
    Pid.

%%--------------------------------------------------------------------

start_link(Configuration) ->
    Owner = self(),
    gen_server:start_link(?MODULE, [Owner,Configuration], []).

start(Configuration) ->
    Owner = self(),
    gen_server:start(?MODULE, [Owner,Configuration], []).

%%--------------------------------------------------------------------

get_resource(GroupNameOrPid) when is_atom(GroupNameOrPid); is_pid(GroupNameOrPid) ->
    get_resource(where(GroupNameOrPid),self()).

get_resource(GroupNameOrPid,ClientPID) when is_atom(GroupNameOrPid); is_pid(GroupNameOrPid),
				       is_pid(ClientPID) ->
    gen_server:call(where(GroupNameOrPid),{get_resource,ClientPID},infinity).

get_configuration(GroupNameOrPid) when is_atom(GroupNameOrPid); is_pid(GroupNameOrPid) ->
    gen_server:call(where(GroupNameOrPid),get_configuration).

%%----------------
release_resource(GroupNameOrPid,Resource) when is_atom(GroupNameOrPid); is_pid(GroupNameOrPid) ->
    gen_server:call(where(GroupNameOrPid),{release_resource,Resource},infinity).

release_resource(GroupNameOrPid,Resource,WatcherPid) when is_atom(GroupNameOrPid); is_pid(GroupNameOrPid) ->
    gen_server:call(where(GroupNameOrPid),{release_resource,Resource,WatcherPid},infinity).



%%--------------------------------------------------------------------
%% Function: resource_count(GroupNameOrPid) -> {ok,ResourceCount}
%%--------------------------------------------------------------------
%%   ResourceCount = integer() The number of resource processes
%%                               currently started.
%%--------------------------------------------------------------------

resource_count(GroupNameOrPid) when is_atom(GroupNameOrPid); is_pid(GroupNameOrPid) ->
    gen_server:call(where(GroupNameOrPid),resource_count,infinity).

%%--------------------------------------------------------------------
%% Function: is_online(GroupName,Resource) -> true | false
%%--------------------------------------------------------------------

is_online(GroupNameOrPid,Resource) when is_atom(GroupNameOrPid); is_pid(GroupNameOrPid) ->
    gen_server:call(where(GroupNameOrPid),{is_state,online,Resource},infinity).

%%====================================================================
%% API (Internal)
%%====================================================================

'$register_completion'(GroupNameOrPid,CompletionFun) when is_atom(GroupNameOrPid); is_pid(GroupNameOrPid) ->
    gen_server:call(where(GroupNameOrPid),{register_completion,CompletionFun},infinity).

%%--------------------------------------------------------------------

'$notify_resource_state'(GroupNameOrPid,Status) when is_atom(GroupNameOrPid); is_pid(GroupNameOrPid) ->
    gen_server:cast(where(GroupNameOrPid),{notify_resource_state,Status,self()}).

resource_change_state(GroupNameOrPid,Status) when is_atom(GroupNameOrPid); is_pid(GroupNameOrPid) ->
    '$notify_resource_state'(where(GroupNameOrPid),Status).

%%--------------------------------------------------------------------

'$get_all_resources'(GroupNameOrPid) when is_atom(GroupNameOrPid); is_pid(GroupNameOrPid) ->
    gen_server:call(where(GroupNameOrPid),get_all_resources,infinity).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%%--------------------------------------------------------------------

init([Owner,Configuration = #pool{}]) ->
    process_flag(trap_exit,true),
    Name = Configuration#pool.name,
    _Type = Configuration#pool.pool_type,
    BehaviourMod = Configuration#pool.resource_behaviour_module,
    gproc:reg({n,l,{pool_manager, Name}}),
    {ok, ResourceSupervisor} = gen_pool_resource_sup:start_link(),
    {ok, BehaviourState} = BehaviourMod:init(Configuration),
    State = #state { configuration     = Configuration,
		     behaviour_mod     = BehaviourMod,
		     min_pool_size     = Configuration#pool.min_pool_size,
		     max_pool_size     = Configuration#pool.max_pool_size,
		     notify_completion = Configuration#pool.notify_completion,
		     resource_sup      = ResourceSupervisor,
		     resource_pool_pid = Owner,
		     behaviour_state   = BehaviourState,
		     debug             = Configuration#pool.debug,
		     resources_table = ets:new(pool_resources, [ordered_set,protected])},

    {ok,NewState} = do_start_resources(State#state.min_pool_size,permanent,State),
    init_pool_state(?STATE_UNDEFINED,NewState),
    {ok, NewState}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------

handle_call({get_resource,ClientPID}, _From, State) ->
    do_handle_get_resource(ClientPID,State);

%%--------------------------------------------------------------------
handle_call(get_configuration, _From, State) ->
    {reply,{ok,State#state.configuration},State};

%%--------------------------------------------------------------------

handle_call({release_resource,Resource}, _From, State) ->
    do_handle_release_resource(Resource,State);

handle_call({release_resource,Resource,WatcherPid}, _From, State) ->
    do_handle_release_resource(Resource,WatcherPid,State);


%%--------------------------------------------------------------------

handle_call(resource_count, _From, State) ->    
    Count = ets:info(State#state.resources_table,size),
    {reply, {ok,Count}, State};

%%--------------------------------------------------------------------

handle_call({is_state,online,Resource}, _From, State) ->
    do_is_state(online,Resource,State);

%%--------------------------------------------------------------------

handle_call({register_completion,CompletionFun}, _From, State) ->
    {reply, ok, State#state{ notify_completion = CompletionFun }};

%%--------------------------------------------------------------------

handle_call(get_all_resources, _From, State) ->  
    Resources = ets:select(State#state.resources_table,[{{'$1','$2','$3','$4'},[],['$_']}]),
    {reply, {ok,Resources}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%%--------------------------------------------------------------------

handle_cast({notify_resource_state,ResourceState,ResourcePid}, State) ->
    do_handle_resource_state_change(ResourceState,ResourcePid,State);

%%--------------------------------------------------------------------

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%%--------------------------------------------------------------------

handle_info({'EXIT',Pid,Reason}, State) ->
    do_handle_exit_signal(Pid,Reason,State);

%%--------------------------------------------------------------------

handle_info(_Info, State) ->
    {noreply,State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------

terminate(Reason, State) ->
    io:format("[POOL] [~s] ~p Resource pool shutting down with reason ~w\n",["Resource Pool",self(),Reason]),
    Sup = State#state.resource_sup,
    Mon = erlang:monitor(process,Sup),
    exit(Sup,shutdown),
    receive
	{'DOWN', Mon, _, _, _} ->
	    ok
    end,

    set_pool_state(?STATE_DOWN,State),
    io:format("[POOL] [~s] ~p Resource pool shut down successfully\n",["Resource Pool",self()]),
    log(resource_pool,"Resource pool is terminating (reason: ~p)\n",[Reason],State).

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Startup X new resources
%%--------------------------------------------------------------------
do_start_resources(Count,Type,StateData) ->
    Config         = StateData#state.configuration,
    ResourceModule = Config#pool.resource_module,
    Configuration  = Config#pool{resource_behaviour = Type},  
    do_start_resources(Count,Type,ResourceModule,Configuration,StateData).

%%--------------------------------------------------------------------

do_start_resources(_Count = 0,_Type,_ResourceModule,_Configuration,StateData) ->
    update_resource_count(StateData),
    {ok,StateData};

do_start_resources(Count,Type,ResourceModule,Configuration,StateData) ->
    BehaviourState = StateData#state.behaviour_state,
    BehaviourMod   = StateData#state.behaviour_mod,
    log(resource_pool,"starting new ~p resource",[Type],StateData), 
    {ok,ResourceStartArgs, NewBehaviourState} = BehaviourMod:configure_next_resource(BehaviourState),
    {ok,_Resource} = gen_pool_resource_sup:start_resource(StateData#state.resource_sup,Type,ResourceModule,ResourceStartArgs),
    do_start_resources(Count-1,Type,ResourceModule,Configuration,StateData#state{behaviour_state = NewBehaviourState}).

%%--------------------------------------------------------------------
%% Notify when a resource enters online state, by calling the notify completion
%%--------------------------------------------------------------------

do_resource_notification(_State,StateData) when StateData#state.notify_completion == undefined ->
    ok;
do_resource_notification(State,StateData) ->
    (StateData#state.notify_completion)(State).

%%--------------------------------------------------------------------
%% Handle resource state change notifications
%%--------------------------------------------------------------------

do_handle_resource_state_change(State,ResourcePid,StateData) ->
    log(resource_pool,"resource '~p' changed state to '~p'",[ResourcePid,State],StateData),

    link(ResourcePid),

    {ResourcePid,_Old_State,No,Watcher} = case ets:lookup(StateData#state.resources_table,ResourcePid) of
					      []        -> {ResourcePid,State,0,undefined};
					      [{P,S,N,T}] -> {P,S,N,T}
					  end,

    true = ets:insert(StateData#state.resources_table,{ResourcePid,State,No,Watcher}),

    ok = case State of
	     online      -> do_resource_notification(online,StateData);
	     _OtherState -> ok
	 end,
    update_resource_counters(StateData),

    {noreply, StateData}.

%%--------------------------------------------------------------------
%% Handle exit signal from the resource supervisor.
%% When the resource supervisor exist's we crash aswell
%%--------------------------------------------------------------------

do_handle_exit_signal(Pid,_Reason,State) when (Pid == State#state.resource_sup) ->
    {stop,normal,State};

%%--------------------------------------------------------------------
%% Handle exit signal from resources
%%   When a resource exist's we delete it from the resources ets table
%%--------------------------------------------------------------------
do_handle_exit_signal(Pid,shutdown,StateData) when Pid == StateData#state.resource_pool_pid ->
    debug(resource_pool,"resource pool got shutdown signal from resource pool ~p",[Pid],StateData),
    {stop,shutdown,StateData};

do_handle_exit_signal(Pid,Reason,StateData) ->
    log(resource_pool,"resource pid: ~p existed with reason: ~p",[Pid,Reason],StateData),

    true = ets:delete(StateData#state.resources_table,Pid),

    update_resource_counters(StateData),
    update_resource_count(StateData),

    {noreply, StateData}.

%%--------------------------------------------------------------------

do_handle_get_resource(ClientPID,StateData) ->
    Reply = case do_get_online_resource(StateData) of
		{error,no_available_resource} ->
		    case {has_allocated_resources(StateData), has_max_resources(StateData)}  of
			{_HasAllocated = false,_MaxResources} ->
			    log(resource_pool,"get_resource: (no resources in online/allocated state)",[],StateData),
			    {error,no_available_resource};
			{_HasAllocated = true,_MaxResources = true} ->
			    log(resource_pool,"get_resource: (no resources available, pending)",[],StateData),
			    {error,resource_pending};
			{_HasAllocated = true,_MaxResources = false} ->
			    log(resource_pool,"get_resource: (no resources available, starting new resource and pending)",[],StateData),
			    ok = do_start_resources(1,temporary,StateData),
			    {error,resource_pending}
		    end;

		{ok,ResourcePid} ->
		    Watcher = proc_lib:spawn(sqldb_resource_pool,'$monitor_resource_client',[self(),ClientPID,ResourcePid]),
		    log(resource_pool,"get_resource: (allocating resource)",[],StateData),
		    [{ResourcePid,_State,No,_PrevWatcher}] = ets:lookup(StateData#state.resources_table,ResourcePid),
		    true = ets:insert(StateData#state.resources_table,{ResourcePid,allocated,No+1,Watcher}),
		    update_resource_counters(StateData),
		    {ok,ResourcePid}
	    end,
    {reply, Reply, StateData}.

%%--------------------------------------------------------------------

do_handle_release_resource(Resource,StateData) ->
    log(resource_pool,"release_resource 1",[],StateData),
    case ets:lookup(StateData#state.resources_table,Resource) of
	[{Resource,allocated,N,Watcher}] ->
	    ok = kill_watcher(Watcher),
	    true = ets:insert(StateData#state.resources_table,{Resource,online,N,undefined}),
	    update_resource_counters(StateData),
	    {reply, ok, StateData};
	[{Resource,online,_N,Watcher}] ->
	    %% ADP: This is the case when the resource has restarted while the resource was allocated --- TEST TEST TEST
	    {reply, ok, StateData};
	[{Resource,_onlineOrStarted,_N,Watcher}] when _onlineOrStarted == online;
						      _onlineOrStarted == started->
	    %%      ok = kill_watcher(Watcher),

	    %% RvG: This is the case when the resource has already been deallocated --- TEST TEST TEST
	    {reply, ok, StateData};
	[] ->
	    %% RvG -> Release an invalid resource
	    {reply,ok, StateData}
    end.


do_handle_release_resource(Resource,WatcherPid,StateData) ->
    ok = kill_watcher(WatcherPid),
    log(resource_pool,"release_resource 2",[],StateData),
    case ets:lookup(StateData#state.resources_table,Resource) of
	[{Resource,allocated,N,WatcherPid}] when is_pid(WatcherPid) ->
	    exit(WatcherPid,kill),
	    true = ets:insert(StateData#state.resources_table,{Resource,online,N,undefined}),
	    update_resource_counters(StateData),
	    {reply, true, StateData};
	[{Resource,allocated,_N,OtherWatcherPidNow}]  ->
	    %% RvG: This is the case when the resource has already been deallocated by the owner process and allocated to another one--- TEST TEST TEST
	    {reply, false, StateData};
	[{Resource,_onlineOrStarted,_N,_OtherWatcher}] when _onlineOrStarted == online;
							    _onlineOrStarted == started->
	    %% RvG: This is the case when the resource has already been deallocated --- TEST TEST TEST
	    {reply, false, StateData};
	[] ->
	    %% RvG -> Release an invalid resource
	    {reply,ok, StateData}
    end.

kill_watcher(undefined) ->
    ok;
kill_watcher(Pid) when is_pid(Pid) ->
    exit(Pid,kill),
    ok.

%%--------------------------------------------------------------------

do_get_online_resource(StateData) ->
    case ets:select(StateData#state.resources_table,[{{'$1','$2','$3','$4'},[{'==', '$2', online}],['$_']}],1) of
	'$end_of_table' ->
	    {error,no_available_resource};
	{[{ResourcePid,online,_N,_Watcher}],_C} ->
	    {ok,ResourcePid}
    end.

%%--------------------------------------------------------------------

do_is_state(State,_Resource,StateData) ->
    case ets:select(StateData#state.resources_table,[{{'$1','$2','$3','$4'},[{'==', '$2', State}],['$_']}],1) of
	{[{_ResourcePid,online,_N,_Watcher}],_C} ->
	    {reply,true,StateData};
	{[{_ResourcePid,_Other,_N,_Watcher}],_C} ->
	    {reply,false,StateData};
	'$end_of_table' ->
	    {reply,false,StateData}
    end.


%%--------------------------------------------------------------------

has_allocated_resources(StateData) ->
    case ets:select(StateData#state.resources_table,[{{'$1','$2','$3','$4'},[{'==', '$2', allocated}],['$_']}],1) of
	'$end_of_table' ->
	    false;
	{[{_ResourcePid,allocated,_N,_Watcher}],_C} ->
	    true
    end.

has_max_resources(StateData) ->
    ets:info(StateData#state.resources_table,size) >= StateData#state.max_pool_size.

%%--------------------------------------------------------------------
%% Update sqldb counters
%%--------------------------------------------------------------------

init_pool_state(State,StateData) ->
    Config = StateData#state.configuration,
    %%ID      = Config#pool.id,
    %%perfcounter:set({sqldb_pool,node(),Config#pool.pool_state},State).
    ok.

set_pool_state(State,StateData) ->
    Config = StateData#state.configuration,
    %%ID      = Config#pool.id,
    %%CounterName = {sqldb_pool,node(),ID,pool_state},
    ok.
%    case perfcounter:value(CounterName) of
%	State -> %% The state did not change
%	    ok;
%	_OldState -> %% The state changed
%	    set_counter(pool_state,State,StateData),
%	    case State of
%		?STATE_UP   -> sqldb_snmp:sqlUp(ID);
%		?STATE_DOWN -> sqldb_snmp:sqlDown(ID)
%	    end
%    end.

update_resource_counters(StateData) ->
    Results = ets:select(StateData#state.resources_table,[{{'$1','$2','$3','$4'},[],['$_']}]),

    F = fun({_CPid,online,_C,_Watcher},{OnlineCount,AllocatedCount}) ->
		{OnlineCount+1,AllocatedCount};
	   ({_CPid,allocated,_C,_Watcher},{OnlineCount,AllocatedCount}) ->
		{OnlineCount,AllocatedCount+1};
	   ({_CPid,_State,_C,_Watcher},{OnlineCount,AllocatedCount}) ->
		{OnlineCount,AllocatedCount}
	end,

    {OnlineCount,AllocatedCount} = lists:foldl(F,{0,0},Results), 

    %% Update the pool state to up or down
    case {OnlineCount,AllocatedCount} of
	{0,0} ->  %% There is no online or allocated resources
	    set_pool_state(?STATE_DOWN,StateData);
	{_O,_A} -> %% There is atleast one online or allocated resource
	    set_pool_state(?STATE_UP,StateData)
    end,

    set_counter(resource_online_count,OnlineCount,StateData),
    set_counter(resource_allocated_count,AllocatedCount,StateData),

    ok.

update_resource_count(StateData) ->
    Children = supervisor:which_children(StateData#state.resource_sup),
    set_counter(resource_count,length(Children),StateData).

set_counter(CounterName,Value,StateData) ->
    Config = StateData#state.configuration,
%%    ID      = Config#pool.id,
    %perfcounter:set({sqldb_pool,node(),ID,CounterName},Value).
    ok.

%%--------------------------------------------------------------------

'$monitor_resource_client'(PoolPid, ClientPid, ResourcePid) ->
    Ref  = erlang:monitor(process,ClientPid),
    PRef = erlang:monitor(process,PoolPid),

    receive
	{'DOWN',PRef,process,_Object,_Other} ->
	    %% Pool died
	    ok;
	{'DOWN',Ref, process,_Object,_Other} ->
	    case catch release_resource(PoolPid,ResourcePid,self()) of
		true ->	  
		    ClientInfo = try
				     erlang:process_info(ClientPid)
				 catch
				     error:badarg ->
					 remote_process
				 end,
		    error_logger:error_report([ {sqldb_pool, monitor_resource_client}, 
						{client_pid,ClientPid}, 
						{resource,ResourcePid}, 
						{reason,"Resource was not released by client process"}, 
						{client_info,ClientInfo}]),
		    ok;
		false ->
		    ok;
		_ ->
		    ok
	    end
    end.

%%--------------------------------------------------------------------

log(Type,Message,Parameters,StateData) ->
    debug(Type,Message,Parameters,StateData).

%%--------------------------------------------------------------------

debug(Type,Message,Parameters,StateData) when StateData#state.debug == true ->
    io:format("[POOL] [~p] "++Message++"\n",[Type|Parameters]);

debug(_Type,_Message,_Parameters,_StateData) ->
    ok.

%%--------------------------------------------------------------------


