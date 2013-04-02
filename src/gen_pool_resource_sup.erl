%%% @author Rudolph van Graan <rvg@wyemac.local>
%%% @copyright (C) 2013, Rudolph van Graan
%%% @doc
%%%
%%% @end
%%% Created : 19 Mar 2013 by Rudolph van Graan <rvg@wyemac.local>

-module(gen_pool_resource_sup).


-behaviour(supervisor).
%%-------------------------------------------------------------------------------------------------------------
-export([start_link/0,
	 start_resource/4]).
%%-------------------------------------------------------------------------------------------------------------
-export([init/1]).
%%-------------------------------------------------------------------------------------------------------------

start_link() ->
  supervisor:start_link(?MODULE, []).

%%-------------------------------------------------------------------------------------------------------------

start_resource(SupervisorPid,Type,Module,StartArgs) ->
  {ok,Spec} = resource_spec(Type,Module,StartArgs),
  supervisor:start_child(SupervisorPid,Spec).

%%-------------------------------------------------------------------------------------------------------------

init([]) ->
  {ok,{{one_for_one,100,10}, []}}.

%%-------------------------------------------------------------------------------------------------------------
%% Internal functions
%%-------------------------------------------------------------------------------------------------------------

resource_spec(permanent,Module,StartArgs) ->
  Spec = {{Module,make_ref()},{Module,start_resource,StartArgs},
	  permanent,10000,worker,[Module]},
  {ok,Spec};
resource_spec(temporary,Module,StartArgs) ->
  Spec = {{Module,make_ref()},{Module,start_resource,StartArgs},
	  temporary,10000,worker,[Module]},
  {ok,Spec}.


