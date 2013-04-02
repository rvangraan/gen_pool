%%%-------------------------------------------------------------------
%%% @author Rudolph van Graan <rvg@wyemac.lan>
%%% @copyright (C) 2013, Rudolph van Graan
%%% @doc
%%%
%%% @end
%%% Created :  2 Apr 2013 by Rudolph van Graan <rvg@wyemac.lan>
%%%-------------------------------------------------------------------
-module(gen_pool_resource_behaviour).

-export([init/1,
	 configure_next_resource/1,
	 terminate/1]).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{init,1},
     {configure_next_resource, 1},
     {terminate, 1}];
behaviour_info(_Other) ->
    undefined.

-include_lib("gen_pool/include/gen_pool.hrl").

-record(behaviour_state,{configuration}).

init(Configuration = #pool{}) ->
    {ok,#behaviour_state{configuration = Configuration}}.

%% @doc callback to derive the actual resource configuration from the pool configuration and state
configure_next_resource(State = #behaviour_state{configuration = Configuration}) ->
    {ok,Configuration,State}.


terminate(#behaviour_state{}) ->
    ok.
