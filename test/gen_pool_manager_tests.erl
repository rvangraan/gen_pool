%%% @author Rudolph van Graan <rvg@wyemac.local>
%%% @copyright (C) 2013, Rudolph van Graan
%%% @doc
%%%
%%% @end
%%% Created : 19 Mar 2013 by Rudolph van Graan <rvg@wyemac.local>

-module(gen_pool_manager_tests).

-export([]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("../include/gen_pool.hrl").

pool_manager_test_() ->
    {foreach,
     fun() ->
	     application:start(gproc),
	     meck:new(test_resource, [passthrough]),
	     meck:new(gen_pool_resource_manager, [passthrough]),
%	     meck:expect(test_resource, start_link, 1, {ok,_PID}),
	     Configuration = #pool{name="test",
				   resource_module=test_resource,
				   min_pool_size=1,
				   max_pool_size=2,
				   debug=true},
	     debughelper:start(),
	     debughelper:trace(test_resource),
	     debughelper:trace(gen_pool_resource_manager),
	     {ok,Pool} = gen_pool_manager:start_link(Configuration),
	     ?assertMatch(1, meck:num_calls(test_resource, start_link, ['_'])),
	     ?assertMatch(1, meck:num_calls(gen_pool_resource_manager, '$notify_resource_state', ['_',started])),
	     {ok,ResPool} = gen_pool_manager:'$get_resource_pool'("test"),
	     {ok,[{ResPid1,started,0,_}]} = gen_pool_resource_manager:'$get_all_resources'(ResPool),
	     ?assertMatch(ok, test_resource:move(ResPid1,online)),

	     timer:sleep(100),
	     ?assertMatch({ok,1}, gen_pool_resource_manager:resource_count(ResPool)),
	     [Pool,ResPool]
     end,
     fun([Pool,ConnPool]) ->
	     exit(Pool,normal),
	     ?assert(meck:validate(test_resource)),
	     ?assert(meck:validate(gen_pool_resource_manager)),

	     meck:unload(test_resource),
	     meck:unload(gen_pool_resource_manager),
	     
	     ok
     end,
     [
      {"Test basic get and release of a resource", basic_get_release_of_resource()}
     ]}.   

basic_get_release_of_resource() ->
    ?_test(
       begin
	   ok
       end).

-endif.



  
%%   timer:sleep(100),
%%   ?line {ok,1} = pool_resource_manager:resource_count(ConnPool),
%%   ?line {ok,[{ConnPid1,started,0,_}]} = pool_resource_manager:'$get_all_resources'(ConnPool),
%%   ?line ok = pool_test_resource:move(ConnPid1,online),
%%   F = fun(State) ->
%% 	 pool_manager:'$notify_resource_state'(Pool,State)
%%       end,
%%   ok = pool_resource_manager:'$register_completion'(ConnPool,F),
  
%%   [{resource_pool,ConnPool},{pool_manager,Pool}|Config].
