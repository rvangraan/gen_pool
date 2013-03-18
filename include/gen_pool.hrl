


-define(POOL_RESOURCE,'$pool_resource').


-define(E_RESOURCE_ALREADY_ALLOCATED, e_resource_already_allocate).
-define(E_MNESIA_TRANSACTION_ACTIVE,  e_mnesia_transaction_active).

-define(R_RESOURCE_PENDING,       pool_resource_pending).
-define(R_RESOURCE_UNAVAILABLE,   pool_resource_unavailable).

-define(DEFAULT_ALLOCATION_TIMEOUT,10000).
-define(DEFAULT_IDLE_TTL,          30000).


-record(pool,{ id,
	       name,
	       resource_module,
	       pool_type,
	       min_pool_size,
	       max_pool_size,
	       notify_completion,
	       resource_behaviour,
	       allocation_timeout = ?DEFAULT_ALLOCATION_TIMEOUT,
	       idle_ttl           = ?DEFAULT_IDLE_TTL,
	       pool_resource_manager_pid,
	       resource_pool_pid,
	       debug   = false,
	       enabled = false,
	       parameters = []
	     }).
  
%%--------------------------------------------------------------------------------------------------
%% pool_name          string()   The unique name for the pool
%% pool_type          atom()     The pool type (postgres | odbc)
%% allocation_timeout integer()  The timeout for allocating connection from the pool
%% min_pool_size      integer()  The minimum connections that will be maintained by the pool
%% max_pool_size      integer()  The maximum connections that can be started at any one time
%% temporary_idle_ttl integer()  The max time to live for a temporary connection (in milliseconds)
%% enabled            bool()     (true | false)
%% system_sqldb_connection       bool()       System connections cannot be deleted, disabled by user
%% debug              bool()     Enable debug information
%% parameters         proplist() The pool type specific parameters
