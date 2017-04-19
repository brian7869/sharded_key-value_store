Team member: Wei Lee, Wei-Chih Lu

 - Instructions on how to use it:
 	
 	0. Edit configuration in config.py, master_config, shardX_cfg if necessary

 	1. Before testing:

 		Decide these following input arguments before continuing 
	 		a. MAX_FAILURE: That is f (Default maximum if shardx_cfg not modified: 3)
		 	b. DROP_RATE: simulate global drop rate when sending messgae

		Start master:
			Run $ python master_node.py master_config
		Start paxos server:
			## NOTICE: must start shards in order of id
			Run $ python paxos_server.py <shard_id> <replica_id> <shard_config> <skipped slot(-1 to not skip)> <fail_view_change>
			example: python paxos_server 0 0 shard0_cfg -1 0
		Start client:
			manual mode:
			Run $ python paxos_client.py <client_id>
			batch mode:
			Run $ python paxos_client.py <client_id> < testcase/<testcase_file>

		Check output:
			Run $ diff log/<log_file1> log/<log_file>
			to check difference between replicas in one shard
			Run $ python check_key.py <log_folder> <number_of_shards> <number_of_replicas>
			to check for linearizability for each key
			Run $ python simulator.py <replica_log_path>
			to check if key value pairs within a replica is consistent

	2. Testing functionality:

		a. Normal Operations
			Run $ python paxos_client.py 0 < testcase/test_normal_0
			or
			Run $ python paxos_client.py 0 < testcase/test_normal_1
			or
			Run $ python paxos_client.py 0 < testcase/test_normal_2
			or
			concurrently on different clients and check linearizability
		b. Randomly drop message with DROP_RATE
			Run a. concurrently with DROP_RATE
			Can simulate dropping of request, reply, isLeader...
		c. Finest-grained testing
			Before starting paxos_server:
				Go to paxos_utils.py
				Comment current send_message and uncomment the commented one
			Start replicas as usual:
				When prompt shows up, type y/n to (not drop)/(drop) a message
		c. Add Shard
			Run $ python paxos_client.py 0 < testcase/test_add_shard_0
		e. Contigent Add Shard
			Start shard0 servers 1 before 0, 2, so that there is leader initially
			Start shard1 servers 0 before 1, 2, so that there is no leader initially
			Start shard2 servers 1 before 0, 2, so that there is leader initially

			Run $ python paxos_client.py 0 < testcase/test_contigent_addshard_0	
			and
			Run $ python paxos_client.py 1 < testcase/test_contigent_addshard_1

			When client 0 is waiting for addShard1(in shard 1, no one receives 0's IAmLeader), another addShard2 request comes. This might cause the request right after addShard1 may time out, cleant's retry may end up being sent to a different shard, leading to duplicated execution
 

 - How we implemented it:

 	1. Scalability
 		We believe that scalability is a key feature of sharding, therefore we allow a shard to handle multiple requests from different clients.
 		Therefore, the original Paxos from project 1 was modified a little bit. The master node is not treated as a single client to the underlying Paxos, but a super client that forward client's request and handle view change or retry on requests for each shard.
 		This leads to the problem during adding shard, where a request can either still be in process on the old shard, or just lost during transmission. If the client retries, the master node can not distinguish if it should forward th e request to the old or new shard.
	2. Addressing Add Shard
		To address the above mentioned issue, we use a ongoning list to keep track of ongoing requests for each shard. Add shard request can only be sent to shard when its ongoing list is empty. The master node will keep retrying for all requests in the ongoing list to avoid (message loss) and (message loss with reorder of request, which make the ongoing list have different order with replicas' log)
	3. Stop handling requests when splitting key space
		To avoid inconsistency, we stop processing request for the key space that is splitting. We use a wait list to queue requests for both the old and new shard. Add shard requests will also be queued in this list.