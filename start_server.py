import paxos_server, sys, json

def start_server(shard_id, replica_id, address_list, skipped_slots, fail_view_change = 0):
	server = paxos_server.Paxos_server(shard_id, replica_id, address_list, skipped_slots, fail_view_change)
	server.start()
	return server

if __name__ == '__main__':
	shard_id = int(sys.argv[1])
	replica_id = int(sys.argv[2])
	shard_config = open(sys.argv[3], 'r')
	address_list = json.load(shard_config)
	shard_config.close()
	skipped_slots = sys.argv[4].split(',')
	for i in xrange(len(skipped_slots)):
		skipped_slots[i] = int(skipped_slots[i])
	fail_view_change = int(sys.argv[5])

	start_server(shard_id, replica_id, address_list, skipped_slots, fail_view_change)