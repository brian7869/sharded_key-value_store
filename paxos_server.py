import socket, threading, datetime, json, os, sys, random, time
from paxos_utils import *
from multiprocessing import Process, Lock
from config import MASTER_HOST, MASTER_PORT, MAX_FAILURE, HEARTBEART_CYCLE, TIMEOUT

SEED = int(time.time())
print 'SEED is {}'.format(SEED)
random.seed(SEED)

class Paxos_server(Process):
	def __init__(self, shard_id, replica_id, address_list, skipped_slots, fail_view_change = 0):
		# super(Paxos_server, self).__init__()
		# 0. Initialize internal data
		self.num_replicas = 2 * MAX_FAILURE + 1
		self.shard_id = shard_id
		self.replica_id = replica_id
		self.address_list = address_list
		self.host = address_list[replica_id][0]
		self.port = address_list[replica_id][1]
		self.data = {}
		self.last_leader_num_for_campaign = self.replica_id - self.num_replicas
		self.log_name = 'log/shard{}_replica{}.log'.format(self.shard_id, self.replica_id)
		
		# self.chat_log_filename = 'chat_log/server_{}.chat_log'.format(str(replica_id))

		self.accepted = {}	# {
							#	<slot_num>:{
							#		'client_addr':<client_addr>,
							#		'client_seq':<client_seq>,
							#		'leader_num':<leader_num>,
							#		'command':<command>,
							#		'accepted_replicas':<list_of_accepted_replica_id>
							#		'result': <result>
							#	}
							# }
		self.client_progress = {}	# {	//what has been proposed for this client
									#	<client_addr>:{
									#		'client_seq':<client_seq>,
									#		'slot':<slot_num>
									#	}
									# }
		# self.client_list = []	# [<client_address>]
		self.executed_command_slot = -1
		self.assigned_command_slot = -1
		self.leader_num = -1
		self.skipped_slots = skipped_slots
		self.fail_view_change = fail_view_change

		self.num_followers = 0

		self.replica_heartbeat = []
		self.live_set = []
		self.heartbeat_lock = Lock()
		self.liveset_lock = Lock()

		if not os.path.isdir('log'):
			os.makedirs('log')
		if os.path.exists(self.log_name):
			os.remove(self.log_name)
		
		for i in xrange(self.num_replicas):
			self.replica_heartbeat.append(datetime.datetime.now())
		
	def run(self):
		# 1. Create new UDP socket (receiver)
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.bind((self.host, self.port))
		sock.listen(5)

		# 2. Create heartbeat sender and failure detector
		heartbeat_sender_thread = threading.Thread(name="heartbeat_sender_" + str(self.replica_id),
												   target=self.heartbeat_sender, args=())
		failure_detector_thread = threading.Thread(name="failure_detector_" + str(self.replica_id),
												   target=self.failure_detector, args=())
		
		heartbeat_sender_thread.daemon = True
		failure_detector_thread.daemon = True
		heartbeat_sender_thread.start()
		failure_detector_thread.start()
		
		# 3. Start receiving message
		
		while True:
			conn, addr = sock.accept()
			message = ''
			while True:
				data = conn.recv(4096)
				if not data:
					break
				message += data
			self.message_handler(message)
		

	def heartbeat_sender(self):
		heartbeat_message = "Heartbeat {}".format(str(self.replica_id))
		while True:
			self.broadcast(heartbeat_message)
			time.sleep(HEARTBEART_CYCLE)

	def failure_detector(self):
		while True:
			new_live_set = []
			self.heartbeat_lock.acquire()
			for i in xrange(self.num_replicas):
				if (datetime.datetime.now() - self.replica_heartbeat[i]).total_seconds() < TIMEOUT or i == self.replica_id:
					new_live_set.append(i)
			self.heartbeat_lock.release()

			self.liveset_lock.acquire()
			self.live_set = new_live_set
			self.liveset_lock.release()

			if self.replica_id == new_live_set[0] and self.num_followers == 0 and \
				( (self.leader_num % self.num_replicas) not in new_live_set or self.leader_num == -1 ):
				self.runForLeader()
				
			time.sleep(TIMEOUT)

	def message_handler(self, message):
		# Possible messages
		# From replicas:
		# 0. "Heartbeat <sender_id>"
		# 1. "IAmLeader <leader_num>"
		# 2. "YouAreLeader <leader_num> <accepted>"
		# 3. "Propose <leader_num> <slot_num> <Request_message>"
		# 4. "Accept <sender_id> <leader_num> <sequence_num> <Request_message>"
		# From clients:
		# 1. "Request <host> <port> <client_seq> <command>"
		# 2. "ViewChange <host> <port>"
		type_of_message, rest_of_message = tuple(message.split(' ', 1))

		if type_of_message != "Heartbeat":
			if type_of_message == "YouAreLeader":	
				self.debug_print(" *** receiving message: {} ***".format(message[:message.find('{')]))
			else:
				self.debug_print(" *** receiving message: {} ***".format(message))
		if type_of_message == "Heartbeat":
			sender_id = int(rest_of_message)
			self.heartbeat_lock.acquire()
			self.replica_heartbeat[sender_id] = datetime.datetime.now()
			self.heartbeat_lock.release()

		elif type_of_message == "IAmLeader":
			new_leader_num = int(rest_of_message)
			if new_leader_num > self.leader_num:
				leader_id = new_leader_num % self.num_replicas
				response = "YouAreLeader {} {}".format(str(new_leader_num)
					, json_spaceless_dump(self.accepted))
				send_message(self.address_list[leader_id][0], self.address_list[leader_id][1], response, random)
				self.leader_num = new_leader_num

		elif type_of_message == "YouAreLeader":
			new_leader_num, accepted = tuple(rest_of_message.split(' ', 1))
			new_leader_num = int(new_leader_num)
			if self.num_followers > 0 and new_leader_num > self.leader_num:
				self.num_followers += 1

				accepted = json_set_serializable_load(accepted)
				for slot, inner_dict in accepted.iteritems():
					slot = int(slot)
					if slot not in self.accepted or inner_dict['leader_num'] > self.accepted[slot]['leader_num']:
						self.accepted[slot] = inner_dict
						if inner_dict['client_address'] not in self.client_progress or inner_dict['client_seq'] > self.client_progress[inner_dict['client_address']]['client_seq']:
							self.client_progress[inner_dict['client_address']] = {'client_seq': inner_dict['client_seq'], 'slot': slot}
					elif inner_dict['leader_num'] == self.accepted[slot]['leader_num']:
						if self.accepted[slot]['client_address'] == inner_dict['client_address']\
							and self.accepted[slot]['client_seq'] == inner_dict['client_seq']:
							self.accepted[slot]['accepted_replicas'].update(inner_dict['accepted_replicas'])
							if len(self.accepted[slot]['accepted_replicas']) >= MAX_FAILURE + 1:
								self.decide_value(slot)
						else:
							assert False and 'Different values proposed in the same slot by the same leader'
				if self.num_followers >= MAX_FAILURE + 1:
					self.leader_num = int(new_leader_num)
					msg = "LeaderIs {} {}".format(str(self.shard_id), str(self.replica_id))
					send_message(MASTER_HOST, MASTER_PORT, msg, random)
					self.repropose_undecided_value()
					### FOR TESTING ###
					if self.fail_view_change:
						sys.exit()
					### FOR TESTING ###
					self.fill_holes()
					self.num_followers = 0

		elif type_of_message == "Propose":
			leader_num, slot, request_message = tuple(rest_of_message.split(' ', 2))
			leader_num = int(leader_num)
			leader_id = leader_num % self.num_replicas
			slot = int(slot)
			request = self.parse_request(request_message)
			client_address = self.get_client_address(request)
			if leader_num >= self.leader_num and (client_address not in self.client_progress or request['client_seq'] >= self.client_progress[client_address]['client_seq']):
				self.leader_num = leader_num

				self.accept(self.replica_id, slot, request_message)

				if client_address in self.client_progress\
					and request['client_seq'] > self.client_progress[client_address]['client_seq']\
					and self.accepted[self.client_progress[client_address]['slot']]['result'] is None:
					slot_to_fill = self.client_progress[client_address]['slot']
					self.decide_value(slot_to_fill)

				if client_address != '-1:-1':	# Don't need to update client_progress for NOOP request
					self.client_progress[client_address] = {'client_seq': request['client_seq'], 'slot': slot}

				if slot not in self.accepted:
					self.accepted[slot] = self.get_new_accepted(client_address, request['client_seq'], leader_num, request['command'], set([self.replica_id, leader_id]), None)
				else:
					if self.accepted[slot]['client_address'] == client_address\
						and self.accepted[slot]['client_seq'] == request['client_seq']:
						self.accepted[slot]['accepted_replicas'].add(self.replica_id)
					else: # larger leader_num wins
						self.accepted[slot] = self.get_new_accepted(client_address, request['client_seq'], leader_num, request['command'], set([self.replica_id, leader_id]), None)

				if len(self.accepted[slot]['accepted_replicas']) >= MAX_FAILURE + 1\
					and self.accepted[slot]['result'] is None:
					self.decide_value(slot)

		elif type_of_message == "Accept":
			# if self.isLeader():
			# 	self.debug_print(" *** receiving message: {} ***".format(message))
			sender_id, leader_num, slot, request_message = tuple(rest_of_message.split(' ', 3))
			leader_num = int(leader_num)
			leader_id = leader_num % self.num_replicas
			slot = int(slot)
			request = self.parse_request(request_message)
			sender_id = int(sender_id)
			client_address = self.get_client_address(request)
			if leader_num >= self.leader_num and (client_address not in self.client_progress or request['client_seq'] >= self.client_progress[client_address]['client_seq']):
				self.leader_num = leader_num

				if client_address in self.client_progress\
					and request['client_seq'] > self.client_progress[client_address]['client_seq']\
					and self.accepted[self.client_progress[client_address]['slot']]['result'] is None:
					slot_to_fill = self.client_progress[client_address]['slot']
					self.decide_value(slot_to_fill)

				if client_address != '-1:-1':
					self.client_progress[client_address] = {'client_seq': request['client_seq'], 'slot': slot}

				if slot not in self.accepted:
					self.accepted[slot] = self.get_new_accepted(client_address, request['client_seq'], leader_num, request['command'], set([self.replica_id, leader_id]), None)
				else:
					if self.accepted[slot]['client_address'] == client_address\
						and self.accepted[slot]['client_seq'] == request['client_seq']:
						self.accepted[slot]['accepted_replicas'].add(sender_id)
					else: # larger leader_num wins
						# debug_print('over write happens!!!')
						self.accepted[slot] = self.get_new_accepted(client_address, request['client_seq'], leader_num, request['command'], set([self.replica_id, leader_id]), None)

				if len(self.accepted[slot]['accepted_replicas']) >= MAX_FAILURE + 1\
					and self.accepted[slot]['result'] is None:
					self.decide_value(slot)

		elif type_of_message == "Request":
			if self.isLeader():
				request = self.parse_request(message)
				client_address = self.get_client_address(request)
				# if client_address not in self.client_list:
				# 	self.client_list.append(client_address)

				# self.debug_print('leader processing client request')
				# self.debug_print(str(client_address in self.client_progress))
				# if client_address in self.client_progress:
				# 	self.debug_print(str(self.client_progress[client_address]['client_seq']))
				if client_address in self.client_progress\
					and self.client_progress[client_address]['client_seq'] >= request['client_seq']:
					if self.client_progress[client_address]['client_seq'] == request['client_seq']:
						allocated_slot = self.client_progress[client_address]['slot']
						result = self.accepted[allocated_slot]['result']
						if result is not None and result is not False:
							if self.accepted[allocated_slot]['command'].find('AddShard') != -1:
								type_of_command, begin, end, new_shard_address = tuple(self.accepted[allocated_slot]['command'].split(' ', 3))
								data_to_transfer_json = self.accepted[allocated_slot]['result']
								self.transfer_data(request['host'], request['port'], new_shard_address, data_to_transfer_json)
							else:
								message = "Reply {} {}".format(request['client_seq'], str(self.accepted[allocated_slot]['result']))
								send_message(request['host'], request['port'], message, random)
						else:
							# self.debug_print("Repropose request {}".format(message))
							self.propose(allocated_slot, message)
				else:
					assigned_slot = self.get_new_assigned_slot()
					self.accepted[assigned_slot] = self.get_new_accepted(client_address, request['client_seq'], self.leader_num, request['command'], set([self.replica_id]), None)
					self.client_progress[client_address] = {'client_seq': request['client_seq'], 'slot': assigned_slot}
					self.propose(assigned_slot, message)
					self.accept(self.replica_id, assigned_slot, message)
				
			else:	# forward message to current leader
				if self.leader_num != -1:
					leader_id = self.leader_num % self.num_replicas
					send_message(self.address_list[leader_id][0], self.address_list[leader_id][1], message, random)

		elif type_of_message == 'ViewChange':
			host, port = tuple(rest_of_message.split(' ', 1))
			client_address = host+':'+port
			# if client_address not in self.client_list:
			# 	self.client_list.append(client_address)
			self.liveset_lock.acquire()
			new_live_set = self.live_set
			self.liveset_lock.release()
			if self.replica_id == new_live_set[0]:
				# self.debug_print('I should be the one!')
				self.runForLeader()

	################# Here are helper functions for sending messages #################
	def broadcast(self, message):
		type_of_message, rest_of_message = message.split(' ', 1)
		# if type_of_message != "Heartbeat":
		# 	self.debug_print("=== sending message : {} ===".format(message))
		for i in xrange(self.num_replicas):
			if i != self.replica_id:
				send_message(self.address_list[i][0], self.address_list[i][1], message, random)

	def propose(self, seq, request):
		propose_message = "Propose {} {} {}".format(str(self.leader_num), str(seq), request)
		self.broadcast(propose_message)

	def accept(self, sender_id, seq, request):
		accept_message = "Accept {} {} {} {}".format(str(sender_id), str(self.leader_num), str(seq), request)
		self.broadcast(accept_message)
	################# End of helper functions for sending messages #################

	################# Here are helper functions for creating objects #################
	def get_new_accepted(self, client_addr, client_seq, leader_num, command, accepted_replicas, result):
		accepted = {
			'client_address': client_addr,
			'client_seq': client_seq, 
			'leader_num': leader_num, 
			'command': command, 
			'accepted_replicas': accepted_replicas, 
			'result': result
		}
		return accepted

	def parse_request(self, request_message):
		components = request_message.split(' ',4)
		request = {
			'host': components[1],
			'port': int(components[2]),
			'client_seq':int(components[3]),
			'command': components[4]
		}
		return request
	################# End of helper functions for creating objects #################

	################# Here are helper functions for learner #################
	def decide_value(self, slot):
		# print("Decide slot {}".format(str(slot)))
		if self.accepted[slot]['result'] is None:
			self.accepted[slot]['result'] = False
		self.execute()

	def execute(self):
	    while self.executed_command_slot + 1 in self.accepted and self.accepted[self.executed_command_slot + 1]['result'] is not None:
	        self.executed_command_slot += 1
	        client_addr = self.accepted[self.executed_command_slot]['client_address'].split(':')
	        result = self.run_command(client_addr[0], client_addr[1], self.accepted[self.executed_command_slot]['command'])
	        with open('log/shard{}_replica{}.log'.format(self.shard_id, self.replica_id), 'a') as log_f:
				log_f.write('{}\tresult: {}\n'.format(self.accepted[self.executed_command_slot]['command'], result))
	        if self.accepted[self.executed_command_slot]['result'] is not False and self.accepted[self.executed_command_slot]['result'] != result:
	        	print self.accepted[self.executed_command_slot]['result'], result
	        	assert False and 'Reach divergent state'
	        self.accepted[self.executed_command_slot]['result'] = result
	        reply_to_client_message = "Reply {} {}".format(self.accepted[self.executed_command_slot]['client_seq'], str(result))
	        reply_to_master_message = "Reply {} {}".format(self.accepted[self.executed_command_slot]['client_address'], self.shard_id)
	        if client_addr[0] == '-1' or self.accepted[self.executed_command_slot]['command'].find('AddShard') != -1:
	            continue
	        send_message(client_addr[0], client_addr[1], reply_to_client_message, random)
	        send_message(MASTER_HOST, MASTER_PORT, reply_to_master_message, random)

	def run_command(self, host, port, command):
		print("Execute {}".format(command))
		if command == "NOOP":
			return 0
		type_of_command, rest_of_command = tuple(command.split(' ', 1))
		if type_of_command == "Put":
			key, value = tuple(rest_of_command.split(' ', 1))
			self.data[key] = value
			return 0
		elif type_of_command == "Get":
			key = rest_of_command.split(' ')[0]
			if key in self.data:
				return self.data[key]
			else:
				return 'No such key'
		elif type_of_command == "Delete":
			key = rest_of_command.split(' ')[0]
			if key in self.data:
				del self.data[key]
				return 0
			else:
				return 'No such key'
		elif type_of_command == "AddShard":
			begin, end, new_shard_address = tuple(rest_of_command.split(' ', 2))
			data_to_transfer_json = self.filter_data(int(begin), int(end))
			self.transfer_data(host, port, new_shard_address, data_to_transfer_json)
			return data_to_transfer_json
		elif type_of_command == "InitData":
			self.data = json.loads(rest_of_command)
			return self.shard_id
		else:
			assert False and 'No such command'

	def filter_data(self, begin, end):
		data_to_transfer = {}
		for key in self.data:
			hash_value = get_hash_value(key)
			if within_the_range(begin, end, hash_value):
				data_to_transfer[key] = self.data[key]
		return json_spaceless_dump(data_to_transfer)

	def transfer_data(self, master_host, master_port, new_shard_address, data_to_transfer_json):
		dest_addr_list = json.loads(new_shard_address)
		init_data_message = "Request {} {} 0 InitData {}".format(master_host, master_port, data_to_transfer_json)

		for addr in dest_addr_list:
			send_message(addr[0], addr[1], init_data_message, random)

	################# End of helper functions for learner #################

	################# Here are helper functions for view change #################
	def runForLeader(self):
		self.num_followers = 1
		new_leader_num = max(self.replica_id + (self.leader_num - (self.leader_num % self.num_replicas)), self.last_leader_num_for_campaign + self.num_replicas)
		if new_leader_num <= self.leader_num:
			new_leader_num += self.num_replicas
		self.last_leader_num_for_campaign = new_leader_num
		message = "IAmLeader {}".format(str(new_leader_num))
		# self.debug_print("Make America Great Again!!!")
		self.broadcast(message)

	def repropose_undecided_value(self):
		for slot, inner_dict in self.accepted.iteritems():
			if inner_dict['result'] is None:
				host, port = tuple(inner_dict['client_address'].split(':', 1))
				request = "Request {} {} {} {}".format(host, port, inner_dict['client_seq'], inner_dict['command'])
				self.propose(slot, request)


	def fill_holes(self):
		if not self.accepted:
			return
		self.assigned_command_slot = max(self.accepted.keys())
		for slot in xrange(int(self.assigned_command_slot)):
			if slot not in self.accepted:
				noop_request = "Request -1 -1 -1 NOOP"
				self.accepted[slot] = self.get_new_accepted('-1:-1', -1, self.leader_num, 'NOOP', set([self.replica_id]), None)
				self.propose(slot, noop_request)
	################# End of helper functions for view change #################

	################# Here are miscellaneous helper functions #################
	def isLeader(self):
		return self.leader_num != -1 and self.leader_num % self.num_replicas == self.replica_id

	def get_client_address(self, request):
		return "{}:{}".format(request['host'], request['port'])

	def get_new_assigned_slot(self):
		self.assigned_command_slot += 1
		while self.assigned_command_slot in self.skipped_slots:
			self.assigned_command_slot += 1
		return self.assigned_command_slot
	################# End of miscellaneous helper functions #################

	################# Here is helper function for debugging  #################
	def debug_print(self, msg):
		print str(self.replica_id) + ': '+msg
	################# End of helper function for debugging  #################


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

	server = Paxos_server(shard_id, replica_id, address_list, skipped_slots, fail_view_change)
	server.run()