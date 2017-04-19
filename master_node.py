import json, threading, socket, sys, random, time
from multiprocessing import Process, Lock
from paxos_utils import json_spaceless_dump, get_hash_value, send_message, within_the_range
from config import NUM_HEXDIGITS, TIMEOUT

SHARDING = 2
NEW = 1
READY = 0

SEED = int(time.time())
print 'SEED is {}'.format(SEED)
random.seed(SEED)

class Master:
	def __init__(self, master_config_file):
		# super(Master, self).__init__()
		self.load_config(master_config_file)
		self.host = self.cfg['master_addr'][0]
		self.port = self.cfg['master_addr'][1]
		self.num_shards = len(self.cfg['shards'])
		self.key_range = []
		self.wait_list = [[]] * self.num_shards
		self.wait_list_lock = Lock()
		self.ongoing_list = [[]] * self.num_shards
		self.ongoing_list_lock = Lock()
		self.assignment_list = {}	# {<client_addr>: (<resposible_shard_id>, <request>)}
		self.add_shard_list = []	# [(<addShard_req_to_old_shard>, <old_shard>, <new_shard>)]
		begin = 0
		for i in xrange(self.num_shards):
			end = (16 ** NUM_HEXDIGITS) * (i+1) / self.num_shards
			self.key_range.append((begin, end))
			begin = end

	def run(self):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.bind((self.host, self.port))
		sock.listen(5)
		# while True:
		# 	message = sock.recv(65535)
		# 	self.message_handler(message)

		while True:
			conn, addr = sock.accept()
			message = ''
			while True:
				data = conn.recv(256)
				if not data:
					break
				message += data
			self.message_handler(message)

	def message_handler(self, message):
		# Possible message
		# From replicas:
		# 1. "LeaderIs <shard_id> <leader_id>"
		# From clients:
		# 1. "Request <host> <port_number> <client_seq> <command>"
		# 2. "ViewChange <Request>"
		# 3. "AddShard <configFile>"
		# 4. "Reply <client_addr> <shard_id>"
		print("*** receiving message: {} ***".format(message))
		type_of_message, rest_of_message = tuple(message.split(' ', 1))

		if type_of_message == "Request":
			host, port, client_seq, command = tuple(rest_of_message.split(' ', 3))
			client_addr = "{}:{}".format(host, port)
			key = command.split(' ')[1]
			client_seq = int(client_seq)
			self.ongoing_list_lock.acquire()
			self.pop_ongoing_list_with_next_client_seq(client_addr, client_seq)
			self.ongoing_list_lock.release()

			hash_value = get_hash_value(key)
			shard_id = self.get_responsible_shard(hash_value)
			self.assignment_list[client_addr] = (shard_id, message)
			
			status_code = self.cfg["shards"][shard_id]["status"]
			print("key: {},\thash: {},\tshard_id: {},\tstatus: {}".format(key, str(hash_value), shard_id, status_code))
			if status_code == READY:
				self.forward_message(shard_id, message)
				self.ongoing_list_lock.acquire()
				self.ongoing_list[shard_id].append(message)
				self.ongoing_list_lock.release()
			elif status_code == NEW:
				self.wait_list_lock.acquire()
				self.wait_list[shard_id].append(message)
				self.wait_list_lock.release()
			elif status_code == SHARDING:
				self.wait_list_lock.acquire()
				self.wait_list[shard_id].append(message)
				self.wait_list_lock.release()

		elif type_of_message == "ViewChange":
			# request_componenets = rest_of_message.split(' ', 4)
			# command = request_componenets[4]
			type_of_req, host, port, client_seq, command = tuple(rest_of_message.split(' ', 4))
			client_addr = "{}:{}".format(host, port)
			key = command.split(' ')[1]
			client_seq = int(client_seq)
			self.ongoing_list_lock.acquire()
			self.pop_ongoing_list_with_next_client_seq(client_addr, client_seq)
			self.ongoing_list_lock.release()
			shard_id = -1
			if self.assignment_list[client_addr][1] == rest_of_message:	# It has been assgined
				shard_id = self.assignment_list[client_addr][0]
			else:														# It hasn't been assgined (request message lost)
				hash_value = get_hash_value(key)
				shard_id = self.get_responsible_shard(hash_value)
				self.assignment_list[client_addr] = (shard_id, rest_of_message)

			status_code = self.cfg["shards"][shard_id]["status"]
			print("key: {},\tshard_id: {},\tstatus: {}".format(key, shard_id, status_code))
			if status_code == READY:
				viewchange_message = "ViewChange {} {}".format(host, port)
				self.wait_list_lock.acquire()
				self.wait_list[shard_id].append(rest_of_message)
				self.wait_list_lock.release()
				self.broadcast_message(shard_id, viewchange_message)
			elif status_code == NEW:
				self.wait_list_lock.acquire()
				self.wait_list[shard_id].append(rest_of_message)
				self.wait_list_lock.release()
			elif status_code == SHARDING:
				self.wait_list_lock.acquire()
				self.wait_list[shard_id].append(rest_of_message)
				self.wait_list_lock.release()

		elif type_of_message == "LeaderIs":
			shard_id, leader_id = tuple(rest_of_message.split(' ', 2))
			shard_id, leader_id = int(shard_id), int(leader_id)

			if shard_id < len(self.cfg["shards"]):
				self.cfg["shards"][shard_id]["leader_id"] = leader_id
				status_code = self.cfg["shards"][shard_id]["status"]
				if status_code == READY:
					self.wait_list_lock.acquire()
					self.forward_and_empty_wait_list(shard_id)
					self.wait_list_lock.release()
				elif status_code == NEW or status_code == SHARDING:
					for i in xrange(len(self.add_shard_list)):
						new_shard_id = self.add_shard_list[i][2]
						old_shard_id = self.add_shard_list[i][1]
						if new_shard_id == shard_id or old_shard_id == shard_id:
							send_message(self.host, self.port + new_shard_id, message, random)

		elif type_of_message == "AddShard":
			old_shard_id = self.find_shard_to_split()
			begin, end = self.update_and_get_new_key_range(old_shard_id)
			self.num_shards += 1
			shard_config_file = rest_of_message
			self.load_shard_config(shard_config_file)
			self.wait_list_lock.acquire()
			self.wait_list.append([])
			self.wait_list_lock.release()
			self.ongoing_list_lock.acquire()
			self.ongoing_list.append([])
			self.ongoing_list_lock.release()
			new_shard_id = self.num_shards - 1
			status_code = self.cfg["shards"][old_shard_id]["status"]

			new_shard_address = json_spaceless_dump(self.cfg["shards"][new_shard_id]["replicas"])
			add_shard_command = "AddShard {} {} {}".format(str(begin), str(end), new_shard_address)
			thread_port = self.port + new_shard_id
			add_shard_request = "Request {} {} {} {}".format(self.host, str(thread_port), str(new_shard_id), add_shard_command)
			self.add_shard_list.append((add_shard_request, old_shard_id, new_shard_id))

			if status_code == READY:
				self.cfg["shards"][old_shard_id]["status"] = SHARDING
				add_shard_thread = threading.Thread(name="add_shard_" + str(new_shard_id),
												   target=self.add_shard, args=(old_shard_id, new_shard_id, add_shard_request))
				add_shard_thread.start()
			elif status_code == NEW:
				self.wait_list_lock.acquire()
				self.wait_list[old_shard_id].append(add_shard_request)
				self.wait_list_lock.release()
			elif status_code == SHARDING:
				self.wait_list_lock.acquire()
				self.wait_list[old_shard_id].append(add_shard_request)
				self.wait_list_lock.release()
		elif type_of_message == "Reply":
			client_addr, shard_id = tuple(rest_of_message.split(' ', 2))
			shard_id = int(shard_id)
			status_code = self.cfg["shards"][shard_id]["status"]
			if status_code == READY:
				self.ongoing_list_lock.acquire()
				self.pop_ongoing_list_with_client_addr(shard_id, client_addr)
				self.ongoing_list_lock.release()
			elif status_code == NEW:
				# self.end_add_shard_thread(shard_id)
				for i in xrange(len(self.add_shard_list)):
					if self.add_shard_list[i][2] == shard_id:
						new_shard_id = shard_id
						old_shard_id = self.add_shard_list[i][1]
						self.cfg["shards"][new_shard_id]["status"] = READY
						self.cfg["shards"][old_shard_id]["status"] = READY
						self.add_shard_list.pop(i)

						self.wait_list_lock.acquire()
						self.forward_and_empty_wait_list(old_shard_id)
						self.forward_and_empty_wait_list(new_shard_id)
						self.wait_list_lock.release()
						break

			elif status_code == SHARDING:
				self.ongoing_list_lock.acquire()
				self.pop_ongoing_list_with_client_addr(shard_id, client_addr)
				self.ongoing_list_lock.release()
		else:
			assert False and 'Unknown message'

	def load_config(self, master_config_file):
		with open(master_config_file, 'r') as master_config:
			self.cfg = json.load(master_config)
		for shard_id in xrange(len(self.cfg["shards"])):
			self.cfg["shards"][shard_id]["status"] = READY

	def load_shard_config(self, shard_config_file):
		with open(shard_config_file, 'r') as shard_config:
			self.cfg["shards"].append({
				"leader_id": 0,
				"status": NEW,
				"replicas": json.load(shard_config)
			})

	def get_responsible_shard(self, hash_value):
		for i in xrange(self.num_shards):
			if within_the_range(self.key_range[i][0], self.key_range[i][1], hash_value):
				return i
		print "No responsible shard!"
		return -1

	def forward_message(self, shard_id, message):
		leader_id = self.cfg["shards"][shard_id]["leader_id"]
		host = self.cfg["shards"][shard_id]["replicas"][leader_id][0]
		port = self.cfg["shards"][shard_id]["replicas"][leader_id][1]
		send_message(host, port, message, random)

	def broadcast_message(self, shard_id, message):
		for i in xrange(len(self.cfg["shards"][shard_id]["replicas"])):
			host = self.cfg["shards"][shard_id]["replicas"][i][0]
			port = self.cfg["shards"][shard_id]["replicas"][i][1]
			send_message(host, port, message, random)

	def forward_and_empty_wait_list(self, shard_id):
		while len(self.wait_list[shard_id]) > 0:
			request = self.wait_list[shard_id].pop(0)
			if request.find("AddShard") == -1:
				self.forward_message(shard_id, request)
			else:
				self.cfg["shards"][shard_id]["status"] = SHARDING
				new_shard_id = int(request.split(' ', 4)[3])
				old_shard_id = shard_id
				add_shard_thread = threading.Thread(name="add_shard_" + str(new_shard_id),
												   target=self.add_shard, args=(old_shard_id, new_shard_id, request))
				add_shard_thread.start()
				break

	def find_shard_to_split(self):
		shard_id = -1
		largest_range = -1
		for i in xrange(self.num_shards):
			if self.key_range[i][1] - self.key_range[i][0] >= largest_range:
				shard_id = i
				largest_range = self.key_range[i][1] - self.key_range[i][0]
		return shard_id

	def update_and_get_new_key_range(self, old_shard_id):
		old_begin, old_end = self.key_range[old_shard_id]
		new_begin = (old_end + old_begin)/2
		new_end = old_end
		self.key_range.append((new_begin, new_end))
		self.key_range[old_shard_id] = (old_begin, new_begin)
		print self.key_range
		return new_begin, new_end

	def pop_ongoing_list_with_next_client_seq(self, client_addr, client_seq):
		for shard_id in xrange(self.num_shards):
			for i in xrange(len(self.ongoing_list[shard_id])):
				ongoing_request = self.ongoing_list[shard_id][i]
				type_of_req, host, port, old_client_seq, command = tuple(ongoing_request.split(' ', 4))
				old_client_addr = '{}:{}'.format(host, port)
				old_client_seq = int(old_client_seq)
				if old_client_addr == client_addr and old_client_seq < client_seq:
					self.pop_ongoing_list(shard_id, i)
					return

	def pop_ongoing_list(self, shard_id, list_index):
		self.ongoing_list[shard_id].pop(list_index)
		if self.cfg["shards"][shard_id]["status"] == SHARDING:
			for i in xrange(len(self.add_shard_list)):
				if self.add_shard_list[i][1] == shard_id:
					old_shard_id = shard_id
					new_shard_id = self.add_shard_list[i][2]
					self.notify_ongoing_list_change(old_shard_id, new_shard_id)

	def pop_ongoing_list_with_client_addr(self, shard_id, client_addr):
		pop_host, pop_port = client_addr.split(':', 1)
		for i in xrange(len(self.ongoing_list[shard_id])):
			ongoing_req = self.ongoing_list[shard_id][i]
			type_of_req, host, port, old_client_seq, command = tuple(ongoing_req.split(' ', 4))
			if host == pop_host and port == pop_port:
				self.pop_ongoing_list(shard_id, i)
				return

	def notify_ongoing_list_change(self, old_shard_id, new_shard_id):
		update_msg = "Update {}".format(old_shard_id)
		send_message(self.host, self.port + new_shard_id, update_msg, random)

	# def end_add_shard_thread(self, new_shard_id):
	# 	end_msg = "End {}".format(new_shard_id)
	# 	send_message(self.host, self.port + new_shard_id, end_msg, random)

	def add_shard(self, old_shard_id, new_shard_id, add_shard_request):
		thread_port = self.port + new_shard_id
		viewchange = "ViewChange {} {}".format(self.host, str(thread_port))
		
		print("-------- AddShard {} begins --------".format(new_shard_id))
		# Serve request in ongoing_list
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.bind((self.host, thread_port))
		sock.listen(5)
		sock.settimeout(TIMEOUT)
		timeout = TIMEOUT

		ongoing_list_copy = []

		while True:
			self.ongoing_list_lock.acquire()
			ongoing_list_copy = self.ongoing_list[old_shard_id]
			self.ongoing_list_lock.release()

			if len(ongoing_list_copy) == 0:
				break

			print("-------- AddShard {}: {} ongoing requests remain --------".format(new_shard_id, len(ongoing_list_copy)))
			for ongoing_req in ongoing_list_copy:
				self.forward_message(old_shard_id, ongoing_req)

			while True:
				try:
					message = ''
					start_time = time.time()
					conn, addr = sock.accept()
					while True:
						data = conn.recv(256)
						if not data:
							break
						message += data
					elapsed = time.time() - start_time
					# possible message: 
					# 1. "Update <old_shard_id>"
					# 2. "LeaderIs <shard_id> <leader_id>"
					type_of_message, rest_of_message = tuple(message.split(' ', 1))
					if type_of_message == "Update":
						shard_id = int(rest_of_message)
						if shard_id == old_shard_id:
							sock.settimeout(TIMEOUT)
							timeout = TIMEOUT
							break
						sock.settimeout(max(sock.gettimeout() - elapsed, 0.01))
					elif type_of_message == "LeaderIs":
						shard_id = int(rest_of_message.split(' ', 1)[0])
						if shard_id == old_shard_id:
							for ongoing_req in ongoing_list_copy:
								self.forward_message(old_shard_id, ongoing_req)
							timeout = TIMEOUT
							sock.settimeout(timeout)
						else:
							sock.settimeout(max(sock.gettimeout() - elapsed, 0.01))

				except socket.timeout:
					self.ongoing_list_lock.acquire()
					ongoing_list_copy = self.ongoing_list[old_shard_id]
					self.ongoing_list_lock.release()
					if len(ongoing_list_copy) == 0:
						break
					self.broadcast_message(old_shard_id, viewchange)
					timeout *= 2
					sock.settimeout(timeout)
					
		print("-------- AddShard {} empties ongoing_list --------".format(new_shard_id))
		# Upon emptying the ongoing_list
		self.forward_message(old_shard_id, add_shard_request)
		sock.settimeout(TIMEOUT)
		timeout = TIMEOUT
		leaderIs_received = []

		while True:
			try:
				start_time = time.time()
				message = ''
				conn, addr = sock.accept()
				while True:
					data = conn.recv(256)
					if not data:
						break
					message += data
				elapsed = time.time() - start_time
				# possible message: 
				# 1. "End <new_shard_id>"
				# 2. "LeaderIs <shard_id> <replica_id>"
				type_of_message, rest_of_message = tuple(message.split(' ', 1))
				if type_of_message == "LeaderIs":
					shard_id = int(rest_of_message.split(' ', 1)[0])
					if (shard_id == new_shard_id or shard_id == old_shard_id) and\
							shard_id not in leaderIs_received:
						leaderIs_received.append(shard_id)
					if len(leaderIs_received) == 2:
						leaderIs_received = []
						self.forward_message(old_shard_id, add_shard_request)
						timeout = TIMEOUT
						sock.settimeout(timeout)
					else:
						sock.settimeout(max(sock.gettimeout() - elapsed, 0.01))
				# elif type_of_message == "End":
				# 	shard_id = int(rest_of_message)
				# 	if shard_id == new_shard_id:
				# 		break
			except socket.timeout:
				if self.cfg["shards"][new_shard_id]["status"] != NEW:
					break
				if old_shard_id not in leaderIs_received:
					self.broadcast_message(old_shard_id, viewchange)
				if new_shard_id not in leaderIs_received:
					self.broadcast_message(new_shard_id, viewchange)
				timeout *= 2
				sock.settimeout(timeout)
		print("-------- AddShard {} completes --------".format(new_shard_id))

if __name__ == '__main__':
	master_config_file = sys.argv[1]
	master = Master(master_config_file)
	master.run()