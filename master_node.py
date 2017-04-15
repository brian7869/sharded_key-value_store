import json, threading, socket, sys
from multiprocessing import Process, Lock
from paxos_utils import json_spaceless_dump, get_hash_value, send_message, within_the_range
from config import *
import time

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
		print("*** receiving message: {} ***".format(message))
		type_of_message, rest_of_message = tuple(message.split(' ', 1))

		if type_of_message == "Request":
			command = rest_of_message.split(' ', 3)[3]
			key = command.split(' ')[1]
			hash_value = get_hash_value(key)
			print("key: {}, hash_value: {}".format(key, str(hash_value)))
			shard_id = self.get_responsible_shard(hash_value)
			if shard_id != -1:
				self.wait_list_lock.acquire()
				if len(self.wait_list[shard_id]) > 0:
					self.wait_list[shard_id].append(message)
				else:
					self.forward_message(shard_id, message)
				self.wait_list_lock.release()

		elif type_of_message == "ViewChange":
			request_componenets = rest_of_message.split(' ', 4)
			command = request_componenets[4]
			key = command.split(' ')[1]
			hash_value = get_hash_value(key)
			print("key: {}, hash_value: {}".format(key, str(hash_value)))
			shard_id = self.get_responsible_shard(hash_value)
			if shard_id != -1:
				# Thinking about how it would influence the inner data structure of this shard and
				# Waht if there are multiple ViewChange?
				self.wait_list_lock.acquire()
				if self.cfg["shards"][shard_id]["status"] == NEW:
					self.wait_list[shard_id].append(rest_of_message)
				else:
					viewchange_message = "ViewChange {} {} {}".format(request_componenets[1], request_componenets[2], request_componenets[3])
					self.wait_list[shard_id].append(rest_of_message)
					self.broadcast_message(shard_id, viewchange_message)
				self.wait_list_lock.release()

		elif type_of_message == "LeaderIs":
			shard_id, leader_id = tuple(rest_of_message.split(' ', 2))
			shard_id, leader_id = int(shard_id), int(leader_id)
			self.cfg["shards"][shard_id]["leader_id"] = leader_id
			self.wait_list_lock.acquire()
			if self.cfg["shards"][shard_id]["status"] != NEW:
				self.forward_and_empty_wait_list(shard_id)
			self.wait_list_lock.release()

		# TODO: To seek a better way to send AddShard request
		elif type_of_message == "AddShard":
			old_shard_id = self.find_shard_to_split()
			begin, end = self.update_and_get_new_key_range(old_shard_id)
			self.num_shards += 1
			shard_config_file = rest_of_message
			self.load_shard_config(shard_config_file)
			self.wait_list_lock.acquire()
			self.wait_list.append([])
			self.wait_list_lock.release()
			new_shard_id = self.num_shards - 1
			add_shard_thread = threading.Thread(name="add_shard_" + str(new_shard_id),
												   target=self.add_shard, args=(old_shard_id, new_shard_id, begin, end))
			add_shard_thread.start()
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
		send_message(host, port, message)

	def broadcast_message(self, shard_id, message):
		for i in xrange(len(self.cfg["shards"][shard_id]["replicas"])):
			host = self.cfg["shards"][shard_id]["replicas"][i][0]
			port = self.cfg["shards"][shard_id]["replicas"][i][1]
			send_message(host, port, message)

	def forward_and_empty_wait_list(self, shard_id):
		for i in xrange(len(self.wait_list[shard_id])):
			self.forward_message(shard_id, self.wait_list[shard_id][i])
		self.wait_list[shard_id] = []

	def find_shard_to_split(self):
		shard_id = -1
		largest_range = -1
		for i in xrange(self.num_shards):
			if self.key_range[i][1] - self.key_range[i][0] > largest_range:
				shard_id = i
				largest_range = self.key_range[i][1] - self.key_range[i][0]
		return shard_id

	def update_and_get_new_key_range(self, old_shard_id):
		old_begin, old_end = self.key_range[old_shard_id]
		new_begin = (old_end - old_begin)/2
		new_end = old_end
		self.key_range.append((new_begin, new_end))
		self.key_range[old_shard_id] = (old_begin, new_begin)
		return new_begin, new_end

	# TODO: settimeout and resend
	def add_shard(self, old_shard_id, new_shard_id, begin, end):
		new_shard_address = json_spaceless_dump(self.cfg["shards"][new_shard_id]["replicas"])
		add_shard_command = "AddShard {} {} {}".format(str(begin), str(end), new_shard_address)
		thread_port = self.port + new_shard_id
		request = "Request {} {} {} {}".format(self.host, str(thread_port), str(new_shard_id), add_shard_command)
		viewchange = "ViewChange {} {}".format(self.host, str(thread_port))
		self.forward_message(old_shard_id, request)
		message_sent = request

		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.bind((self.host, thread_port))
		sock.listen(5)
		sock.settimeout(TIMEOUT)

		# TODO: Send viewchange upon timeout and need to handle LeaderIs message and resend requests.
		while True:
			try:
				message = ''
				conn, addr = sock.accept()
				while True:
					data = conn.recv(256)
					if not data:
						break
					message += data
				# possible message: "Reply 0 <replica_id>"
				type_of_message, rest_of_message = tuple(message.split(' ', 1))
				if type_of_message == "Reply":
					client_seq, replica_id = rest_of_message.split(' ', 1)
					self.wait_list_lock.acquire()
					self.cfg["shards"][new_shard_id]["status"] = READY
					self.forward_and_empty_wait_list(new_shard_id)
					self.wait_list_lock.release()
					break
			except socket.timeout:
				if message_sent == request:
					self.forward_message(new_shard_id, viewchange)
					self.forward_message(old_shard_id, viewchange)
					sock.settimeout(sock.gettimeout() * 2)
					message_sent = viewchange
				else:
					self.forward_message(old_shard_id, request)
					sock.settimeout(sock.gettimeout())
					message_sent = request

if __name__ == '__main__':
	master_config_file = sys.argv[1]
	master = Master(master_config_file)
	master.run()