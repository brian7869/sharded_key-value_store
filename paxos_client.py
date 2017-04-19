from multiprocessing import Process
import socket, sys, random, time
from paxos_utils import send_message, send_message_without_failure
from config import MASTER_HOST, MASTER_PORT, TIMEOUT, CLIENT_ADDR

ACK = 1
SKIP = 2
SEED = int(time.time())
print 'SEED is {}'.format(SEED)
random.seed(SEED)

# send request to known leader, if timeout, ask all replicas WhoIsLeader and pick one with f+1
def run(client_id, host, port):
	# send message format	
	# "Request <host> <port_number> <client_seq> <command>"
	port = int(port)
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.bind((host, port))
	sock.listen(5)
	sock.settimeout(TIMEOUT)
	timeout = TIMEOUT
	client_seq = -1

	while True:
		# time.sleep(random())
		client_seq += 1
		user_input = raw_input("Type in request: (1) Get <key> (2) Put <key> <value> (3) Delete <key> (4) AddShard <configFileName>\n")
		request = user_input.rstrip()
		if request.find("AddShard") != -1:
			send_message(MASTER_HOST, MASTER_PORT, request, random)
		else:
			req_message = "Request {} {} {} {}".format(host, str(port)
						, str(client_seq), request)
			send_message_without_failure(MASTER_HOST, MASTER_PORT, req_message)
			while True:
				# debug_print(client_id, 'wait for message')
				try:
					start_time = time.time()
					conn, addr = sock.accept()
					message = ''
					while True:
						data = conn.recv(256)
						if not data:
							break
						message += data
					elapsed = time.time() - start_time
					status = message_handler(message, client_seq)
					if status == ACK:
						timeout = TIMEOUT
						sock.settimeout(TIMEOUT)
						break
					elif status == SKIP:
						sock.settimeout(max(sock.gettimeout() - elapsed, 0.01))
					else:
						assert False and 'Unknown message'
						# debug_print(client_id, 'resend request: '+self.commands[client_seq])
						# send_message(MASTER_HOST, MASTER_PORT, req_message)
						# sock.settimeout(TIMEOUT)
				except socket.timeout:
					# debug_print(client_id, 'send viewchange: '+str(client_seq))
					message = "ViewChange {}".format(req_message)
					timeout *= 2
					sock.settimeout(timeout)
					send_message_without_failure(MASTER_HOST, MASTER_PORT, message)

def message_handler(message, client_seq):
	# Possible messages
	# From primary:
	# "Reply <client_seq>"
	# "LeaderIs <leader_id>"
	# return True if command succeeded
	type_of_message, rest_of_message = tuple(message.split(' ', 1))

	if type_of_message == 'Reply' and client_seq == int(rest_of_message.split(' ', 1)[0]):
		print(rest_of_message.split(' ', 1)[1])
		return ACK
	else:
		return SKIP

def debug_print(client_id, msg):
	print 'client ' + str(client_id) + ': '+msg

if __name__ == '__main__':
	client_id = int(sys.argv[1])
	run(client_id, CLIENT_ADDR[client_id][0], CLIENT_ADDR[client_id][1])
