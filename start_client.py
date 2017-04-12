import paxos_client, sys
from config import *

def start_client(client_id):
	client = paxos_client.Paxos_client(client_id, CLIENT_ADDR[client_id][0], CLIENT_ADDR[client_id][1])
	client.start()

if __name__ == '__main__':
	client_id = int(sys.argv[1])
	start_client(client_id)