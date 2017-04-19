import random, sys

prob_of_get = 0.4
prob_of_put = 0.4

def random_request(client_id, seq):
	random_for_type = random.random()
	if random_for_type < prob_of_get:
		return 'Get {} {}_{}\n'.format(random.randint(0,15), client_id, seq)
	elif random_for_type >= prob_of_get and random_for_type < prob_of_get + prob_of_put:
		return 'Put {} {}_{}\n'.format(random.randint(0,15), client_id, seq)
	else:
		return 'Delete {} {}_{}\n'.format(random.randint(0,15), client_id, seq)
	seq += 1

if __name__ == '__main__':
	num_request = int(sys.argv[1])
	client_id = int(sys.argv[2])
	file_name = sys.argv[3]
	with open(file_name, 'w') as f:
		for i in xrange(num_request):
			f.write(random_request(client_id, i))