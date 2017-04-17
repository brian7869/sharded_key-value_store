import random, sys

prob_of_get = 0.4
prob_of_put = 0.4

def random_request():
	random_for_type = random.random()
	if random_for_type < prob_of_get:
		return 'Get {}\n'.format(random.randint(100,109))
	elif random_for_type >= prob_of_get and random_for_type < prob_of_get + prob_of_put:
		return 'Put {} {}\n'.format(random.randint(100,109), random.randint(0, 99))
	else:
		return 'Delete {}\n'.format(random.randint(100,109))

if __name__ == '__main__':
	num_request = int(sys.argv[1])
	file_name = sys.argv[2]
	with open(file_name, 'w') as f:
		for i in xrange(num_request):
			f.write(random_request())
