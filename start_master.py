import master_node, sys

def start_master(master_config_file):
	master = master_node.Master(master_config_file)
	master.start()

if __name__ == '__main__':
	master_config_file = sys.argv[1]
	start_master(master_config_file)