import aiohttp
import asyncio
import json
import time
import sys


API_KEY = "AIzaSyBSVMu8_UyOtRYSeRwVlOQ-Dh1Ygfc4JjQ"

communication_graph = {
	'Goloman': ['Hands', 'Holiday', 'Wilkes'],
	'Hands': ['Goloman', 'Wilkes'],
	'Holiday': ['Goloman', 'Welsh'],
	'Welsh': ['Holiday'],
	'Wilkes': ['Goloman', 'Hands']
}

server_names=['Goloman', 'Hands', 'Holiday', 'Welsh', 'Wilkes']

port_num = { 
	'Goloman' : 11590, 
	'Hands' : 11591,
	'Holiday' : 11592,
	'Welsh' : 11593,
	'Wilkes' : 11594
}

#dictionary: key==client id :: value==["location", float(time_sent), int(updates), "received_from"]
client_list={}			


#send UPDATE message to other servers other than receieved_from.
async def flood_servers(c_id, received_from):
	for server in communication_graph[server_name]:
		#https://docs.python.org/3/library/asyncio-stream.html
		#flooding client information to neighbor servers other than visited
		#msg = "UPDATE client_id location time update sender"
		if(server != received_from):
			try:
				msg = "UPDATE {0} {1} {2} {3} {4}\n".format(c_id, client_list[c_id][0],  client_list[c_id][1], client_list[c_id][2], client_list[c_id][3])
				reader, writer = await asyncio.open_connection('127.0.0.1', port_num[server])								
				log_file.write("Connected to " + server + "\n")
				log_file.write("Sending... " + msg )
				writer.write(msg.encode())
				await writer.drain()
				writer.close()
			except:
				log_file.write("NO connection to " + server + "\n")
				pass


#check command of msg
async def check_msg(msg):
	command = msg[0]
	
	if (command == "IAMAT"):
		"""	
		IAMAT returns 1 if true command syntax
		check if correct length and valid location 
		"""
		if(len(msg) != 4):
			return 0
		else:
			if (msg[2][0] != "-" and msg[2][0] != "+"):
				return 0
			else:
				loc_len = len(msg[2])
				check_neg = 0
				for i in range(1, loc_len):
					if (msg[2][i] == "-" or	msg[2][i] == "+"):
						check_neg += 1
						if check_neg != 1:
							return 0
						else:
							return 1
	elif (command == "WHATSAT"):
		"""
		WHATSAT returns 2 if true
		check if correct length and the range of radius and information Bound
		"""
		if(len(msg) != 4):
			return 0
		else:
			if (msg[1] not in client_list):
				return 0
			else:
				int_radius = int(msg[2])
				int_infoBound = int(msg[3])
				if((int_radius < 0 or int_radius >50) or (int_infoBound < 0 or int_infoBound > 20)):
					return 0
				else:
					return 2

	elif (command == "UPDATE"):
		"""
		UPDATE returns 3
		"""
		return 3

	else:
		"""
		Unrecognizable Command returns 0
		"""
		return 0


async def update_client(msg):
	"""
	this is handler for message received from other server.
	msg:: UPDATE id location time update_number received_from
	"""
	c_id = msg[1]
	c_loc=msg[2]
	c_time = float(msg[3])
	c_num = int(msg[4])
	
	#if client is already in the list
	if(c_id in client_list):
		#if received the client info with same location before
		#also flood
		if( c_num  > client_list[c_id][2]):
			client_list[c_id] = [c_loc, c_time, c_num, server_name]
			asyncio.ensure_future(flood_servers(c_id, msg[5]))
			#else:
			#asyncio.ensure_future(flood_servers(c_id))
	else:
		client_list[c_id] = [c_loc, c_time, c_num, server_name]
		asyncio.ensure_future( flood_servers(c_id, msg[5]) )


async def whatsat_handler(msg, time_stp, writer):
	"""
	handle WHATSAT command
	...replace + and - sign so the location can be used for url
	...json.dumps is used to beautify the result
	"""
	c_id = msg[1]
	c_radius = int(msg[2]) * 1000
	c_infoBound = int(msg[3])

	#location change format
	loc = client_list[c_id][0]
	loc_str = ""
	for count, item in enumerate(loc):
		if (count == 0 and item == "+"):
			loc_str += ""
		elif (item == "+"):
			loc_str += ","
		elif (item == "-"):
			loc_str = loc_str + "," + item
		else:
			loc_str += item

			#url to make request to Google Place
	url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={0}&radius={1}&key={2}".format(loc_str, c_radius, API_KEY)
	
	#time difference and if possitive add +
	time_diff = time_stp - float(client_list[c_id][1])
	time_diff_str = str(time_diff)
	if(time_diff > 0):
		time_diff_str = "+" + time_diff_str				

	#output_msg = AT server time_difference client location time
	output_msg = "AT {0} {1} {2} {3} {4}\n".format(server_name, time_diff_str, msg[1], msg[2], msg[3])

	#make HTTP request to Google Places for nearby information
	#https://www.programcreek.com/python/example/84343/aiohttp.ClientSession
	async with aiohttp.ClientSession() as session:
		async with session.get(url) as resp: 
			data = await resp.json()
			new_results = data['results'][:c_infoBound]
			data['results'] = new_results
			output_msg += json.dumps(data, indent=3)
			output_msg += "\n"	
			
	#output result to log and client
	log_file.write("Sending... " + output_msg)
	writer.write(output_msg.encode())
	await writer.drain()



async def iamat_handler(msg, time_stp, writer):
	"""
	handles IAMAT command
	...if client already exists then increment the index number to indicate it's a new information
	...add the client to client_list
	...flood the servers with client's information
	...log the output
	"""
	c_id = msg[1]
	c_loc = msg[2]
	c_time = float(msg[3])
	
	#check if client is in the client_list	
	if(c_id in client_list):
		c_new_num = client_list[c_id][2] + 1
		client_list[c_id] = [c_loc, c_time, c_new_num, server_name]
	else:
		client_list[c_id] = [c_loc, c_time, 1, server_name]
	
	#calculate time difference
	time_diff = time_stp - c_time
	time_diff_str = str(time_diff)
	if(time_diff > 0):
		time_diff_str = "+" + time_diff_str
		
	#flood and output
	asyncio.ensure_future(flood_servers(c_id, server_name))
	output_str = "AT {0} {1} {2} {3} {4}\n".format(server_name, time_diff_str, msg[1], msg[2], msg[3])
	log_file.write("Sending... " + output_str)
	writer.write(output_str.encode())
	await writer.drain()




async def handle_input(reader, writer):
	"""
	handles the message and operates depend on which command:IAMAT, WHATSAT, and UPDATE
	...read message and put each field into an array
	...check the command and operate the message acccordingly
	"""
	#message manipulate
	data = await reader.readline()
	msg_sentence = data.decode()
	msg = msg_sentence.split()
	
	#get received time
	time_stp = time.time()
	
	
	#check command and log message
	command_num = await(check_msg(msg))
	log_file.write("Received... " + msg_sentence)
	if(command_num == 0):
		error_msg = "? " + msg_sentence
		writer.write(error_msg.encode())
		await writer.drain()
		log_file.write("Sending... " + error_msg)
				
	#IAMAT
	if(command_num == 1):
		task1 = iamat_handler(msg, time_stp, writer)
		await(task1)
	#WHATSAT
	elif(command_num == 2):
		task2 = whatsat_handler(msg, time_stp, writer)#asyncio.create_task(whatsat_handler(msg, time_stp))
		await(task2)
	#UPDATE
	elif(command_num == 3): 
		task3 = update_client(msg)#asyncio.create_task(update_client(msg))
		await(task3)
		

def main():
	#check the argument length
	if len(sys.argv) != 2:
		print("Usage: python3 server.py [Server Name]")
		sys.exit(1)
	#check server name
	global server_name
	server_name = sys.argv[1]
	if server_name not in server_names:
		print("Invalid server name... Names: Goloman, Hands, Holiday, Welsh, orWilkes")  
		sys.exit(1)
		
	# log file
	global log_file 
	log_file = open(server_name + "_log.txt", "w+")

	#server implementation with asyncio
	#https://asyncio.readthedocs.io/en/latest/tcp_echo.html
	loop = asyncio.get_event_loop()
	coro = asyncio.start_server(handle_input, '127.0.0.1', port_num[server_name], loop=loop)
	server = loop.run_until_complete(coro)

	try:
		loop.run_forever()
	except KeyboardInterrupt:
		pass

	server.close()
	loop.run_until_complete(server.wait_closed())
	loop.close()
	log_file.close()

if __name__== '__main__':
	main()
