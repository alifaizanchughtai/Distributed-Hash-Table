import socket 
import threading
import os
import time
import hashlib
import queue


class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.successor = (self.host, self.port)
		self.predecessor = (host, port)
		# additional state variables
		
		threading.Thread(target=self.pinging).start()
		self.portsQueue = queue.Queue()
		self.hostsQueue = queue.Queue()
		self.srcQueue = queue.Queue()
		self.filesQueue = queue.Queue()
		self.nextSuccessor = (self.host, self.port)
		# self.predecessor = (self.host, self.port)


 
	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N


	def handleConnection(self, client, addr):
		try:
			data = client.recv(1024).decode('utf-8')
			soc = socket.socket()
			split = data.split()

			if split[0] == "send_succ":
				key = int(split[2])
				where = [self.lookup(key)]
				soc.connect((self.host, int(split[1])))
				message = f"succ_ret {split[1]} {where[0][0]} {where[0][1]}"
				soc.send(message.encode('utf-8'))
				soc.close()

			elif split[0] == "succ_ret":
				self.hostsQueue.put(split[2])
				self.portsQueue.put(split[3])
				client.close()

			elif split[0] ==  "succ_upd":
				self.successor = (split[1], int(split[2]))
				client.close()

			elif split[0] == "pred_upd":
				message = f"succ_ret {self.port} {self.predecessor[0]} {self.predecessor[1]}"
				self.predecessor = (split[1], int(split[2]))
				soc.connect(self.predecessor)
				soc.send(message.encode('utf-8'))
				soc.close()

				soc = socket.socket()
				soc.connect(self.predecessor)
				message = f"succ_jmp {self.successor[0]} {self.successor[1]} "
				soc.send(message.encode('utf-8'))
				soc.close()

				self.backUpFiles.clear()

			elif split[0] == "succ_jmp":
				self.nextSuccessor = (split[1],int(split[2]))

			elif split[0] == "file_placement":
				fileName = split[1]
				self.files.append(fileName)
				path = f"{os.getcwd()}/{self.host}_{self.port}/{fileName}"
				self.recieveFile(client, path)
				message = f"file_placement_backup {fileName}"
				path = f"{os.getcwd()}/{self.host}_{self.port}/{fileName}"
				soc.connect(self.predecessor)
				message = message.encode('utf-8')
				soc.send(message)
				time.sleep(0.1)
				self.sendFile(soc, path)

			elif split[0] == "ret_file":
				putting = split[1]
				self.hostsQueue.put(putting)

			elif split[0] == "file_placement_backup":
				fileName = split[1]
				self.backUpFiles.append(fileName)
				path = f"{os.getcwd()}/{self.host}_{self.port}/{fileName}"
				self.recieveFile(client,path)

			elif split[0] == "file_check":
				fileName = split[1]
				message = f"ret_file {fileName if fileName in self.files else 'None'}"
				soc.connect((self.host, int(split[2])))
				soc.send(message.encode('utf-8'))

			elif split[0] == "sending_file":
				key = int(split[1])
				for idx in self.files:
					sendKey = self.hasher(idx)
					if (sendKey < key and sendKey > int(split[3])) or (int(split[3]) > key and sendKey < key):
						path = f"{os.getcwd()}/{self.host}_{self.port}/{idx}"
						message = f"file_placement {idx}"
						soc = socket.socket()
						soc.connect((self.host, int(split[2])))
						message = message.encode('utf-8') 
						soc.send(message)
						time.sleep(0.1)
						self.sendFile(soc, path)
						soc.close()
		except:
			pass



	def lookup(self, key):
		succHash = self.hasher(self.successor[0] + str(self.successor[1]))
		predHash = self.hasher(self.predecessor[0] + str(self.predecessor[1]))

		if key == self.key:
			return (self.host, self.port)

		if self.key < succHash < key or succHash < key < self.key or key < self.key < succHash:
			soc = socket.socket()
			soc.connect(self.successor)
			message = f"send_succ {self.port} {key}"
			soc.send(message.encode('utf-8'))
			soc.close()

			while True:
				try:
					host = str(self.hostsQueue.get())
					port = int(self.portsQueue.get())
					break
				except:
					pass
			return (host, port)

		elif predHash > self.key > key and succHash > self.key:
			return (self.host, self.port)

		else:
			return self.successor

	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shut Down Node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()


	
	def join(self, joiningAddr):
		try:
			host, port = joiningAddr

			with socket.socket() as soc:
				soc.connect((host, port))
				message = f"send_succ {self.port} {self.key}"
				soc.send(message.encode('utf-8'))
				self.successor = self.hostsQueue.get(), int(self.portsQueue.get())

				with socket.socket() as soc:
					soc.connect(self.successor)
					message = f"pred_upd {self.host} {self.port}"
					soc.send(message.encode('utf-8'))
					self.predecessor = self.hostsQueue.get(), int(self.portsQueue.get())

				with socket.socket() as soc:
					soc.connect(self.predecessor)
					message = f"succ_upd {self.host} {self.port}"
					soc.send(message.encode('utf-8'))

				with socket.socket() as soc:
					soc.connect(self.successor)
					message = f"sending_file {self.key} {self.port} {self.hasher(self.predecessor[0] + str(self.predecessor[1]))}"
					soc.send(message.encode('utf-8'))
		except Exception as e:
			# print(f"Error during join: {e}")
			pass


	def put(self, fileName):
		key = self.hasher(fileName)
		host, port = self.lookup(key)
		foundAddr = (host, port)
		soc = socket.socket()
		message = f"file_placement {fileName}"
		soc.connect(foundAddr)
		message = message.encode('utf-8')
		soc.send(message)
		time.sleep(0.1)
		self.sendFile(soc, fileName)
		soc.close()



	def get(self, fileName):
		host, port = self.lookup(self.hasher(fileName))
		foundAddr = (host, port)
		soc = socket.socket()
		message = f"file_check {fileName} {self.port}"
		soc.connect(foundAddr)
		soc.send(message.encode('utf-8'))

		while True:
			try:
				file_in = str(self.hostsQueue.get())
				break
			except:
				pass
		soc.close()
		return fileName if file_in == fileName else None

 
	def leave(self):
		def send_message(soc, host_port, message):
			soc.connect(host_port)
			soc.send(message.encode('utf-8'))
			soc.close()

		send_message(socket.socket(), self.successor, f"pred_upd {self.predecessor[0]} {self.predecessor[1]}")
		send_message(socket.socket(), self.predecessor, f"succ_upd {self.successor[0]} {self.successor[1]}")

		for idx in self.files:
			soc = socket.socket()
			message = f"file_placement {idx}"
			path = f"{os.getcwd()}/{self.host}_{self.port}/{idx}"
			soc.connect(self.successor)
			soc.send(message.encode('utf-8'))
			time.sleep(0.1)
			self.sendFile(soc, path)
			soc.close()

		self.stop = True


	def pinging(self):
		while not self.stop:
			isSuccessor = False
			if self.successor[1] != self.port: 
				nodenum = 0
				while nodenum < 2:
					soc = socket.socket()
					try:
						soc.connect(self.successor)
						message = "pinging".encode('utf-8')
						soc.send(message)
						isSuccessor = True
						break
					except:
						nodenum += 1
						isSuccessor = False
				if not isSuccessor:
					self.successor = self.nextSuccessor
					soc = socket.socket()
					soc.connect(self.nextSuccessor)
					message = f"pred_upd {self.host} {self.port}"
					message = message.encode('utf-8')
					soc.send(message)
					soc.close()
					for idx in self.backUpFiles:
						soc = socket.socket()
						message = f"file_placement {idx}"
						path = f"{os.getcwd()}/{self.host}_{self.port}/{idx}"
						soc.connect(self.successor)
						message = message.encode('utf-8')
						soc.send(message)
						time.sleep(0.1)
						self.sendFile(soc, path)
						soc.close()
			time.sleep(0.5)


	def sendFile(self, soc, fileName):
		''' 
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)


	def recieveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True
	