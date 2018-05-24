import socket
import threading
import sys
import MySQLdb



#---- INIT ----
if len(sys.argv) < 2 :
	bind_ip = '0.0.0.0'
	bind_port = 10000


elif len(sys.argv) < 3 :
	bind_ip = sys.argv[1]
	bind_port = 10000

else :
	bind_ip = sys.argv[1]
	bind_port = int(sys.argv[2])

#---- functions ----

def handle_client_connection(client_socket):
	request = client_socket.recv(1024)
	print 'Received data {}'.format(request)

	client_socket.send('ACK from server {} !'.format(bind_ip))
	client_socket.close()


#---- Server socket ----

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((bind_ip, bind_port))
server.listen(5)  # max backlog of connections

print 'Listening on {}:{}'.format(bind_ip, bind_port)

#---- MAIN LOOP ----

while True:
	client_sock, address = server.accept()
	print 'Accepted connection from {}:{}'.format(address[0], address[1])


	client_handler = threading.Thread(
		target=handle_client_connection,
		args=(client_sock,)  
	)

	client_handler.start()



