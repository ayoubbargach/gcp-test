import socket
import sys

#---- INIT ----
if len(sys.argv) < 2 :
	ip = '0.0.0.0'
	port = 10000


elif len(sys.argv) < 3 :
	ip = sys.argv[1]
	port = 10000

else :
	ip = sys.argv[1]
	port = int(sys.argv[2])


# create an ipv4 (AF_INET) socket object using the tcp protocol (SOCK_STREAM)
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# connect the client
# client.connect((target, port))
client.connect((ip, port))

# send some data 
client.send('TEST')

# receive the response data (4096 is recommended buffer size)
response = client.recv(4096)

print response
