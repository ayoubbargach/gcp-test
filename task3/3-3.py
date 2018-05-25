#! /usr/bin/python3
# -*- coding: utf-8 -*-
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import Forbidden
from matplotlib import pyplot as plt
import io
import os
import time
import threading


#---- INIT ----

# Init global variables
bucket_name = 'noyau-gcptest'
number_tests = 5

# Instantiates a client
storage_client = storage.Client.from_service_account_json('../key.json')



#---- CLASSES ----

# We create a class to manipulate our bucket

class Bkt:
	""" Functional class to manage efficiently all the common operations.
	If the bucket do not exist, this constructor create it in the US region with multiregional storage class.
	Google API for more information """

	def __init__(self, bucket_name, n):

		""" Create a new bucket if does not exist
		N correspond to the wanted number of buffers """
		try:
			self.bucket = storage_client.get_bucket(bucket_name)
		except NotFound :
			self.bucket = storage_client.create_bucket(bucket_name)
			print('Bucket {} created'.format(self.bucket.name))
		except Forbidden :
			print('Seems that the bucket name {} is already taken'.format(bucket.name))
		finally:
			print('connected to the bucket {}'.format(bucket_name))
		
		self.name = bucket_name
		
		# Start the wanted number of buffers
		self.buffer = []

		for i in range(n):
			self.buffer.append(io.BytesIO())

	#-- Buffer methods --

	def write_file_buffer( self, file_object, i ):
		# The idea here is to use this function to add the file data to the memory buffer.
		# file_object is the file object of the proposed file

		# For testing read on local SSD, we should test the timing of this method

		# clean buffer first
		self.clean_buffer(i)
		
		self.buffer[i].write( file_object.read() )
		
		print( "New file added to the buffer" )

	def write_buffer_file( self, file_object, i ):
		# WARNING there is two methods, write_buffer_file and write_file_buffer depenging on the direction

		self.buffer[i].seek(0)
		
		file_object.write( self.buffer[i].read() )
		
		print( "The content of the buffer have been append to the file" )

	def write_blob_buffer( self, blob_name, i ):
		# clean buffer first
		self.clean_buffer(i)

		# download in the buffer

		self.get_blob_as_file( blob_name, self.buffer[i] )

		print( "New blob added to the buffer" )

	def write_buffer_blob( self, blob_name, i ):
		# upload from the buffer

		self.buffer[i].seek(0)
		
		self.add_blob_from_file( blob_name, self.buffer[i] )

		print( "New blob added from the buffer" )

	def get_buffer( self, i ):
		return self.buffer[i]

	def clean_buffer( self, i ):
		self.buffer[i] = io.BytesIO()

	#-- GENERAL methods --

	def add_blob(self, blob_name, file_name):
		blob = self.bucket.blob(blob_name)
		blob.upload_from_filename(file_name)

		print('File {} uploaded as {}'.format(file_name, blob_name))

	def add_blob_from_file(self, blob_name, file_object):
		blob = self.bucket.blob(blob_name)

    		blob.upload_from_file(file_object)

		print('File stream uploaded as {}'.format( blob_name ))

	def add_blob_from_string(self, blob_name, str_value):
		blob = self.bucket.blob(blob_name)
		blob.upload_from_string(str_value)

		print('String {} uploaded as {}'.format(str_value, blob_name))
	
	def get_blob(self, blob_name, file_name):
		blob = self.bucket.blob(blob_name)
		blob.download_to_filename(file_name)

		print('File {} downloaded from {}'.format(file_name, blob_name))

	def get_blob_as_file(self, blob_name, file_object):
		blob = self.bucket.blob(blob_name)

		blob.download_to_file(file_object)

		print('File stream downloaded from {}'.format( blob_name))

		return file_object

	def get_blob_as_string(self, blob_name):
		blob = self.bucket.blob(blob_name)
		str_value = blob.download_as_string()

		print('String {} downloaded from {}'.format(str_value, blob_name))

	def get_all_blobs(self):
		"""Lists all the blobs in the bucket."""
		return self.bucket.list_blobs()

	def delete_all_blobs(self):
		""" Delete all blobs of the experimentation """
		blobs = self.get_all_blobs()
		for blob in blobs :
			blob.delete()
		print("All blobs of the bucket have been deleted !")

	def get_bkt(self):
		return self.bucket

	def get_name(self):
		return self.name


#---- FUNCTIONS ----

def get_latency( timestamp1, timestamp2 ):
	return (timestamp2 - timestamp1) * 1000

def get_bandwidth( file_size, timestamp1, timestamp2 ):
	return round( file_size / (timestamp2 - timestamp1) )

def draw( data, title, XLabel, YLabel ):
	index = range( 1, 3 )
	average_tab = []

	average_up = sum(data[:number_tests]) / len(data[:number_tests])
	average_down = sum(data[number_tests:]) / len(data[number_tests:])

	print("Average of up -> "+title+" = "+str(average_up))
	print("Average of down <- "+title+" = "+str(average_down))
	print("Max = "+str(max(data)))
	print("Min = "+str(min(data)))

	average_tab.append(average_up)
	average_tab.append(average_down)
	
	print(data)
	plt.bar(index, average_tab, width=0.5, color='r')
	plt.title(title)
	plt.xlabel(XLabel)
	plt.ylabel(YLabel)
	plt.savefig(title+".png")
	plt.close()

#---- MAIN ----

def main():
	""" Main function """

	# We start by initiating the connection and some variables
	b = Bkt(bucket_name, 4)

	threads = []
	latency_result = []
	seq_bandwidth_result = []
	par_bandwidth_result = []

	seq_bandwidth_result_local = []	
	par_bandwidth_result_local = []
	seq_bandwidth_result_from_buffer = []
	par_bandwidth_result_from_buffer = []

	# Sizes in KB
	file_size1 = os.path.getsize( "files/file1.data" )
	file_size2 = os.path.getsize( "files/file2.data" )


	#--- Sequential ---
	print("Start sequencial tests...")
	# Up
	for i in range( 0, number_tests ):
		# latency can be directly simulated in the sequencial part
		timestamp1 = time.time()
		b.add_blob_from_string( "latency", "T")
		timestamp2 = time.time()

		latency_result.append( get_latency( timestamp1, timestamp2 ))

		# bandwidth from local SSD to cloud		
		timestamp1 = time.time()
		b.add_blob( "file2"+str(i), "files/file2.data")
		timestamp2 = time.time()

		seq_bandwidth_result.append( get_bandwidth( file_size2, timestamp1, timestamp2 ))

		# TEST local disk to buffer
		timestamp1 = time.time()
		
		with open( "files/file2.data", "r") as f:
			b.write_file_buffer( f, 0 )
		
		timestamp2 = time.time()

		seq_bandwidth_result_local.append( get_bandwidth( file_size2, timestamp1, timestamp2 ))
		
		# Second, TEST buffer to blob

		timestamp1 = time.time()

		b.write_buffer_blob( "buffer_file2"+str(i), 0 )
		
		timestamp2 = time.time()

		seq_bandwidth_result_from_buffer.append( get_bandwidth( file_size2, timestamp1, timestamp2 ))

	# Down
	for i in range( 0, number_tests ):
		# latency can be directly simulated in the sequencial part
		timestamp1 = time.time()
		b.get_blob_as_string( "latency")
		timestamp2 = time.time()

		latency_result.append( get_latency( timestamp1, timestamp2 ))

		# bandwidth		
		timestamp1 = time.time()
		b.get_blob( "file2"+str(i), "files/return"+str(i)+".data" )
		timestamp2 = time.time()
		if os.path.getsize( "files/return"+str(i)+".data" ) == file_size2 :
			print( "Number of bytes correct : Download OK" )
		else :
			print( "Download uncomplete" )
		# os.remove( "files/return"+i+".jpeg" )

		seq_bandwidth_result.append( get_bandwidth( file_size2, timestamp1, timestamp2 ))
		
		# TEST blob to buffer

		timestamp1 = time.time()

		b.write_blob_buffer( "buffer_file2"+str(i), 0 )
		
		timestamp2 = time.time()

		seq_bandwidth_result_from_buffer.append( get_bandwidth( file_size2, timestamp1, timestamp2 ))

		# Second, TEST local disk to buffer
		timestamp1 = time.time()
		
		with open( "files/buffer_result"+str(i)+".data", "wb") as f:
			b.write_buffer_file( f, 0 )
		
		timestamp2 = time.time()

		seq_bandwidth_result_local.append( get_bandwidth( file_size2, timestamp1, timestamp2 ))
		


	#--- Parallel ---
	print("Start parallel tests...")
	# up
	for i in range( 0, number_tests ):
		timestamp1 = time.time()
	
		for j in range(4):
			t = threading.Thread(target=b.add_blob, args=["file1"+str(i)+str(j), "files/file1.data"])
			threads.append(t)
			t.start()

		for t in threads :
			t.join()

		timestamp2 = time.time()

		par_bandwidth_result.append( get_bandwidth( file_size1, timestamp1, timestamp2 ))


		# Test now local to buffer
		timestamp1 = time.time()

		with open( "files/file1.data", "r") as f:
	
			for j in range(4):
				t = threading.Thread(target=b.write_file_buffer, args=[f, j])
				threads.append(t)
				t.start()

			for t in threads :
				t.join()

		timestamp2 = time.time()

		par_bandwidth_result_local.append( get_bandwidth( file_size1, timestamp1, timestamp2 ))

		# Test now buffer to blob
		timestamp1 = time.time()
	
		for j in range(4):
			t = threading.Thread(target=b.write_buffer_blob, args=["buffer_file1"+str(i), j])
			threads.append(t)
			t.start()

		for t in threads :
			t.join()

		timestamp2 = time.time()

		par_bandwidth_result_from_buffer.append( get_bandwidth( file_size1, timestamp1, timestamp2 ))

	# down
	for i in range( 0, number_tests ):
		timestamp1 = time.time()
		
		for j in range(4):
			t = threading.Thread(target=b.get_blob, args=["file1"+str(i)+str(j), "files/result"+str(i)+str(j)+".data"])
			threads.append(t)
			t.start()

		for t in threads :
			t.join()

		timestamp2 = time.time()

		par_bandwidth_result.append( get_bandwidth( file_size1, timestamp1, timestamp2 ))

		# Test now blob to buffer
		timestamp1 = time.time()
	
		for j in range(4):
			t = threading.Thread(target=b.write_blob_buffer, args=["file1"+str(i)+str(j), j])
			threads.append(t)
			t.start()

		for t in threads :
			t.join()

		timestamp2 = time.time()

		par_bandwidth_result_from_buffer.append( get_bandwidth( file_size1, timestamp1, timestamp2 ))

		# Test now buffer to local
		timestamp1 = time.time()


		f = []
	
		for j in range(4):
			f.append( open( "files/buffer_return"+str(i)+str(j)+".data", "wb") )
			t = threading.Thread(target=b.write_buffer_file, args=[f[j], j])
			threads.append(t)
			t.start()

		for t in threads :
			t.join()
		
		for j in range(4):
			f[j].close()
		

		timestamp2 = time.time()

		par_bandwidth_result_local.append( get_bandwidth( file_size1, timestamp1, timestamp2 ))

	#--- Results ---
	draw( latency_result, "Latency results", "Tests", "Latency in milliseconds")
	draw( seq_bandwidth_result, "Sequential throughput", "Tests", "Throughput in bytes/s")
	draw( par_bandwidth_result, "Parallel throughput", "Tests", "Throughput in bytes/s")

	draw( seq_bandwidth_result_local, "Sequential throughput SSD <-> Buffer", "Tests", "Throughput in bytes/s")
	draw( par_bandwidth_result_local, "Parallel throughput SSD <-> Buffer", "Tests", "Throughput in bytes/s")

	draw( seq_bandwidth_result_from_buffer, "Sequential throughput GC Storage <-> Buffer", "Tests", "Throughput in bytes/s")
	draw( par_bandwidth_result_from_buffer, "Parallel throughput GC Storage <-> Buffer", "Tests", "Throughput in bytes/s")

	# delete all the blobs
	b.delete_all_blobs()

if __name__=="__main__":
	main()


