#! /usr/bin/python3
# -*- coding: utf-8 -*-
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import Forbidden
from matplotlib import pyplot as plt
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

	def __init__(self, bucket_name):

		""" Create a new bucket if does not exist"""
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

	def add_blob(self, blob_name, file_name):
		blob = self.bucket.blob(blob_name)
		blob.upload_from_filename(file_name)

		print('File {} uploaded as {}'.format(file_name, blob_name))

	def add_blob_from_string(self, blob_name, str_value):
		blob = self.bucket.blob(blob_name)
		blob.upload_from_string(str_value)

		print('String {} uploaded as {}'.format(str_value, blob_name))
	
	def get_blob(self, blob_name, file_name):
		blob = self.bucket.blob(blob_name)
		blob.download_to_filename(file_name)

		print('File {} downloaded from {}'.format(file_name, blob_name))

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
	plt.show()
	plt.close()

#---- MAIN ----

def main():
	""" Main function """

	# We start by initiating the connection and some variables
	b = Bkt(bucket_name)

	threads = []
	latency_result = []
	seq_bandwidth_result = []
	par_bandwidth_result = []

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

		# bandwidth		
		timestamp1 = time.time()
		b.add_blob( "file2"+str(i), "files/file2.data")
		timestamp2 = time.time()

		seq_bandwidth_result.append( get_bandwidth( file_size2, timestamp1, timestamp2 ))

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

	#--- Results ---
	draw( latency_result, "Latency results", "Tests", "Latency in milliseconds")
	draw( seq_bandwidth_result, "Sequential throughput", "Tests", "Throughput in bytes/s")
	draw( par_bandwidth_result, "Parallel throughput", "Tests", "Throughput in bytes/s")

	# delete all the blobs
	b.delete_all_blobs()

if __name__=="__main__":
	main()


