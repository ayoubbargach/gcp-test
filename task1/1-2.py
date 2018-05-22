#! /usr/bin/python3
# -*- coding: utf-8 -*-
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import Forbidden


#---- INIT ----

# Init global variables
bucket_name = 'noyau-gcptest'

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
	
	def get_blob(self, blob_name, file_name):
		blob = self.bucket.blob(blob_name)
		blob.download_to_filename(file_name)

		print('File {} downloaded as {}'.format(file_name, blob_name))

	def get_all_blobs(self):
		"""Lists all the blobs in the bucket."""
		return self.bucket.list_blobs()

	def delete_all_blobs(self):
		""" Delete all blobs of the experimentation """
		blobs = self.get_all_blobs()
		for blob in blobs :
			blob.delete()

	def get_bkt(self):
		return self.bucket

	def get_name(self):
		return self.name


#---- FUNCTIONS ----

def get_latency( timestamp1, timestamp2 ):
	return timestamp2 - timestamp1

def get_bandwidth( file_size, timestamp1, timestamp2 ):
	return round( file_size / get_latency( timestamp1, timestamp2))
	


#---- MAIN ----

def main():
	""" Main function """

	# We start by initiating the connection
	b = Bkt(bucket_name)

	b.add_blob( "cat", "files/cat.jpeg")

	# Download the file
	b.get_blob( "cat", "files/return.jpeg")
	
	# delete all the blobs
	# b.delete_all_blobs()

if __name__=="__main__":
	main()


