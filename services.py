from abc import abstractmethod, ABC
from typing import Iterable
from xml.dom import NotFoundErr
from azure.storage.blob import  ContainerClient
import azure.cosmos.cosmos_client as cosmos_client
import pymongo
from pymongo.collection import Collection
from azure.storage.blob import BlobServiceClient


class DatalakeContainer:
    def __init__(self, conn_string, container_name):
        self.conn_string =conn_string
        self.container_name = container_name
        self.repo_client = None
        super(DatalakeContainer, self).__init__()
    
    def setup(self):
        blob_service_client = BlobServiceClient.from_connection_string(self.conn_string)
        self.repo_client = blob_service_client.get_container_client(self.container_name)

    @abstractmethod
    def read_blob(self, blob_name:str):
        blob_client = self.repo_client.get_blob_client(blob_name)
        stream_downloader = blob_client.download_blob()
        return stream_downloader

    @abstractmethod
    def upload_blob(self, blob_name: str, data: Iterable, overwrite = True):
        blob_client = self.repo_client.get_blob_client(blob=blob_name)
        blob_client.upload_blob(data = data, overwrite=overwrite)

      
class MongoCollection:
    
    def __init__(self, connection_string: str, db_name: str):
        self.connection_string = connection_string
        self.db_name = db_name
        self.client = None
        self.database = None
        self.collection = None
        self.setup()

    def setup(self):
        self.client = pymongo.MongoClient(self.connection_string)
        try:
            self.client.server_info() # validate connection string
        except pymongo.errors.ServerSelectionTimeoutError:
            raise TimeoutError("Invalid API for MongoDB connection string or timed out when attempting to connect")

        self.database = self.client[self.db_name]
        print(self.database)
    
    def get_collection(self, collection_name: str) -> Collection:
        if collection_name in self.database.list_collection_names():
            return self.database[collection_name]
        raise NotFoundErr("collection {} not exited in database".format(collection_name))

