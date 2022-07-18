from abc import abstractmethod, ABC
from typing import Iterable
from xml.dom import NotFoundErr
from azure.storage.blob import  ContainerClient
import azure.cosmos.cosmos_client as cosmos_client
import pymongo
from pymongo.collection import Collection
from azure.storage.blob import BlobServiceClient
class BaseRepo(ABC):
    def __init__(self):
        self.repo_client = None
        self._time = 0
        self._status = 200
        self.setup()
    
    @abstractmethod
    def setup(self):
        raise NotImplementedError


class ContainerRepo(BaseRepo):
    
    def __init__(self, conn_string, container_name):
        self.conn_string = conn_string
        self.container_name = container_name
        super(ContainerRepo, self).__init__()

    
    def setup(self):
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.conn_string)
            self.repo_client = blob_service_client.get_container_client(self.container_name)
        
        except Exception as e:
            print(e)


class DataSourceRepo(ContainerRepo):
    def __init__(self, conn_string = None, container_name = None):
        super(DataSourceRepo, self).__init__(conn_string, container_name)
    
    @abstractmethod
    def read_blob(self, blob_name:str):
        blob_client = self.container_client.get_blob_client(blob_name)
        stream_downloader = blob_client.download_blob()
        return stream_downloader

    @abstractmethod
    def upload_blob(self, blob_name: str, data: Iterable, overwrite = True):
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        blob_client.upload_blob(data = data, overwrite=overwrite)


class CosmosRepo(BaseRepo):
    def __init__(self, host = None, master_key = None, database_id = None, container_id = None):
        self.host = host
        self.master_key = master_key
        self.database_id = database_id
        self.container_id = container_id
        super(CosmosRepo, self).__init__()

    def setup(self):
        client = cosmos_client.CosmosClient(self.host, {'masterKey': self.master_key}, user_agent="CosmosDBPythonQuickstart", user_agent_overwrite=True)
        db = client.get_database_client(self.database_id)
        self.container_client = db.get_container_client(self.container_id)


class DataCosmosRepo(CosmosRepo):
    def __init__(self, host = None, master_key = None, database_id = None, container_id = None):
        super(DataSourceRepo, self).__init__(host, master_key, database_id, container_id)
    
    def read_item(self, query, parameters):  
        items = list(self.container_client.query_items(
        query=query,
        parameters=parameters))
        return items

    def replace_item(self, doc_id, partition_key, dict_replace):
        read_item = self.container_client.read_item(item=doc_id, partition_key=partition_key)
        for key in dict_replace.keys():
            content_replace = dict_replace[key]
            read_item[key] = content_replace
        response = self.container_client.replace_item(item=read_item, body=read_item)
     
    def create_items(self, item_content):
        self.container_client.create_item(body=item_content)

      

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

