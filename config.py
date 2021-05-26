from pymongo import MongoClient

NamePath_files = 'Odata{year}File.csv'

class Connect(object):
    @staticmethod    
    def get_connection():
        return MongoClient("mongodb://admin:admin@localhost:27017/?authSource=admin") 