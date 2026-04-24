from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.services.tables_db import TablesDB
from appwrite.services.storage import Storage
from appwrite.services.users import Users
from app.config import APPWRITE_ENDPOINT, APPWRITE_PROJECT_ID, APPWRITE_API_KEY

client = Client()

client.set_endpoint(APPWRITE_ENDPOINT)
client.set_project(APPWRITE_PROJECT_ID)
client.set_key(APPWRITE_API_KEY)

databases = Databases(client)   # kept for attribute/index creation (setup script)
tables = TablesDB(client)       # new row-based API (replaces deprecated document API)
storage = Storage(client)
users = Users(client)
