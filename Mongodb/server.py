#!/usr/bin/env python3
import os

from fastapi import FastAPI
from pymongo import MongoClient

from routes import router as data_router
import DGraph.modeldgraph as dgraph_module
import Cassandra.model as dgraph_module

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
DB_NAME = os.getenv('MONGODB_DB_NAME', 'iteso')

app = FastAPI()

@app.on_event("startup")
def startup_db_client():
    app.mongodb_client = MongoClient(MONGODB_URI)
    app.database = app.mongodb_client[DB_NAME]
    print(f"Connected to MongoDB at: {MONGODB_URI} \n\t Database: {DB_NAME}")

@app.on_event("shutdown")
def shutdown_db_client():
    app.mongodb_client.close()
    print("Bye bye...!!")

app.include_router(data_router, tags=["Data"], prefix="/Data")
