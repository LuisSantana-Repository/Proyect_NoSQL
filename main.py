#!/usr/bin/env python3
import os
import pydgraph
from cassandra.cluster import Cluster
from pymongo import MongoClient

from Cassandra import cmodel
from DGraph import dmodel

# Cassandra Connection
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'socialmedia')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

# Mongo Connection
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
DB_NAME = os.getenv('MONGODB_DB_NAME', 'iteso')

# Dgraph Connection
DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def print_menu():
    options = {
        1: "Set models",
        2: "Load data",
        3: "User actions",
        4: "Analysis actions",
        5: "Delete all data",
        6: "Exit"
    }
    for key, value in options.items():
        print(f"{key} -- {value}")

def print_second_menu():
    options = {
        1: "Register",
        2: "Log In",
        3: "Modify Profile Information",
        4: "Add/Update Preferences",
        5: "Post",
        6: "Comment on Posts",
        7: "View Your Feed",
        8: "Search for Users by Name or Tags",
        9: "View a User's Posts",
        10: "Discover Posts",
        11: "Report Posts",
        12: "Save Posts",
        13: "View Notifications",
        14: "Send Follow Requests",
        15: "Accept/Reject Follow Requests",
        16: "Unfollow",
        17: "Block",
        18: "Unblock",
        19: "Private Messages",
        20: "View Friends",
        21: "Discover Potential Friends",
        22: "Cancel"
    }
    for key, value in options.items():
        print(f"{key} -- {value}")

def print_third_menu():
    options = {
        1: "Posts with Most Likes",
        2: "Posts with Most Comments",
        3: "Posts with Most Reports",
        4: "Most Used Topics",
        5: "Most Used Hashtags",
        6: "User Growth",
        7: "User Logins",
        8: "Cancel"
    }
    for key, value in options.items():
        print(f"{key} -- {value}")


def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def close_client_stub(client_stub):
    client_stub.close()

def main():
    # Initialize Cassandra
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()
    cmodel.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    # Initialize Mongo
    mongodb_client = MongoClient(MONGODB_URI)
    database = mongodb_client[DB_NAME]
    collection = database['socialmedia']
    
    # Initialize Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    while True:
        print_menu()
        option = int(input("Enter your choice: "))
        try:
            if option == 1:
                # Cassandra model
                cmodel.create_schema(session)
                # Mongo doesnt need to set the model

                # Dgraph model
                dmodel.set_schema(client)
            elif option == 2:
                # Load random data (not necessary)
                pass
            elif option == 3:
                print_second_menu()
                option = int(input("Enter your choice: "))
                try:
                    if option == 1:
                        # Code to handle "Register"
                        pass
                    elif option == 2:
                        # Code to handle "Log In"
                        pass
                    elif option == 3:
                        # Code to handle "Modify Profile Information"
                        pass
                    elif option == 4:
                        # Code to handle "Add/Update Preferences"
                        pass
                    elif option == 5:
                        # Code to handle "Post"
                        pass
                    elif option == 6:
                        # Code to handle "Comment on Posts"
                        pass
                    elif option == 7:
                        # Code to handle "View Your Feed"
                        pass
                    elif option == 8:
                        # Code to handle "Search for Users by Name or Tags"
                        pass
                    elif option == 9:
                        # Code to handle "View a User's Posts"
                        pass
                    elif option == 10:
                        # Code to handle "Discover Posts"
                        pass
                    elif option == 11:
                        # Code to handle "Report Posts"
                        pass
                    elif option == 12:
                        # Code to handle "Save Posts"
                        pass
                    elif option == 13:
                        # Code to handle "View Notifications"
                        pass
                    elif option == 14:
                        # Code to handle "Send Follow Requests"
                        pass
                    elif option == 15:
                        # Code to handle "Accept/Reject Follow Requests"
                        pass
                    elif option == 16:
                        # Code to handle "Unfollow"
                        pass
                    elif option == 17:
                        # Code to handle "Block"
                        pass
                    elif option == 18:
                        # Code to handle "Unblock"
                        pass
                    elif option == 19:
                        # Code to handle "Private Messages"
                        pass
                    elif option == 20:
                        # Code to handle "View Friends"
                        pass
                    elif option == 21:
                        # Code to handle "Discover Potential Friends"
                        pass
                    elif option == 22:
                        # Cancel
                        pass
                    else:
                        print("Invalid option. Please select a valid number.")
                except Exception as e:
                    print(f"An error occurred: {e}")

            elif option == 4:
                try:
                    print_third_menu()
                    option = int(input("Enter your choice: "))
                    if option == 1:
                        # Code to handle "Posts with Most Likes"
                        pass
                    elif option == 2:
                        # Code to handle "Posts with Most Comments"
                        pass
                    elif option == 3:
                        # Code to handle "Posts with Most Reports"
                        pass
                    elif option == 4:
                        # Code to handle "Most Used Topics"
                        pass
                    elif option == 5:
                        # Code to handle "Most Used Hashtags"
                        pass
                    elif option == 6:
                        # Code to handle "User Growth"
                        pass
                    elif option == 7:
                        # Code to handle "User Logins"
                        pass
                    elif option == 8:
                        # Cancel
                        pass
                    else:
                        print("Invalid option. Please select a valid number.")
                except Exception as e:
                    print(f"An error occurred: {e}")
            elif option == 5:
                # Delete all data
                pass
            elif option == 6:
                # Cassandra doesnt need to be closed?
                mongodb_client.close() # Close Mongo
                close_client_stub(client_stub) # Close Dgraph
                break
            else:
                print("Invalid option. Please select a valid number.")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")