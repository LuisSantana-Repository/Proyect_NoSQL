#!/usr/bin/env python3
import os
import pydgraph
from cassandra.cluster import Cluster
from pymongo import MongoClient
import random

from Cassandra import cmodel
from DGraph import dmodel
from Mongodb import MongoFuncs

# Cassandra Connection
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'socialmedia')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

# Mongo Connection
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
DB_NAME = os.getenv('MONGODB_DB_NAME', 'socialmedia')

# Dgraph Connection
DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

# Functions
def choose_category():
    categories = ["Action", "Fantasy", "Theory", "Adventure", "Comedy", "Drama", "Mystery", "Horror"]
    print("Available categories:")
    for i, category in enumerate(categories, start=1):
        print(f"{i}. {category}")
    while True:
        choice = int(input("Enter the number corresponding to your choice: "))
        if 1 <= choice <= len(categories):
            return categories[choice - 1]
        else:
            print(f"Please choose a number between 1 and {len(categories)}.")

def get_hashtags():
    hashtags_input = input("Enter hashtags separated by commas: ").strip()
    hashtags = [tag.strip() for tag in hashtags_input.split(",") if tag.strip()]
    return hashtags

def choose_languages():

    languages = ["English", "Spanish"]
    
    print("Available languages:")
    for i, language in enumerate(languages, start=1):
        print(f"{i}. {language}")

    print("\nEnter the number of lengualle:")
    
    while True:
        choices = int(input("Your selection: "))
        if 1 <= choices <= len(languages):
            return languages[choices - 1]
        else:
            print(f"Please choose numbers between 1 and {len(languages)}.")

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
        6: "Like Post",
        7: "Comment on Post",
        8: "View Your Feed",
        9: "Search for Users by Name or Tags",
        10:"View a User's Posts",
        11: "Discover Posts",
        12: "Report Posts",
        13: "Save Posts",
        14: "View Notifications",
        15: "Send Follow Requests",
        16: "Accept/Reject Follow Requests",
        17: "Unfollow",
        18: "Block",
        19: "Unblock",
        20: "Private Messages",
        21: "View Friends",
        22: "Discover Potential Friends",
        23: "Cancel"
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



def print_Profile_Opctions():
    options = {
        1: "Change bio",
        2: "Change name",
        3: "Privacity settings",
        4: "Add lenguaje preferences",
        5: "Add topic preferences",
        6: "Add social Links",
        7: "Cancel"
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
    db = mongodb_client[DB_NAME]
    
    # Initialize Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)
    
    mongo_user_id = None
    dgraph_user_id = None
    while True:
        print(f"Mongo id: {mongo_user_id}")
        print(f"Dgraph id: {dgraph_user_id}")
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
                        # Register
                        username = input("Enter Username> ")
                        email = input("Enter email> ")
                        password = input("Enter password> ")
                        name = input("Enter Oficial Name> ")
                        bio = input("Enter Bio (opctional)> ")
                        mongo_user_id = MongoFuncs.add_user_registtration(db,username,email,password,bio,name)
                        dgraph_user_id = dmodel.createUser(client,username,mongo_user_id)
                        ip = ".".join(map(str, (random.randint(0, 255) for _ in range(4))))
                        cmodel.insert_logIn(session,mongo_user_id,ip)
                    elif option == 2:
                        # Log In
                        email = input("Enter email> ")
                        password = input("Enter password> ")
                        mongo_user_id = MongoFuncs.get_Log_In(db,email,password)
                        print(mongo_user_id)
                        if(mongo_user_id):
                            dgraph_user_id = dmodel.get_user_uid_by_mongo(client,mongo_user_id)
                            print(dgraph_user_id)
                        ip = ".".join(map(str, (random.randint(0, 255) for _ in range(4))))
                        cmodel.insert_logIn(session,mongo_user_id,ip)
                    elif option == 3 and mongo_user_id:
                        # Modify Profile Information
                        print_Profile_Opctions()
                        inputs = int(input("select an option> "))
                        if(inputs == 1):
                            #bio Change
                            bio = input("New bio data > ")
                            MongoFuncs.set_update_profile_information(db,mongo_user_id,bio,None)
                            pass
                        elif(inputs == 2):
                            #name change
                            name = input("chose new name > ")
                            MongoFuncs.set_update_profile_information(db,mongo_user_id,None,name)
                            pass
                        elif(inputs == 3):
                            #private or public
                            privacity = input("private or public >")
                            if(privacity == "private"):
                                MongoFuncs.Set_privacy(db,mongo_user_id,"private")
                            if(privacity == "public"):
                                MongoFuncs.Set_privacy(db,mongo_user_id,"public")
                            pass
                        elif(inputs == 4):
                            #lengualle
                            lenguaje = choose_languages()
                            MongoFuncs.set_add_preferences(db,mongo_user_id,lenguaje)
                            pass
                        elif(inputs == 5):
                            tags = get_hashtags()
                            MongoFuncs.set_add_preferences(db,mongo_user_id,None,tags)
                            pass
                        elif(inputs == 6):
                            #social links
                            plataform = input("Plataform Name >").strip()
                            Url = input("Url>").strip()
                            MongoFuncs.set_add_social_link(db,mongo_user_id,plataform,Url)
                            pass
                        pass
                    elif option == 4 and mongo_user_id:
                        # Add/Update Preferences
                        preferences = choose_category()
                        lenguage = choose_languages()
                        tags = get_hashtags()
                        # privacity setting
                        pass
                    elif option == 5 and mongo_user_id:
                        # Post
                        post = input("Write your post here >")
                        hashtags = get_hashtags()
                        categoty = choose_category()
                        lenguage = choose_languages()
                        cmodel.insert_Post(mongo_user_id,session,post,hashtags,categoty,lenguage,None)
                        pass
                    elif option == 6 and mongo_user_id:
                        # Like Post
                        post = input("Post to like >")
                        cmodel.add_Like(session, post, mongo_user_id)
                    elif option == 7 and mongo_user_id:
                        # Comment Post
                        parent = input("Post to comment >")
                        post = input("Write your post here >")
                        hashtags = get_hashtags()
                        categoty = choose_category()
                        lenguage = choose_languages()
                        cmodel.comment_post(mongo_user_id,session,post,hashtags,categoty,lenguage,parent)
                        pass
                    elif option == 8 and mongo_user_id:
                        # View Your Feed 
                        lenguage = MongoFuncs.get_language_preferences(db,mongo_user_id)
                        tags = MongoFuncs.get_tag_preferences(db,mongo_user_id)
                        for tag in tags:
                            #print(cmodel.)
                            pass
                        pass
                    elif option == 9 and mongo_user_id:
                        # Search for Users by Name or Tags
                        search_type = input("Name or tag>")
                        if(search_type == "Name"):
                            Name = input("Tell Me its Name >")
                            result = MongoFuncs.get_users_by_name(db,Name)
                            print(result)
                        elif(search_type == "Tag"):
                            Tag = input("Write a prefered Tag >")
                            MongoFuncs.get_users_by_tag(db,Tag)
                        #have the serch user by name or tag but missing the privasity notification or the block feature
                        pass
                    elif option == 10 and mongo_user_id:
                        # View a User's Posts
                        #lists of users or decide myself // No, solo inserta el user id y muestra sus posts
                        user = input("Give me the user_id>")
                        cmodel.get_post_by_user(session,user)
                        #cmodel.get_post_by_user
                        pass
                    elif option == 11 and mongo_user_id:
                        # Discover Posts
                        #do cpsot by each ppreference anf do a join
                        pass
                    elif option == 12 and mongo_user_id: 
                        # Report Posts
                        #list post or input post id
                        Post_id = input("Post id>")
                        reason = input("Reason of report >")
                        MongoFuncs.add_report_post(db,mongo_user_id,Post_id,reason)
                        pass
                    elif option == 13 and mongo_user_id:
                        # Save Posts
                        Post_id = input("Post ID>")
                        
                        # input post id
                        MongoFuncs.add_saved_post(db,mongo_user_id,Post_id)
                        #also a way to see my saved post is needed
                        
                        print(MongoFuncs.get_saved_posts(db,mongo_user_id))
                        pass
                    elif option == 14 and mongo_user_id:
                        # View Notifications
                        print(MongoFuncs.get_noficitation(db,mongo_user_id))
                        pass
                    elif option == 15 and mongo_user_id:
                        # Send Follow Requests
                        friend_id = input("user you want to send request> ")
                        friend = dmodel.get_user_uid_by_mongo(friend_id)
                        dmodel.sent_friend_request(client,dgraph_user_id,friend)
                        pass
                    elif option == 16 and mongo_user_id:
                        # Accept/Reject Follow Requests
                        dmodel.get_SendFollowRequest(client,mongo_user_id)
                        
                        id = input("Give me the id of the user> ")
                        Frined_dgraf_id = dmodel.get_user_uid_by_mongo(id)
                        
                        decition = input ("accept/deny >")
                        if(decition == "accept"):
                            dmodel.accept_friend_request(client,dgraph_user_id,Frined_dgraf_id)
                        elif(decition == "deny"):
                            dmodel.reject_friend_request(client,dgraph_user_id,Frined_dgraf_id)
                        pass
                    elif option == 17 and mongo_user_id:
                        # Unfollow
                        dmodel.get_my_friends(client,mongo_user_id)
                        friend_id = input("Tell me the uid of friend >")
                        dmodel.unfollow_friend(client,dgraph_user_id,friend_id)
                        pass
                    elif option == 18 and mongo_user_id:
                        # Block
                        bloked_id = input("Give me the mongo_id of the user>")
                        blocked_dgraph = dmodel.get_user_uid_by_mongo(bloked_id)
                        dmodel.block(client,dgraph_user_id,blocked_dgraph)
                        
                        dmodel.get_blocked_Users(client,dgraph_user_id)
                        pass
                    elif option == 19 and mongo_user_id:
                        # Unblock
                        
                        dmodel.get_blocked_Users(client,dgraph_user_id)
                        unblock_id = input("Give me the uid of the user >")
                        dmodel.unblock(client,dgraph_user_id,unblock_id)
                        dmodel.get_blocked_Users(client,dgraph_user_id)
                        pass
                    elif option == 20 and mongo_user_id:
                        # Private Messages
                        dmodel.createMessage
                        dmodel.get_my_friends
                        pass
                    elif option == 21 and mongo_user_id:
                        # View Friends
                        dmodel.get_my_friends(client,dgraph_user_id)
                        pass
                    elif option == 22 and mongo_user_id:
                        # Discover Potential Friends
                        # Missing query (cual missing? es el recursivo)
                        pass
                    elif option == 23:
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
                        # Posts with Most Likes
                        cmodel.get_top_10_liked_posts(session)
                    elif option == 2:
                        # Posts with Most Comments
                        cmodel.get_top_10_commented_posts(session)
                    elif option == 3:
                        # Posts with Most Reports
                        print(MongoFuncs.get_reported_posts(db))
                    elif option == 4:
                        # Most Used Topics
                        print(cmodel.get_popularTopics(session))
                    elif option == 5:
                        # Most Used Hashtags
                        print(cmodel.get_popularHashtags(session))
                    elif option == 6:
                        # User Growth
                        print(MongoFuncs.get_user_growth(db))
                        pass
                    elif option == 7:
                        email = input("Enter email> ")
                        password = input("Enter password> ")
                        mongo_user_id = MongoFuncs.get_Log_In()
                        if(mongo_user_id):
                            #FAlta esto
                            cmodel.get_LoginUser(session,mongo_user_id)
                        # User Logins
                        
                        
                        # falta opcion para user logins pero daily
                    elif option == 8:
                        # Cancel
                        pass
                    else:
                        print("Invalid option. Please select a valid number.")
                except Exception as e:
                    print(f"An error occurred: {e}")
            elif option == 5:
                # Cassandra
                cmodel.delete_all(session)
                # Mongo
                MongoFuncs.delete_all(db)
                # Dgraph
                dmodel.delete_all(client)
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