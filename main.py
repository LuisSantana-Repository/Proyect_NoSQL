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
        4: "Post",
        5: "Like Post",
        6: "Comment on Post",
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
        22: "User Logins",
        23: "Logout"
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
        7: "Daily Logins",
        8: "Return"
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
        7: "Remove a lengualle",
        8: "Remoove a topic",
        9: "Cancel"
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
                while True:
                    print(f"Mongo id: {mongo_user_id}")
                    print(f"Dgraph id: {dgraph_user_id}")
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
                            cmodel.insert_activity(session,mongo_user_id,"Register")
                            cmodel.insert_logIn(session,mongo_user_id,ip)
                        elif option == 2:
                            # Log In
                            email = input("Enter email> ")
                            password = input("Enter password> ")
                            mongo_user_id = MongoFuncs.get_Log_In(db,email,password)
                            # print(mongo_user_id)
                            if(mongo_user_id):
                                dgraph_user_id = dmodel.get_user_uid_by_mongo(client,mongo_user_id)
                                # print(dgraph_user_id)
                            ip = ".".join(map(str, (random.randint(0, 255) for _ in range(4))))
                            cmodel.insert_logIn(session,mongo_user_id,ip)
                        elif option == 3:
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Modify Profile Information
                            print_Profile_Opctions()
                            inputs = int(input("select an option> "))
                            if(inputs == 1):
                                #bio Change
                                bio = input("New bio data > ")
                                MongoFuncs.set_update_profile_information(db,mongo_user_id,bio,None)
                            elif(inputs == 2):
                                #name change
                                name = input("chose new name > ")
                                MongoFuncs.set_update_profile_information(db,mongo_user_id,None,name)
                            elif(inputs == 3):
                                #private or public
                                privacity = input("private or public >")
                                if(privacity == "private"):
                                    MongoFuncs.Set_privacy(db,mongo_user_id,"private")
                                if(privacity == "public"):
                                    MongoFuncs.Set_privacy(db,mongo_user_id,"public")
                            elif(inputs == 4):
                                #lengualle
                                lenguaje = choose_languages()
                                MongoFuncs.set_add_preferences(db,mongo_user_id,lenguaje)
                            elif(inputs == 5):
                                topic = choose_category()
                                MongoFuncs.set_add_preferences(db,mongo_user_id,None,topic)
                            elif(inputs == 6):
                                #social links
                                plataform = input("Plataform Name >").strip()
                                Url = input("Url>").strip()
                                MongoFuncs.set_add_social_link(db,mongo_user_id,plataform,Url)
                            elif(inputs == 7):
                                lenguaje = choose_languages()
                                MongoFuncs.set_remove_Preferences(db,mongo_user_id,lenguaje,None)
                            elif(inputs == 8):
                                topic = choose_category()
                                MongoFuncs.set_remove_Preferences(db,mongo_user_id,None,topic)
                            else:
                                continue
                            cmodel.insert_activity(session, mongo_user_id, "Modified Profile Information")
                        elif option == 4:   
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Post
                            post = input("Write your post here >")
                            hashtags = get_hashtags()
                            categoty = choose_category()
                            lenguage = choose_languages()
                            cmodel.insert_Post(mongo_user_id,session,post,hashtags,categoty,lenguage,None)
                            pass
                        elif option == 5:   
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Like Post
                            post = input("Post to like >")
                            cmodel.add_Like(session, post, mongo_user_id)
                            cmodel.insert_activity(session, mongo_user_id, "Liked a post", post)
                        elif option == 6:   
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Comment Post
                            parent = input("Post to comment >")
                            post = input("Write your post here >")
                            hashtags = get_hashtags()
                            categoty = choose_category()
                            lenguage = choose_languages()
                            cmodel.comment_post(mongo_user_id,session,post,hashtags,categoty,lenguage,parent)
                            pass
                        elif option == 7:
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # View Your Feed 
                            friends = dmodel.get_my_friends(client, dgraph_user_id)
                            # print(friends)
                            for friend in friends:
                                posts = cmodel.get_post_by_user(session, friend['mongo'])
                                for post in posts:
                                    print("-" * 40) 
                                    cmodel.print_post(session, post, friend['username'])
                                    cmodel.insert_activity(session, mongo_user_id, "Viewed a post", post.post_id)
                                print("-" * 40)
                        elif option == 8:   
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Search for Users by Name or Tags
                            search_type = input("Name or Topic>")
                            if(search_type == "Name"):
                                Name = input("Tell Me its Name >").strip()
                                # print(Name)
                                results = MongoFuncs.get_users_by_name(db,Name)
                                for result in results:
                                    print("-" * 40)
                                    MongoFuncs.print_user(result)
                                print("-" * 40)
                            elif(search_type == "Topic"):
                                topic = choose_category()
                                results = MongoFuncs.get_users_by_topic(db,topic)
                                for result in results:
                                    print("-" * 40)
                                    MongoFuncs.print_user(result)
                                print("-" * 40)
                        elif option == 9:   #Too many arguments provided to bind()
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # View a User's Posts
                            #lists of users or decide myself // No, solo inserta el user id y muestra sus posts
                            user = input("Give me the user_id>")
                            cmodel.get_post_by_user(session,user)
                            #cmodel.get_post_by_user
                            pass
                        elif option == 10:  #Not programed 
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Discover Posts
                            #do cpsot by each ppreference anf do a join
                            pass
                        elif option == 11:  # Correctly generates the report, need furder testing jsut in case 
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Report Posts
                            #list post or input post id
                            Post_id = input("Post id>")
                            reason = input("Reason of report >")
                            MongoFuncs.add_report_post(db,mongo_user_id,Post_id,reason)
                            pass
                        elif option == 12:  #las agrega al array, digo que funciona, faltan pruebas 
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Save Posts
                            Post_id = input("Post ID>")
                            
                            # input post id
                            MongoFuncs.add_saved_post(db,mongo_user_id,Post_id)
                            #also a way to see my saved post is needed
                            
                            print(MongoFuncs.get_saved_posts(db,mongo_user_id))
                            pass
                        elif option == 13:   #falta crear notificaciones, upsi
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # View Notifications
                            print(MongoFuncs.get_noficitation(db,mongo_user_id))
                            pass
                        elif option == 14:   #funciona
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Send Follow Requests
                            friend_id = input("username you want to send request> ").strip()
                            friend_id = MongoFuncs.get_uid_by_username(db,friend_id)
                            
                            friend = dmodel.get_user_uid_by_mongo(client,friend_id)
                            dmodel.sent_friend_request(client,dgraph_user_id,friend)
                            pass
                        elif option == 15:  #funciona 
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Accept/Reject Follow Requests
                            dmodel.get_SendFollowRequest(client,dgraph_user_id)
                            
                            id = input("Give me the id of the user> ").strip()
                            
                            decition = input ("accept/deny >")
                            if(decition == "accept"):
                                dmodel.accept_friend_request(client,id,dgraph_user_id)
                            elif(decition == "deny"):
                                dmodel.reject_friend_request(client,id,dgraph_user_id)
                            pass
                        elif option == 16:   #FUNCIONA
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Unfollow
                            dmodel.get_my_friends(client,dgraph_user_id)
                            friend_id = input("Tell me the uid of friend >")
                            dmodel.unfollow_friend(client,dgraph_user_id,friend_id)
                            pass
                        elif option == 17:  #funciona 
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Block
                            bloked_username = input("Give me the username of the user>")
                            blocked_dgraph = dmodel.get_user_uid_by_mongo(client,MongoFuncs.get_uid_by_username(db,bloked_username))
                            dmodel.block(client,dgraph_user_id,blocked_dgraph)
                            
                            dmodel.get_blocked_Users(client,dgraph_user_id)
                            pass
                        elif option == 18:  #soy un cafro y funciona 
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Unblock
                            
                            dmodel.get_blocked_Users(client,dgraph_user_id)
                            unblock_id = input("Give me the uid of the user >")
                            dmodel.unblock(client,dgraph_user_id,unblock_id)
                            dmodel.get_blocked_Users(client,dgraph_user_id)
                            pass
                        elif option == 19:   #funciona
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            print("Mensajes que has recivido")
                            dmodel.get_RecivedMessages(client,dgraph_user_id)
                            
                            id = input("Escribe el username >")
                            id = dmodel.get_user_uid_by_mongo(client,MongoFuncs.get_uid_by_username(db,id))
                            message = input("Contenido > ")
                            # Private Messages
                            dmodel.createMessage(client,id,message,dgraph_user_id)
                            
                            print("Tus mensajes")
                            dmodel.get_myMessages(client,dgraph_user_id)
                            pass
                        elif option == 20:   #funciona, creo, teste con solo un amigo
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # View Friends
                            dmodel.get_my_friends(client,dgraph_user_id)
                            pass
                        elif option == 21:   #funciona, pero hace bucles
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            # Discover Potential Friends
                            recusive = int(input("select grade of recursive >  "))
                            dmodel.get_relationships(client,dgraph_user_id,recusive)
                            pass
                        elif option == 22: #funciona
                            if not mongo_user_id:
                                print("Please, register or log in before selecting this option")
                                continue
                            login_list = cmodel.get_LoginUser(session,mongo_user_id)
                            for index, log in enumerate(login_list, start=1):
                                print(f"{index}. {log}")

                            
                        elif option == 23:
                            # Log out
                            mongo_user_id = None
                            dgraph_user_id = None
                            break
                        else:
                            print("Invalid option. Please select a valid number.")
                    except Exception as e:
                        print(f"An error occurred: {e}")
            elif option == 4:
                while True:
                    print_third_menu()
                    option = int(input("Enter your choice: "))
                    try:
                        if option == 1:#error
                            # Posts with Most Likes
                            cmodel.get_top_10_liked_posts(session)
                        elif option == 2: #error
                            # Posts with Most Comments
                            cmodel.get_top_10_commented_posts(session)
                        elif option == 3: #regresa count, funciona creo
                            # Posts with Most Reports
                            print(MongoFuncs.get_reported_posts(db))
                        elif option == 4:#error
                            # Most Used Topics
                            print(cmodel.get_popularTopics(session))
                        elif option == 5: #error
                            # Most Used Hashtags
                            print(cmodel.get_popularHashtags(session))
                        elif option == 6: #funciona
                            # User Growth
                            print(MongoFuncs.get_user_growth(db))
                            pass
                        elif option == 7: #error
                            cmodel.get_dailyLogin(session)
                        elif option == 8:
                            # Return
                            break
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