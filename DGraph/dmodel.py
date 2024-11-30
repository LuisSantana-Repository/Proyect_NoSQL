#!/usr/bin/env python3
from datetime import datetime
import json

import pydgraph


def set_schema(client):
    schema = """
    type User {
        username
        follows
        mongo
        blocked
        sent_message
        follow_request
    }
    type Message {
    content
    timestamp
    receiver
    }
    receiver: uid @reverse .
    content: string .
    timestamp: datetime .
    username: string .
    follows: [uid] .
    blocked: [uid] .
    sent_message: [uid] @reverse .
    follow_request: [uid] @reverse .
    mongo: string @index(hash) .
    """
    return client.alter(pydgraph.Operation(schema=schema))


def createUser(client,username, mongo):
    txn = client.txn()
    try:
        p = {
            'set': [
                # User data
                {
                    'uid': '_:user1',
                    'dgraph.type': 'User',
                    'username': username,
                    'mongo': mongo,
                }
            ]
        }
        response = txn.mutate(set_obj=p)
        commit_response = txn.commit()
        #print(f"Commit Response: {response}")
        assigned_uid = response.uids.get("user1")
        #print(f"UIDs: {response.uids}")
        return assigned_uid
    finally:
        txn.discard()


def createMessage(client,receiver, mssage, sender):
    txn = client.txn()
    try:
        p = {
            'set': [
                # User data
                {
                    'uid': sender,
                    'sent_message':[{
                        'uid': '_:new_message',
                        'dgraph.type': 'Message',
                        'content': mssage,
                        'timestamp': datetime.now().isoformat(),
                        'receiver': {'uid': receiver}
                        }]
                }
            ]
        }
        response = txn.mutate(set_obj=p)
        commit_response = txn.commit()
        #print(f"Commit Response: {commit_response}")
        #print(f"UIDs: {response.uids}")
    finally:
        txn.discard()

def sent_friend_request(client, user, friend):
    txn = client.txn()
    try:
        p = {
            'set': [
                # User data
                {
                    'uid': user,
                    'follow_request':[{
                            'uid': friend
                        }]
                }
            ]
        }
        response = txn.mutate(set_obj=p)
        commit_response = txn.commit()
        #print(f"Commit Response: {commit_response}")
        #print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def reject_friend_request(client, user, friend):
    txn = client.txn()
    try:
        #print(user,friend)
        response = txn.mutate(del_nquads= f'<{user}> <follow_request> <{friend}> .') 
        commit_response = txn.commit()
        #print(f"Commit Response: {commit_response}")
        #print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def accept_friend_request(client, user, friend):
    txn = client.txn()
    try:
        response = txn.mutate(del_nquads= f'<{user}> <follow_request> <{friend}> .')
        set_mutation = {
            'set': [
                {
                    'uid': user,
                    'follows': [{'uid': friend}]
                },
                {
                    'uid': friend,
                    'follows': [{'uid': user}]
                }
            ]
        }
        txn.mutate(set_obj=set_mutation)
        commit_response = txn.commit()
        #print(f"Commit Response: {commit_response}")
    finally:
        # Clean up. 
        txn.discard()

def unfollow_friend(client, user, friend):
    txn = client.txn()
    try:
        response = txn.mutate(del_nquads= f'<{user}> <follows> <{friend}> .')
        commit_response = txn.mutate(del_nquads= f'<{friend}> <follows> <{user}> .')
        commit_response = txn.commit()
        #print(f"Commit Response: {commit_response}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def block(client, user, bloqued):
    txn = client.txn()
    
    try:
        delete_mutation = {
            'set': [
                {
                    'uid': user,
                    'blocked': [{'uid': bloqued}]
                }
            ]
        }
        txn.mutate(set_obj=delete_mutation)
        commit_response = txn.commit()
        #print(f"Commit Response: {commit_response}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def get_user_uid_by_mongo(client, mongo_id):
    query ="""query findUser($mongo: string) {
            user(func: eq(mongo, $mongo)) {
                uid
            }
        }"""
    variables = {'$mongo': mongo_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    users = data.get('user', [])
    # print(users)
    return users[0]['uid'] if users else None

def get_my_friends(client, user):
    query ="""query findFriends($uid: string) {
            user(func: uid($uid)) {
                follows{
                    uid
                    username
                    mongo
                }
            }
        }"""
    variables = {'$uid': user}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    # print(f"Firends:\n{json.dumps(data, indent=2)}")
    
    if( data['user'] ):
        #print(data)
        return data['user'][0]['follows'] # Returns an array of {dgraph.uid, username}
    else:
        #print(None)
        return None
    
def print_my_friends(friends):
    if friends:
        print("-" * 40)
        for friend in friends:
            uid = friend.get('uid')
            username = friend.get('username')
            print(f"UID: {uid}")
            print(f"Username: {username}")
            print("-" * 40) 
    else:
        print("No friends found.")
        print("-" * 40)


def get_blocked_Users(client, user):
    query ="""query BLockedUsers($uid: string) {
            Blocked(func: uid($uid)) {
                blocked {
                    uid
                    username
                }
            }
        }"""
    variables = {'$uid': user}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    #print(f"Blocked:\n{json.dumps(data, indent=2)}")
    return data


def print_blocked_users(data):
    print("Blocked Users:")
    if data['Blocked']:
        print("-" * 40)
        for blocked_user in data['Blocked'][0]['blocked']:
            uid = blocked_user.get('uid')
            username = blocked_user.get('username')
            print(f"UID: {uid}")
            print(f"Username: {username}")
            print("-" * 40)
    else:
        print("No blocked users found.")
        print("-" * 40)



def get_myMessages(client, user):
    query ="""query mySendedMessages ($uid: string) {
            mySendedMessages (func: uid($uid)) {
                sent_message {
                    uid
                    content
                    timestamp
                    receiver {
                        uid
                        username
                    }
                }
            }
        }"""
    variables = {'$uid': user}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    #print(f"My Messages:\n{json.dumps(data, indent=2)}")
    return data

def print_my_messages(data):
    print("-" * 40)
    print("My Sent Messages:")
    #print(data)
    if data['mySendedMessages']:
        for message_entry in data['mySendedMessages'][0]['sent_message']:
                #print(message_entry)
                content = message_entry.get('content')
                timestamp = message_entry.get('timestamp')
                receiver = message_entry.get('receiver')
                receiver_uid = receiver.get('uid')
                receiver_username = receiver.get('username')

                print(f"Reciver UID: {receiver_uid}")
                print(f"To: {receiver_username}")
                print(f"\tContent: {content}")
                print(f"\tTimestamp: {timestamp}")
                print("-" * 40) 
    else:
        print("No sent messages found.")
        print("-" * 40)

def get_RecivedMessages(client, user):
    query ="""query myRecivedMessages ($uid: string) {
            myRecivedMessages (func: uid($uid)) {
                ~receiver {
                    uid
                    content
                    timestamp
                    sender: ~sent_message {
                        uid
                        username
                    }
                }
            }
        }"""
    variables = {'$uid': user}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    #print(f"My Recived Messages:\n{json.dumps(data, indent=2)}")
    return data

def print_received_messages(data):
    print("-" * 40)
    print("Received Messages:")
    if data['myRecivedMessages']:
        print("-" * 40)
        for message in data['myRecivedMessages'][0]['~receiver']:
            content = message.get('content')
            timestamp = message.get('timestamp')
            # Sender information
            sender = message.get('sender')[0]
            sender_uid = sender.get('uid')
            sender_username = sender.get('username')
            
            # Print message details
            print(f"Sender UID: {sender_uid}")
            print(f"Sender Username: {sender_username}")
            print(f"\tContent: {content}")
            print(f"\tTimestamp: {timestamp}")

            print("-" * 40)  # Separator for each message
    else:
        print("No received messages found.")
        print("-" * 40)


def get_SendFollowRequest(client, user):
    query ="""query MyFollowRequest ($uid: string) {
            MyFollowRequest (func: uid($uid)) {
                ~follow_request{
                    uid
                    username
                }
            }
        }"""
    variables = {'$uid': user}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    #print(f"My Sended Follow Request:\n{json.dumps(data, indent=2)}")
    return data

def print_follow_request(data):
    if(data.get("MyFollowRequest")):
        print("-" * 40)
        for follow_request in data['MyFollowRequest'][0]['~follow_request']:
            uid = follow_request.get('uid')
            username = follow_request.get('username')
            print(f"UID: {uid}")
            print(f"Username: {username}")
            print("-" * 40) 
    else:
        print("-" * 40)
        print("No follow requests found.")
        print("-" * 40)


def delete_all(client):
    client.alter(pydgraph.Operation(drop_all=True))
    
    

def unblock(client, user, blocked_user):
    txn = client.txn()
    try:
        txn.mutate(del_nquads= f'<{user}> <blocked> <{blocked_user}> .')
        txn.commit()
        print(f"Unblocked User: {blocked_user} from User: {user}")
    finally:
        txn.discard()

def get_relationships(client, user, depth=3):
    depth = str(depth)
    query = """
    query FriendsOfFriends($uid: string, $depth: int) {
        FriendsOfFriends(func: uid($uid)) @recurse(depth: $depth) {
            uid
            username
            follows
        }
    }
    """
    variables = {
        "$uid": user,
        "$depth": depth
    }
    txn = client.txn(read_only=True)
    response = txn.query(query, variables=variables)
    data = json.loads(response.json)
    print(f"Firends of friends:\n{json.dumps(data, indent=2)}")
    return data





def get_mongo_by_uid(client, dgraph_uid):
    query = """
    query getMongoString($uid: string) {
        user(func: uid($uid)) {
            mongo
        }
    }
    """
    variables = {"$uid": dgraph_uid}

    txn = client.txn(read_only=True)
    try:
        response = txn.query(query, variables=variables)
        data = json.loads(response.json)
        users = data.get("user", [])
        if users:
            return users[0].get("mongo")
        return None
    finally:
        txn.discard()
