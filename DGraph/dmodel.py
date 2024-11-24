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
    receiver: uid @count @reverse .
    content: string .
    timestamp: datetime .
    username: string @index(fulltext) .
    follows: [uid] @reverse .
    blocked: [uid] @reverse .
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
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
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
        print(f"Commit Response: {commit_response}")
        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()

def sent_friend_request(client, user, friend):
    txn = client.txn()
    ######TODAVIA NO SE SABE SI SE UTILIZARA UID OF MONGOUID TONSES DEJO LA FUNCION POR SI ACASO ACA ABAJO
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
        print(f"Commit Response: {commit_response}")
        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def reject_friend_request(client, user, friend):
    txn = client.txn()
    try:
        print(user,friend)
        response = txn.mutate(del_nquads= f'<{user}> <follow_request> <{friend}> .') #NOSABES LA CANTIDAD DE DOCUMENTCION QUE TUVE QUE HACER PORQUE NO FUNCIONABA
        commit_response = txn.commit()
        print(f"Commit Response: {commit_response}")
        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def accept_friend_request(client, user, friend):
    txn = client.txn()
    ######TODAVIA NO SE SABE SI SE UTILIZARA UID OF MONGOUID TONSES DEJO LA FUNCION POR SI ACASO ACA ABAJO
    try:
        response = txn.mutate(del_nquads= f'<{user}> <follow_request> <{friend}> .') #NOSABES LA CANTIDAD DE DOCUMENTCION QUE TUVE QUE HACER PORQUE NO FUNCIONABA
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
        print(f"Commit Response: {commit_response}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()

def unfollow_friend(client, user, friend):
    txn = client.txn()
    ######TODAVIA NO SE SABE SI SE UTILIZARA UID OF MONGOUID TONSES DEJO LA FUNCION POR SI ACASO ACA ABAJO
    try:
        response = txn.mutate(del_nquads= f'<{user}> <follows> <{friend}> .')
        commit_response = txn.mutate(del_nquads= f'<{friend}> <follows> <{user}> .')
        commit_response = txn.commit()
        print(f"Commit Response: {commit_response}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def block(client, user, bloqued):
    txn = client.txn()
    ######TODAVIA NO SE SABE SI SE UTILIZARA UID OF MONGOUID TONSES DEJO LA FUNCION POR SI ACASO ACA ABAJO
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
        print(f"Commit Response: {commit_response}")
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
    print(users)
    return users[0]['uid'] if users else None

def get_my_friends(client, user):
    query ="""query findFriends($uid: string) {
            user(func: uid($uid)) {
                friend_count: count(follows)
                follows{
                    uid
                    username
                }
            }
        }"""
    variables = {'$uid': user}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    print(f"Firends:\n{json.dumps(data, indent=2)}")
    return data

def get_blocked_Users(client, user):
    query ="""query BLockedUsers($uid: string) {
            Blocked(func: uid($uid)) {
                uid
                blocked {
                    uid
                    username
                }
            }
        }"""
    variables = {'$uid': user}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    print(f"Blocked:\n{json.dumps(data, indent=2)}")
    return data

def get_blockedBy(client, user):
    query ="""query blockedBy($uid: string) {
            blockedBy(func: uid($uid)) {
                uid
                ~blocked {
                    uid
                    username
                }
            }
        }"""
    variables = {'$uid': user}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    print(f"Blocked:\n{json.dumps(data, indent=2)}")
    return data


def get_myMessages(client, user):
    query ="""query mySendedMessages ($uid: string) {
            mySendedMessages (func: uid($uid)) {
                uid
                username
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
    print(f"My Messages:\n{json.dumps(data, indent=2)}")
    return data

def get_RecivedMessages(client, user):
    query ="""query myRecivedMessages ($uid: string) {
            myRecivedMessages (func: uid($uid)) {
                uid
                username
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
    print(f"My Recived Messages:\n{json.dumps(data, indent=2)}")
    return data


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
    print(f"My Sended Follow Request:\n{json.dumps(data, indent=2)}")
    return data

def get_RecieveFollowRequest(client, user):
    query ="""query PendingRequest ($uid: string) {
            PendingRequest (func: uid($uid)) {
                uid
                username
                follow_request{
                    uid
                    username
                }
            }
        }"""
    variables = {'$uid': user}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    print(f"My Pending Follow Request:\n{json.dumps(data, indent=2)}")
    return data

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





