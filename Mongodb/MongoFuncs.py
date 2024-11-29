#!/usr/bin/env python3
from Mongodb.mmodel import User, Report, Notification

def set_indexes(db):
    #db.users.create_index([('email', 1), ('password', 1)]) no need since email is unique
    db.users.create_index([('username', 1)])
    db.users.create_index([('topics_preferences', 1)]) # unwind faster
    db.users.create_index([('registration_timestamp', 1)]) # aggregation faster
    db.notifications.create_index([("user_id",1)])
    db.users.create_index([('email', 1)], unique=True)
    db.reports.create_index([("reported_content_id",1)])
    
    indexes = db.users.list_indexes()
    # Iterate through the indexes and print them
    for index in indexes:
        print(index)




def add_user_registtration(db, username, email, password, bio=None, name=None):
    user = User(
        username=username,
        email=email,
        password=password,
        bio=bio,
        name=name
    )
    result = db.users.insert_one(user.model_dump(by_alias=True))
    return result.inserted_id

def set_add_preferences(db, user_id, language=None, topic=None):
    update = {}
    if language:
        update["language_preferences"] = {"$each": [language]}
    if topic:
        update["topics_preferences"] = {"$each": [topic]}
    
    result = db.users.update_one({"_id": user_id}, {"$addToSet": update})
    return result.modified_count


def set_remove_Preferences(db, user_id, language=None, topic=None):
    update = {}
    if language:
        update["language_preferences"] = language
    if topic:
        update["topics_preferences"] = topic  
    print(update)
    result = db.users.update_one({"_id": user_id}, {"$pull": update})
    return result.modified_count



def set_update_profile_information(db, user_id, bio=None, name=None):
    update = {}
    if bio:
        update["bio"] = bio
    if name:
        update["name"] = name

    result = db.users.update_one({"_id": user_id}, {"$set": update})
    return result.modified_count

def Set_privacy(db, user_id, privacy_setting):
    result = db.users.update_one({"_id": user_id}, {"$set": {"privacy_setting": privacy_setting}})
    return result.modified_count

def get_common_preferences(db, limit=10):
    pipeline = [
        {"$unwind": "$topics_preferences"},
        {
            "$group": {
                "_id": "$topics_preferences",
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": limit}
    ]
    return list(db.users.aggregate(pipeline))

def get_users_by_name(db, name):
    users = db.users.find(
        {"name": {"$regex": name, "$options": "i"}},
        {"_id":1, "name": 1,"username": 1, "bio": 1, "social_links": 1, "privacy_setting": 1,"topics_preferences": 1}
    )
    users = list(users)
    result = []
    for user in users:
        if user.get("privacy_setting") == "private":
            result.append({
                "_id": user.get("_id"),
                "username": user["username"],
                "name": user.get("name")
                })
        else:
            result.append({
                "_id": user.get("_id"),
                "username": user["username"],
                "name": user.get("name"),
                "bio": user.get("bio"),
                "social_links": user.get("social_links"),
                "topics_preferences": user.get("topics_preferences")
            })
    # print(result)
    return result

def print_user(user):
    print(f"Username: {user['username']}")
    print(f"Name: {user['name']}")
    print(f"Bio: {user['bio']}")
    print("Social links:")
    for link in user['social_links']:
        print(f"    {link['platform']}: {link['url']}")
    print("Interests")
    for topic in user['topics_preferences']:
        print(f"    {topic}")
    print(f"ID: {user['_id']}")

def get_users_by_topic(db, topic):
    return list(db.users.find({"topics_preferences": topic, "privacy_setting": "public"}, {"_id":1,"name":1,"username": 1, "bio": 1, "social_links": 1,"topics_preferences": 1}))

def get_saved_posts(db, user_id):
    user = db.users.find_one(
        {"_id": (user_id)},  # Match the user by their 
        {"_id": 0, "saved_posts": 1}  # Only retrieve the saved_posts field
    )
    if user:
        return user.get("saved_posts", [])  # Return the saved_posts list or an empty list if not found
    return None

def set_add_social_link(db, user_id, platform, url):
    result = db.users.update_one(
        {"_id": user_id},
        {"$addToSet": {"social_links": {"platform": platform, "url": url}}}
    )
    return result.modified_count

def get_user_growth(db):
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$registration_timestamp"}},
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    days = list(db.users.aggregate(pipeline))
    for day in days:
        print(f"Day: {day['_id']}, Users registered: {day['count']}")

def set_user_add_interest(db, user_id, interests):
    result = db.users.update_one(
        {"_id": (user_id)},
        {"$addToSet": {"topics_preferences": {"$each": interests}}}
    )
    return result.modified_count

def add_report_post(db, reporting_user_id, reported_content_id, report_reason):
    report = Report(
        reporting_user_id=reporting_user_id,
        reported_content_id=reported_content_id,
        report_reason=report_reason
    )
    result = db.reports.insert_one(report.model_dump(by_alias=True))
    return result.inserted_id

def get_reported_posts(db, limit=10):
    pipeline = [
        {
            "$group": {
                "_id": "$reported_content_id",
                "report_count": {"$sum": 1}
            }
        },
        {"$sort": {"report_count": -1}},
        {"$limit": limit}
    ]
    reports = list(db.reports.aggregate(pipeline))
    for report in reports:
        print(f"Post ID: {report['_id']}, Reports: {report['report_count']}")


def add_notification(db, user_id, notif_type, content):
    notification = Notification(
        user_id=user_id,
        type=notif_type,
        content=content
    )
    result = db.notifications.insert_one(notification.model_dump(by_alias=True))
    return result.inserted_id


def get_noficitation(db, user_id):
    return list(db.notifications.find({"user_id": user_id}).sort("timestamp", -1).limit(10))


def delete_all(db):
    try:
        collection_names = db.list_collection_names()
        for collection_name in collection_names:
            db[collection_name].drop_indexes()
            db[collection_name].drop()
    except Exception as e:
        return


def get_Log_In(db, email, password):
    user = db.users.find_one({"email": email, "password": password}, {"_id": 1})
    if user:
        return str(user["_id"])
    return None


def get_topics_preferences(db, user_id):
    user = db.users.find_one({"_id": user_id}, {"topics_preferences": 1})
    if user:
        return user.get("topics_preferences", [])
    return None


def get_language_preferences(db, user_id):
    user = db.users.find_one({"_id": user_id}, {"language_preferences": 1})
    if user:
        return user.get("language_preferences", [])
    return None

def add_saved_post(db, user_id, post_id):
    result = db.users.update_one(
        {"_id": user_id},  # Match the user by their 
        {"$addToSet": {"saved_posts": post_id}}  # Add post_id to the saved_posts array if not already present
    )
    return result.modified_count


def get_uid_by_username(db, username):
    user = db.users.find_one(
        {"username": {"$regex": username, "$options": "i"}},
        {"_id": 1}  # Only return the _id field
    )
    if user:
        return str(user["_id"])
    return None


def get_username_by_uid(db, id):
    user = db.users.find_one(
        {"_id": id}, 
        {"username": 1}               
    )
    # print(user)
    if user:
        return str(user["username"])
    return None