#!/usr/bin/env python3
from pymongo import MongoClient
from model import User, Report, Notification
from bson import ObjectId
client = MongoClient("mongodb://localhost:27017/")
db = client['iteso']
collection = db['my_collection']


def add_user_registtration(username, email, password, bio=None, name=None):
    user = User(
        username=username,
        email=email,
        password=password,
        bio=bio,
        name=name
    )
    result = collection.insert_one(user.dict(by_alias=True))
    return result.inserted_id

def set_add_preferences(user_id, languages=None, tags=None):
    update = {}
    if languages:
        update["language_preferences"] = {"$addToSet": {"$each": languages}}
    if tags:
        update["topic_preferences"] = {"$addToSet": {"$each": tags}}
    
    result = collection.update_one({"_id": ObjectId(user_id)}, {"$set": update})
    return result.modified_count


def set_remove_Preferences(user_id, languages=None, tags=None):
    update = {}
    if languages:
        update["language_preferences"] = {"$pull": {"$in": languages}}
    if tags:
        update["topic_preferences"] = {"$pull": {"$in": tags}}
    
    result = collection.update_one({"_id": ObjectId(user_id)}, update)
    return result.modified_count



def set_update_profile_information(user_id, bio=None, name=None):
    update = {}
    if bio:
        update["bio"] = bio
    if name:
        update["name"] = name

    result = collection.update_one({"_id": ObjectId(user_id)}, {"$set": update})
    return result.modified_count

def Set_privacy(user_id, privacy_setting):
    result = collection.update_one({"_id": ObjectId(user_id)}, {"$set": {"privacy_setting": privacy_setting}})
    return result.modified_count

def get_common_preferences(limit=10):
    pipeline = [
        {"$unwind": "$topic_preferences"},
        {
            "$group": {
                "_id": "$topic_preferences",
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": limit}
    ]
    return list(collection.aggregate(pipeline))

def get_users_by_name(name):
    return list(collection.find({"username": {"$regex": name, "$options": "i"}}, {"username": 1, "bio": 1}))

def get_users_by_tag(tag):
    return list(collection.find({"topic_preferences": tag, "privacy_setting": "public"}, {"username": 1, "bio": 1, "topic_preferences": 1}))

def get_save_post(user_id, post_id):
    result = collection.update_one({"_id": ObjectId(user_id)}, {"$addToSet": {"saved_posts": post_id}})
    return result.modified_count

def get_folow_request(user_id, requester_id, action):
    if action == "accept":
        collection.update_one({"_id": ObjectId(user_id)}, {"$pull": {"follow_requests": requester_id}})
    elif action == "deny":
        result = collection.update_one({"_id": ObjectId(user_id)}, {"$pull": {"follow_requests": requester_id}})
        return result.modified_count
    return 0

def set_add_social_link(user_id, platform, url):
    result = collection.update_one(
        {"_id": ObjectId(user_id)},
        {"$addToSet": {"social_links": {"platform": platform, "url": url}}}
    )
    return result.modified_count

def get_user_growth():
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$registration_timestamp"}},
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

def set_user_add_interest(user_id, interests):
    result = collection.update_one(
        {"_id": ObjectId(user_id)},
        {"$addToSet": {"topic_preferences": {"$each": interests}}}
    )
    return result.modified_count

def add_report_post(reporting_user_id, reported_content_id, report_reason):
    report = Report(
        reporting_user_id=reporting_user_id,
        reported_content_id=reported_content_id,
        report_reason=report_reason
    )
    result = db.reports.insert_one(report.dict(by_alias=True))
    return result.inserted_id

def get_reported_posts(limit=10):
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
    return list(db.reports.aggregate(pipeline))


def add_notification(user_id, notif_type, content):
    notification = Notification(
        user_id=user_id,
        type=notif_type,
        content=content
    )
    result = db.notifications.insert_one(notification.dict(by_alias=True))
    return result.inserted_id


def get_noficitation(user_id):
    return list(db.notifications.find({"user_id": user_id}).sort("timestamp", -1).limit(10))

def pop_noficitation(user_id):
    result = db.notifications.find_one_and_delete({"user_id": user_id}, sort=[("timestamp", 1)])
    return result