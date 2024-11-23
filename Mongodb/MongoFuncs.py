#!/usr/bin/env python3
from Mongodb.mmodel import User, Report, Notification
from bson import ObjectId

def add_user_registtration(db, username, email, password, bio=None, name=None):
    user = User(
        username=username,
        email=email,
        password=password,
        bio=bio,
        name=name
    )
    result = db.users.insert_one(user.dict(by_alias=True))
    return result.inserted_id

def set_add_preferences(db, user_id, languages=None, tags=None):
    update = {}
    if languages:
        update["language_preferences"] = {"$addToSet": {"$each": languages}}
    if tags:
        update["topic_preferences"] = {"$addToSet": {"$each": tags}}
    
    result = db.users.update_one({"_id": ObjectId(user_id)}, {"$set": update})
    return result.modified_count


def set_remove_Preferences(db, user_id, languages=None, tags=None):
    update = {}
    if languages:
        update["language_preferences"] = {"$pull": {"$in": languages}}
    if tags:
        update["topic_preferences"] = {"$pull": {"$in": tags}}
    
    result = db.users.update_one({"_id": ObjectId(user_id)}, update)
    return result.modified_count



def set_update_profile_information(db, user_id, bio=None, name=None):
    update = {}
    if bio:
        update["bio"] = bio
    if name:
        update["name"] = name

    result = db.users.update_one({"_id": ObjectId(user_id)}, {"$set": update})
    return result.modified_count

def Set_privacy(db, user_id, privacy_setting):
    result = db.users.update_one({"_id": ObjectId(user_id)}, {"$set": {"privacy_setting": privacy_setting}})
    return result.modified_count

def get_common_preferences(db, limit=10):
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
    return list(db.users.aggregate(pipeline))

def get_users_by_name(db, name):
    return list(db.users.find({"username": {"$regex": name, "$options": "i"}}, {"username": 1, "bio": 1}))

def get_users_by_tag(db, tag):
    return list(db.users.find({"topic_preferences": tag, "privacy_setting": "public"}, {"username": 1, "bio": 1, "topic_preferences": 1}))

def get_save_post(db, user_id, post_id):
    result = db.users.update_one({"_id": ObjectId(user_id)}, {"$addToSet": {"saved_posts": post_id}})
    return result.modified_count

def get_folow_request(db, user_id, requester_id, action):
    if action == "accept":
        db.users.update_one({"_id": ObjectId(user_id)}, {"$pull": {"follow_requests": requester_id}})
    elif action == "deny":
        result = db.users.update_one({"_id": ObjectId(user_id)}, {"$pull": {"follow_requests": requester_id}})
        return result.modified_count
    return 0

def set_add_social_link(db, user_id, platform, url):
    result = db.users.update_one(
        {"_id": ObjectId(user_id)},
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
    return list(db.users.aggregate(pipeline))

def set_user_add_interest(db, user_id, interests):
    result = db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$addToSet": {"topic_preferences": {"$each": interests}}}
    )
    return result.modified_count

def add_report_post(db, reporting_user_id, reported_content_id, report_reason):
    report = Report(
        reporting_user_id=reporting_user_id,
        reported_content_id=reported_content_id,
        report_reason=report_reason
    )
    result = db.reports.insert_one(report.dict(by_alias=True))
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
    return list(db.reports.aggregate(pipeline))


def add_notification(db, user_id, notif_type, content):
    notification = Notification(
        user_id=user_id,
        type=notif_type,
        content=content
    )
    result = db.notifications.insert_one(notification.dict(by_alias=True))
    return result.inserted_id


def get_noficitation(db, user_id):
    return list(db.notifications.find({"user_id": user_id}).sort("timestamp", -1).limit(10))

def pop_noficitation(db, user_id):
    result = db.notifications.find_one_and_delete({"user_id": user_id}, sort=[("timestamp", 1)])
    return result

def delete_all(db):
    try:
        collection_names = db.list_collection_names()
        for collection_name in collection_names:
            db[collection_name].drop()
    except Exception as e:
        return


def get_Log_In(db, email, password):
    user = db.users.find_one({"email": email, "password": password}, {"_id": 1})
    if user:
        return str(user["_id"])
    return None
