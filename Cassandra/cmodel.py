#!/usr/bin/env python3
import datetime
import json
import logging
import random
import uuid
import time_uuid
from cassandra.query import BatchStatement


#cassandra
CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""


CREATE_POST_BY_USER = """
    CREATE TABLE IF NOT EXISTS Posts_by_user (
    user_id UUID,
    post_id UUID,
    parent_id UUID,
    content TEXT,
    timestamp TIMESTAMP,
    tags SET<TEXT>,
    category TEXT,
    language TEXT,
    PRIMARY KEY (user_id, timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);
"""


CREATE_POST_BY_TOPIC = """
    CREATE TABLE IF NOT EXISTS Posts_by_topic (
    user_id UUID,
    post_id UUID,
    parent_id UUID,
    content TEXT,
    timestamp TIMESTAMP,
    tags SET<TEXT>,
    category TEXT,
    language TEXT,
    PRIMARY KEY (category, language, timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);
"""

CREATE_POST_BY_PARENT = """
    CREATE TABLE IF NOT EXISTS Posts_by_parent (
    user_id UUID,
    post_id UUID,
    parent_id UUID,
    content TEXT,
    timestamp TIMESTAMP,
    tags SET<TEXT>,
    category TEXT,
    language TEXT,
    PRIMARY KEY (parent_id)
    );
"""

CREATE_POST_LIKES = """
    CREATE TABLE IF NOT EXISTS Post_likes_count (
    post_id UUID PRIMARY KEY,
    likes_count COUNTER
    );
"""

CREATE_COMMENTS_COUNT = """
    CREATE TABLE IF NOT EXISTS Post_comments_count (
    post_id UUID PRIMARY KEY,
    comments_count COUNTER
    );
"""

CREATE_LOGIN_USER = """
    CREATE TABLE IF NOT EXISTS Login_by_user (
    user_id UUID,
    login_timestamp TIMESTAMP,
    ip_address TEXT,
    PRIMARY KEY (user_id, login_timestamp)
    ) WITH CLUSTERING ORDER BY (login_timestamp DESC);
"""

CREATE_LOGIN_DATE = """
    CREATE TABLE IF NOT EXISTS Login_by_date (
    user_id UUID,
    login_timestamp TIMESTAMP,
    ip_address TEXT,
    PRIMARY KEY (login_timestamp)
    );
"""

CREATE_ACTIVITY_TABLE = """
    CREATE TABLE IF NOT EXISTS Activity (
    user_id UUID,
    activity_timestamp TIMESTAMP,
    action_type TEXT,
    post_id UUID,
    PRIMARY KEY (user_id, activity_timestamp)
    ) WITH CLUSTERING ORDER BY (activity_timestamp DESC);
"""

CREATE_TOPIC_TABLE = """
    CREATE TABLE IF NOT EXISTS Topics (
    topic TEXT PRIMARY KEY,
    usage_count COUNTER
    );
"""
CREATE_HASHTAGS_TABLE = """
    CREATE TABLE IF NOT EXISTS  Hashtags (
    hashtag TEXT PRIMARY KEY,
    usage_count COUNTER
    );
"""


def insert_Post(userUUID,session, post,hashtags,categoty,lenguage, parrent):
    #asuming userUUID is goten from a mongo request or a server that has that user uuid
    pbu_stmt = session.prepare("INSERT INTO Posts_by_user (user_id, post_id, content, timestamp, tags, category, language,parent_id) VALUES(?,?,?,?,?,?,?)")
    pbt_stmt = session.prepare("INSERT INTO Posts_by_topic (user_id, post_id, content, timestamp, tags, category, language,parent_id) VALUES(?,?,?,?,?,?,?)")
    pbp_stmt = session.prepare("INSERT INTO Posts_by_parent (user_id, post_id, content, timestamp, tags, category, language,parent_id) VALUES(?,?,?,?,?,?,?)")
    plc_stmt = session.prepare("INSERT INTO Post_likes_count(post_id,likes_count) VALUES(?,?)")
    pcc_stmt = session.prepare("INSERT INTO Post_comments_count(post_id,comments_count) VALUES(?,?)")
    Activity_stmt = session.prepare("INSERT INTO Activity (user_id, activity_timestamp, action_type, post_id) VALUES(?,?,?,?)")
    Topics_stmt = session.prepare("INSERT INTO Topics (topic, usage_count) VALUES(?, ?)")
    Hastags_stmt = session.prepare("INSERT INTO Hashtags (hashtag, usage_count) VALUES(?, ?)")
    
    post_uid = str(uuid.uuid4())
    if(parrent is None):
        parrent = post_uid
    if parrent != post_uid:  # This is a comment
        pcc_stmt = session.prepare("UPDATE Post_comments_count SET comments_count = comments_count + 1 WHERE post_id = ?")
        session.execute(pcc_stmt, (parrent))
    time = datetime.now()
    data = []
    data.append((userUUID,post_uid,post,time,set(hashtags),categoty,lenguage,parrent))
    #pbu,pbt,pbp
    session.execute(pbu_stmt, data[0])
    session.execute(pbt_stmt, data[0])
    session.execute(pbp_stmt, data[0])
    
    
    
    data = []
    count = 0
    data.append((post_uid,count))
    #plc, pcc
    session.execute(plc_stmt, data[0])
    session.execute(pcc_stmt, data[0])
    
    Action="Created a Post"
    data = []
    data.append((userUUID,time,Action,post_uid))
    #activity
    session.execute(Activity_stmt, data[0])
    
    
    hashtags_check_insert=session.prepare("SELECT usage_count FROM Hashtags WHERE hashtag = ?")
    hashtags_update_stmt = session.prepare("UPDATE Hashtags SET usage_count = usage_count + 1 WHERE hashtag = ?")
    topic_check_insert =session.prepare("SELECT usage_count FROM Hashtags WHERE hashtag = ?")
    topic_update_stmt = session.prepare("UPDATE Topics SET usage_count = usage_count + 1 WHERE topic = ?")

    for hashtag in hashtags:
            hashtag_check = session.execute(hashtags_check_insert, (hashtag))
            if not hashtag_check.one():
                session.execute(Hastags_stmt, (hashtag, 1))
            else:
                session.execute(hashtags_update_stmt, (hashtag,))
    
    topic_check = session.execute(topic_check_insert, (categoty))
    if not topic_check.one():
        session.execute(Topics_stmt, (categoty, 1))
    else:
        session.execute(topic_update_stmt, (categoty))


def insert_logIn(session,user,ip):
    timestamp = datetime.now()
    lbu_stmt = session.prepare("""
        INSERT INTO Login_by_user (user_id, login_timestamp, ip_address)
        VALUES (?, ?, ?)
    """)
    lbd_stmt = session.prepare("""
        INSERT INTO Login_by_date (login_timestamp, user_id, ip_address)
        VALUES (?, ?, ?)
    """)
    session.execute(lbd_stmt, (timestamp, user, ip))
    session.execute(lbu_stmt, (user, timestamp, ip))


def add_Like(session, post_id, user_id):
    plc_stmt = session.prepare("UPDATE Post_likes_count SET likes_count = likes_count + 1 WHERE post_id = ?")
    session.execute(plc_stmt, (post_id,))
    Action="Liked a post"
    time = datetime.now()
    data = []
    data.append((user_id,time,Action,post_id))

def get_post_by_user(session, user_id):
    query = session.prepare("SELECT * FROM Posts_by_user WHERE user_id = ? ORDER BY timestamp DESC")
    rows = session.execute(query, (user_id))
    return list(rows)

def get_post_by_topic(session, category):
    query = session.prepare("SELECT * FROM Posts_by_topic WHERE category = ? ORDER BY timestamp DESC")
    rows = session.execute(query, (category,))
    return list(rows)

def get_comments_for_post(session, parent_id):
    query = session.prepare("SELECT * FROM Posts_by_parent WHERE parent_id = ?")
    rows = session.execute(query, (parent_id,))
    return list(rows)

def get_LoginUser(session, user_id):
    query = session.prepare("""
        SELECT * FROM Login_by_user WHERE user_id = ? ORDER BY login_timestamp DESC
    """)
    rows = session.execute(query, (user_id,))
    return list(rows)

def get_dailyLogin(session):
    today = datetime.now().date()
    tomorrow = today + datetime.timedelta(days=1)
    query = session.prepare("""
        SELECT COUNT(DISTINCT user_id) AS daily_active_users
        FROM Login_by_date
        WHERE login_timestamp >= ? AND login_timestamp < ?
    """)
    result = session.execute(query, (today, tomorrow))
    return result.one().daily_active_users


def get_dailyActivityes(session, user_id):
    today = datetime.now().date()
    tomorrow = today + datetime.timedelta(days=1)

    query = session.prepare("""
        SELECT * FROM Activity
        WHERE user_id = ? AND activity_timestamp >= ? AND activity_timestamp < ?
        ORDER BY activity_timestamp DESC
    """)
    rows = session.execute(query, (user_id, today, tomorrow))
    return list(rows)

def get_popularTopics(session, limit=10):
    query = session.prepare("""
        SELECT topic, usage_count
        FROM Topics
        ORDER BY usage_count DESC
        LIMIT ?
    """)
    rows = session.execute(query, (limit,))
    return list(rows)


def get_popularHashtags(session, limit=10):
    query = session.prepare("""
        SELECT hashtag, usage_count
        FROM Hashtags
        ORDER BY usage_count DESC
        LIMIT ?
    """)
    rows = session.execute(query, (limit))
    return list(rows)

def create_keyspace(session, keyspace, replication_factor):
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    session.execute(CREATE_POST_BY_USER)
    session.execute(CREATE_POST_BY_TOPIC)
    session.execute(CREATE_POST_BY_PARENT)
    session.execute(CREATE_POST_LIKES)
    session.execute(CREATE_COMMENTS_COUNT)
    session.execute(CREATE_LOGIN_USER)
    session.execute(CREATE_LOGIN_DATE)
    session.execute(CREATE_ACTIVITY_TABLE)
    session.execute(CREATE_TOPIC_TABLE)
    session.execute(CREATE_HASHTAGS_TABLE)