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

def insert_Post(userUUID,session, post,tags,categoty,lenguage):
    pbu_stmt = session.prepare("INSERT INTO Posts_by_user (user_id, post_id, content, timestamp, tags, category, language) VALUES(?,?,?,?,?,?,?)")
    pbt_stmt = session.prepare("INSERT INTO Posts_by_topic (user_id, post_id, content, timestamp, tags, category, language) VALUES(?,?,?,?,?,?,?)")
    pbp_stmt = session.prepare("INSERT INTO Posts_by_parent (user_id, post_id, content, timestamp, tags, category, language) VALUES(?,?,?,?,?,?,?)")
    plc_stmt = session.prepare("INSERT INTO Post_likes_count(post_id,likes_count) VALUES(?,?)")
    pcc_stmt = session.prepare("INSERT INTO Post_comments_count(post_id,comments_count) VALUES(?,?)")
    Activity_stmt = session.prepare("INSERT INTO User_activity (user_id, action_type, target_id, timestamp) VALUES(?,?,?,?)")
    Topics_stmt = session.prepare("INSERT INTO Post_topics (post_id, topic) VALUES(?, ?)")
    Hastags_stmt = session.prepare("INSERT INTO Post_hashtags (post_id, hashtag) VALUES(?, ?)")

    
def insertComment(userUUID,session, post,tags,categoty,lenguage, parentUUID):
    pbu_stmt = session.prepare("INSERT INTO Posts_by_user (user_id, post_id, content, timestamp, tags, category, language) VALUES(?,?,?,?,?,?,?)")
    pbt_stmt = session.prepare("INSERT INTO Posts_by_topic (user_id, post_id, content, timestamp, tags, category, language) VALUES(?,?,?,?,?,?,?)")
    pbp_stmt = session.prepare("INSERT INTO Posts_by_parent (user_id, post_id, content, timestamp, tags, category, language) VALUES(?,?,?,?,?,?,?)")
    plc_stmt = session.prepare("INSERT INTO Post_likes_count(post_id,likes_count) VALUES(?,?)")
    pcc_stmt = session.prepare("INSERT INTO Post_comments_count(post_id,comments_count) VALUES(?,?)")
    Activity_stmt = session.prepare("INSERT INTO User_activity (user_id, action_type, target_id, timestamp) VALUES(?,?,?,?)")
    Topics_stmt = session.prepare("INSERT INTO Post_topics (post_id, topic) VALUES(?, ?)")
    Hastags_stmt = session.prepare("INSERT INTO Post_hashtags (post_id, hashtag) VALUES(?, ?)")


def insert_logIn(session,user,ip):
    lbu_stmt = session.prepare("INSERT INTO Login_by_user (user_id, timestamp, ip_address) VALUES(?,?,?)")
    lbd_stmt = session.prepare("INSERT INTO Login_by_device (device_id, timestamp, user_id) VALUES(?,?,?)")
    Activity_stmt = session.prepare("INSERT INTO User_activity (user_id, action_type, target_id, timestamp) VALUES(?,?,?,?)")

def insert_Likes(session,posts,):
    Activity_stmt = session.prepare("INSERT INTO User_activity (user_id, action_type, target_id, timestamp) VALUES(?,?,?,?)")
    likebu_stmt = session.prepare("INSERT INTO Likes_by_user (user_id, post_id, timestamp) VALUES(?,?,?)")
    plc_stmt = session.prepare("UPDATE Post_likes_count SET likes_count = likes_count + 1 WHERE post_id = ?")
