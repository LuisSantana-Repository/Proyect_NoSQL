#!/usr/bin/env python3
from datetime import datetime
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
    user_id TEXT,
    post_id TEXT,
    parent_id TEXT,
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
    user_id TEXT,
    post_id TEXT,
    parent_id TEXT,
    content TEXT,
    timestamp TIMESTAMP,
    tags SET<TEXT>,
    category TEXT,
    language TEXT,
    PRIMARY KEY (category, language, timestamp)
    ) WITH CLUSTERING ORDER BY (language ASC, timestamp DESC);
"""

CREATE_POST_BY_PARENT = """
    CREATE TABLE IF NOT EXISTS Posts_by_parent (
    user_id TEXT,
    post_id TEXT,
    parent_id TEXT,
    content TEXT,
    timestamp TIMESTAMP,
    tags SET<TEXT>,
    category TEXT,
    language TEXT,
    PRIMARY KEY (parent_id, post_id)
    );
"""

CREATE_POST_LIKES = """
    CREATE TABLE IF NOT EXISTS Post_likes_count (
    post_id TEXT PRIMARY KEY,
    likes_count COUNTER
    );
"""

CREATE_COMMENTS_COUNT = """
    CREATE TABLE IF NOT EXISTS Post_comments_count (
    post_id TEXT PRIMARY KEY,
    comments_count COUNTER
    );
"""

# CREATE_POST_LIKES_ORDERED = """
#     CREATE TABLE Post_likes_ordered (
#     likes_count INT,
#     post_id TEXT,
#     PRIMARY KEY (likes_count, post_id)
# );
# """

# CREATE_POST_COMMENTS_ORDERED = """
# CREATE TABLE Post_comments_ordered (
#     comments_count INT,
#     post_id TEXT,
#     PRIMARY KEY (comments_count, post_id)
# );
# """

CREATE_LOGIN_USER = """
    CREATE TABLE IF NOT EXISTS Login_by_user (
    user_id TEXT,
    login_timestamp TIMESTAMP,
    ip_address TEXT,
    PRIMARY KEY (user_id, login_timestamp)
    ) WITH CLUSTERING ORDER BY (login_timestamp DESC);
"""

CREATE_LOGIN_DATE = """
    CREATE TABLE IF NOT EXISTS Login_by_date (
    user_id TEXT,
    login_timestamp TIMESTAMP,
    ip_address TEXT,
    PRIMARY KEY (login_timestamp)
    );
"""

CREATE_ACTIVITY_TABLE = """
    CREATE TABLE IF NOT EXISTS Activity (
    user_id TEXT,
    activity_timestamp TIMESTAMP,
    action_type TEXT,
    post_id TEXT,
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
    pbu_stmt = session.prepare("INSERT INTO Posts_by_user (user_id, post_id, content, timestamp, tags, category, language,parent_id) VALUES(?,?,?,?,?,?,?,?)")
    pbt_stmt = session.prepare("INSERT INTO Posts_by_topic (user_id, post_id, content, timestamp, tags, category, language,parent_id) VALUES(?,?,?,?,?,?,?,?)")
    pbp_stmt = session.prepare("INSERT INTO Posts_by_parent (user_id, post_id, content, timestamp, tags, category, language,parent_id) VALUES(?,?,?,?,?,?,?,?)")
    plc_stmt = session.prepare("UPDATE Post_likes_count SET likes_count = likes_count + ? WHERE post_id = ?")
    pcc_stmt = session.prepare("UPDATE Post_comments_count SET comments_count = comments_count + ? WHERE post_id = ?")
    Topics_stmt = session.prepare("UPDATE Topics SET usage_count = usage_count + ? WHERE topic = ?")
    Hastags_stmt = session.prepare("UPDATE Hashtags SET usage_count = usage_count + ? WHERE hashtag = ?")
    
    post_uid = str(uuid.uuid4())
    
    if(parrent is None):
        parrent = post_uid
        # Logs
        Action="Created a Post"
        insert_activity(session, userUUID, Action, post_uid)
    else:
        Action="Commented a Post"
        insert_activity(session, userUUID, Action, post_uid)
    print(f"Parent: {parrent}\nPost: {post_uid}")

    # Post inserts
    data = []
    time = datetime.now()
    data.append((userUUID,post_uid,post,time,set(hashtags),categoty,lenguage,parrent))
    session.execute(pbu_stmt, data[0])
    session.execute(pbt_stmt, data[0])
    session.execute(pbp_stmt, data[0])
    
    # Comments and likes counters insertion
    session.execute(plc_stmt, [0, post_uid])
    session.execute(pcc_stmt, [0, post_uid])
    
    # hashtags_check_insert=session.prepare("SELECT usage_count FROM Hashtags WHERE hashtag = ?")
    # hashtags_update_stmt = session.prepare("UPDATE Hashtags SET usage_count = usage_count + 1 WHERE hashtag = ?")
    # topic_check_insert =session.prepare("SELECT usage_count FROM Hashtags WHERE hashtag = ?")
    # topic_update_stmt = session.prepare("UPDATE Topics SET usage_count = usage_count + 1 WHERE topic = ?")

    # Hashtags update
    for hashtag in hashtags:
        # hashtag_check = session.execute(hashtags_check_insert, (hashtag))
        # if not hashtag_check.one():
        #     session.execute(Hastags_stmt, (hashtag, 1))
        # else:
        #     session.execute(hashtags_update_stmt, (hashtag,))
        session.execute(Hastags_stmt, (1, hashtag))
    
    # Topics update
    # topic_check = session.execute(topic_check_insert, (categoty))
    # if not topic_check.one():
    #     session.execute(Topics_stmt, (categoty, 1))
    # else:
    #     session.execute(topic_update_stmt, (categoty))
    session.execute(Topics_stmt, (1, categoty))
    print("Post succesfully created")
    

def comment_post(mongo_user_id,session,post,hashtags,categoty,lenguage,parent):
    # Insert new post
    insert_Post(mongo_user_id,session,post,hashtags,categoty,lenguage,parent)
    
    # # Obtain current count
    # get_count_stmt = session.prepare("SELECT comments_count FROM Post_comments_count WHERE post_id = ?")
    # rows = session.execute(get_count_stmt, (parent,))
    # current_comments_count = rows[0].comments_count

    # # Delete old count from ordered table
    # delete_stmt = session.prepare("DELETE FROM Post_comments_ordered WHERE comments_count = ? AND post_id = ?")
    # session.execute(delete_stmt, (current_comments_count, parent))

    # Increase counter
    pcc_stmt = session.prepare("UPDATE Post_comments_count SET comments_count = comments_count + 1 WHERE post_id = ?")
    session.execute(pcc_stmt, (parent,))

    # # Reinsert row
    # insert_stmt = session.prepare("INSERT INTO Post_comments_ordered (comments_count, post_id) VALUES (?, ?)")
    # session.execute(insert_stmt, (current_comments_count+1, parent))


def insert_activity(session, user, Action, post_id=None):
    # Logs
    time = datetime.now()
    data = []
    data.append((user, time, Action, post_id))
    Activity_stmt = session.prepare("INSERT INTO Activity (user_id, activity_timestamp, action_type, post_id) VALUES(?,?,?,?)")
    session.execute(Activity_stmt, data[0])

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
    # print(user)
    # user = uuid.TEXT(user)
    # print(user)
    session.execute(lbd_stmt, (timestamp, user, ip))
    session.execute(lbu_stmt, (user, timestamp, ip))
    
    # Logs
    Action = "Login"
    insert_activity(session, user, Action)


def add_Like(session, post_id, user_id):
    # # Obtain current count
    # get_count_stmt = session.prepare("SELECT likes_count FROM Post_likes_count WHERE post_id = ?")
    # rows = session.execute(get_count_stmt, (post_id,))
    # current_likes_count = rows[0].likes_count

    # # Delete old count from ordered table
    # delete_stmt = session.prepare("DELETE FROM Post_likes_ordered WHERE likes_count = ? AND post_id = ?")
    # session.execute(delete_stmt, (current_likes_count, post_id))

    # Increase counter
    plc_stmt = session.prepare("UPDATE Post_likes_count SET likes_count = likes_count + 1 WHERE post_id = ?")
    session.execute(plc_stmt, (post_id,))

    # # Reinsert row
    # insert_stmt = session.prepare("INSERT INTO Post_likes_ordered (likes_count, post_id) VALUES (?, ?)")
    # session.execute(insert_stmt, (current_likes_count+1, post_id))

def get_post_by_user(session, user_id):
    query = session.prepare("SELECT * FROM Posts_by_user WHERE user_id = ? ORDER BY timestamp DESC")
    rows = session.execute(query, (user_id,))
    # return list(rows)
    return rows

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

def get_most_liked_posts(session, limit=10):
    # query = "SELECT post_id, likes_count FROM Post_likes_ordered ORDER BY likes_count DESC LIMIT 10"
    # rows = session.execute(query)
    # print("Top 10 Most Liked Posts:")
    # for i, row in enumerate(rows, start=1):
    #     print(f"{i}. Post ID: {row.post_id}, Likes: {row.likes_count}")
    query = "SELECT post_id, likes_count FROM Post_likes_count"
    rows = session.execute(query)
    
    # Sort rows by likes_count in descending order
    sorted_rows = sorted(rows, key=lambda x: x.likes_count, reverse=True)
    
    # Print top 10 posts
    print(f"Top {limit} Most Liked Posts:")
    for i, row in enumerate(sorted_rows[:limit], start=1):
        print(f"{i}. Post ID: {row.post_id}, Likes: {row.likes_count}")

def get_most_commented_posts(session, limit=10):
    # query = "SELECT post_id, comments_count FROM Post_comments_ordered ORDER BY comments_count DESC LIMIT 10"
    # rows = session.execute(query)
    # print("Top 10 Most Commented Posts:")
    # for i, row in enumerate(rows, start=1):
    #     print(f"{i}. Post ID: {row.post_id}, Comments: {row.comments_count}")
    query = "SELECT post_id, comments_count FROM Post_comments_count"
    rows = session.execute(query)
    
    # Sort rows by comments_count in descending order
    sorted_rows = sorted(rows, key=lambda x: x.comments_count, reverse=True)
    
    # Print top 10 posts
    print(f"Top {limit} Most Commented Posts:")
    for i, row in enumerate(sorted_rows[:limit], start=1):
        print(f"{i}. Post ID: {row.post_id}, Comments: {row.comments_count}")

def print_post(session, post, username=None): 
    # Get likes
    get_count_stmt = session.prepare("SELECT likes_count FROM Post_likes_count WHERE post_id = ?")
    rows = session.execute(get_count_stmt, (post.post_id,))
    current_likes_count = rows[0].likes_count
    # Get comments
    get_count_stmt = session.prepare("SELECT comments_count FROM Post_comments_count WHERE post_id = ?")
    rows = session.execute(get_count_stmt, (post.post_id,))
    current_comments_count = rows[0].comments_count
    if username:
        print(f"User: {username}")
    print(f"Content: {post.content}")
    print(f"Comments: {current_comments_count}    Likes: {current_likes_count}")
    print(f"Timestamp: {post.timestamp}")
    print(f"Post ID: {post.post_id}")

def create_keyspace(session, keyspace, replication_factor):
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    session.execute(CREATE_POST_BY_USER)
    session.execute(CREATE_POST_BY_TOPIC)
    session.execute(CREATE_POST_BY_PARENT)
    session.execute(CREATE_POST_LIKES)
    session.execute(CREATE_COMMENTS_COUNT)
    session.execute(CREATE_POST_LIKES_ORDERED)
    session.execute(CREATE_POST_COMMENTS_ORDERED)
    session.execute(CREATE_LOGIN_USER)
    session.execute(CREATE_LOGIN_DATE)
    session.execute(CREATE_ACTIVITY_TABLE)
    session.execute(CREATE_TOPIC_TABLE)
    session.execute(CREATE_HASHTAGS_TABLE)

def delete_all(session):
    try:
        # Select all table names within the keyspace
        query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='socialmedia'"
        rows = session.execute(query)
        table_names = [row.table_name for row in rows]    
        # Drop each table
        for table_name in table_names:
            session.execute(f"DROP TABLE {table_name}")    
    except Exception as e:
        return

