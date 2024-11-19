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












