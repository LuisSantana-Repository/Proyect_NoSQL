#!/usr/bin/env python3
import datetime
import json

import pydgraph


def set_schema(client):
    schema = """
    type User {
        username
        follows
        mongo
        blocked
        senr_message
        follow_request
    }
    type Message {
    content: string
    timestamp: datetime
    receiver: uid
    }
    receiver: uid @reversed
    content: string .
    timestamp: datetime .
    receiver: uid .
    username: string @index(fulltext) .
    follows: [uid] .
    blocked: [uid] @reverse .
    sent_message: [uid] @reverse .
    follow_request: [uid] @reverse .
    mongo: string @index(hex) .
    """
    return client.alter(pydgraph.Operation(schema=schema))

def createUser(client,name,)