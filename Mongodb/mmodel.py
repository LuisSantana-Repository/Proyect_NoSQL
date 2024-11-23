#!/usr/bin/env python3
import uuid
from typing import Optional, List
from pydantic import BaseModel, Field
from datetime import datetime


class User(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), alias="_id")
    username: str = Field(...)
    email: str = Field(...)
    password: str = Field(...)
    bio: Optional[str] = Field(None)
    name: Optional[str] = Field(None)
    privacy_setting: str = Field(default="public")  # 'public' or 'private'
    language_preferences: List[str] = Field(default=[])
    tag_preferences: List[str] = Field(default=[])
    social_links: List[dict] = Field(default_factory=list)  # [{"platform": "Twitter", "url": "https://..."}]
    saved_posts: List[str] = Field(default=[])
    registration_timestamp: datetime = Field(default_factory=datetime.now)
    follow_requests: List[str] = Field(default=[])

    class Config:
        populate_by_name = True
        json_schema_extra  = {
            "example": {
                "_id": "abc123",
                "username": "john_doe",
                "email": "john.doe@example.com",
                "password": "securepassword",
                "bio": "Loves books and programming.",
                "name": "John Doe",
                "privacy_setting": "private",
                "language_preferences": ["en", "es"],
                "tag_preferences": ["technology", "science"],
                "social_links": [
                    {"platform": "Twitter", "url": "https://twitter.com/john_doe"},
                    {"platform": "GitHub", "url": "https://github.com/johndoe"}
                ],
                "saved_posts": ["post123", "post456"],
                "registration_timestamp": "2024-11-01T10:00:00Z",
                "follow_requests": ["user789"]
            }
        }


class Report(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), alias="_id")
    reporting_user_id: str = Field(...)
    reported_content_id: str = Field(...)
    report_reason: str = Field(...)
    registration_timestamp: datetime = Field(default_factory=datetime.now) #default factory is able to make dafults but with variable data

    class Config:
        populate_by_name  = True
        json_schema_extra  = {
            "example": {
                "_id": "report123",
                "reporting_user_id": "user456",
                "reported_content_id": "post789",
                "report_reason": "Inappropriate content",
                "timestamp": "2024-11-01T12:30:00Z"
            }
        }


class Notification(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), alias="_id")
    user_id: str = Field(...)
    type: str = Field(...) 
    content: str = Field(...)
    timestamp: datetime = Field(default_factory=datetime.now) #default factory is able to make dafults but with variable data

    class Config:
        populate_by_name  = True
        json_schema_extra  = {
            "example": {
                "_id": "notif123",
                "user_id": "user789",
                "type": "friend_request",
                "content": "John Doe sent you a friend request.",
                "timestamp": "2024-11-01T14:00:00Z"
            }
        }
