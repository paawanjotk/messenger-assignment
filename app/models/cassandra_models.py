"""
Sample models for interacting with Cassandra tables.
Students should implement these models based on their database schema design.
"""
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from app.db.cassandra_client import cassandra_client

class MessageModel:
    """
    Message model for interacting with the messages table.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve messages
    - How to handle pagination of results
    - How to filter messages by timestamp
    """
    
    # TODO: Implement the following methods
    
    @staticmethod
    async def create_message(sender_id: str, receiver_id: str, content: str, **kwargs):
        """
        Create a new message.
        
        Students should decide what parameters are needed based on their schema design.
        """
        session = cassandra_client.get_session()
        created_at = datetime.now(timezone.utc)
        message_id = uuid.uuid1()
        participants = sorted([sender_id, receiver_id])
        sender_id = uuid.UUID(sender_id)
        receiver_id = uuid.UUID(receiver_id)
        conversation_id = uuid.uuid5(uuid.NAMESPACE_DNS, "-".join(participants))
        # This is a stub - students will implement the actual logic

        
        session.execute("""
            INSERT INTO messages_by_conversation (
                conversation_id, message_id, sender_id, receiver_id, content, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (conversation_id, message_id, sender_id, receiver_id, content, created_at))

        session.execute("""
            INSERT INTO messages_by_id (
                message_id, conversation_id, sender_id, receiver_id, content, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (message_id, conversation_id, sender_id, receiver_id, content, created_at))

        for user_id, other_user_id in [(sender_id, receiver_id), (receiver_id, sender_id)]:
            session.execute("""
                INSERT INTO conversations_by_user (
                    user_id, conversation_id, other_user_id, last_message_at
                ) VALUES (%s, %s, %s, %s)
            """, (user_id, conversation_id, other_user_id, created_at))


        return {
            "id": str(message_id),
            "conversation_id": str(conversation_id),
            "sender_id": str(sender_id),
            "receiver_id": str(receiver_id),
            "content": content,
            "created_at": created_at
        }
        raise NotImplementedError("This method needs to be implemented")
    
    @staticmethod
    async def get_conversation_messages(conversation_id: str, page: int = 1, limit: int = 20):
        """
        Get messages for a conversation with pagination.
        
        Students should decide what parameters are needed and how to implement pagination.
        """
        # This is a stub - students will implement the actual logic

        session = cassandra_client.get_session()
        conversation_id = uuid.UUID(conversation_id)
        query = """
        SELECT message_id, sender_id, receiver_id, content, created_at, conversation_id
        FROM messages_by_conversation
        WHERE conversation_id = %s
        """
        result = session.execute(query, (conversation_id,))
        rows = list(result)
        print(rows)
        # Apply pagination manually (Cassandra doesn't support OFFSET)
        offset = (page - 1) * limit
        paginated = rows[offset:offset + limit]

        return paginated
        raise NotImplementedError("This method needs to be implemented")
    
    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: str, 
        before_timestamp: datetime, 
        page: int = 1, 
        limit: int = 20
    ):
        """
        Get messages before a timestamp with pagination.
        
        Students should decide how to implement filtering by timestamp with pagination.
        """
        # This is a stub - students will implement the actual logic
        
        session = cassandra_client.get_session()
        conversation_id = uuid.UUID(conversation_id)
        query = """
        SELECT message_id, sender_id, receiver_id, content, created_at, conversation_id
        FROM messages_by_conversation
        WHERE conversation_id = %s AND created_at < %s
        LIMIT %s
        """
        result = session.execute(query, (conversation_id, before_timestamp, limit * page))
        rows = list(result)

        # Manual pagination
        offset = (page - 1) * limit
        paginated = rows[offset:offset + limit]

        return paginated
        raise NotImplementedError("This method needs to be implemented")


class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve conversations for a user
    - How to handle pagination of results
    - How to optimize for the most recent conversations
    """
    
    # TODO: Implement the following methods
    
    @staticmethod
    async def get_user_conversations(user_id: str, page: int = 1, limit: int = 20):
        """
        Get conversations for a user with pagination.
        
        Students should decide what parameters are needed and how to implement pagination.
        """
        # This is a stub - students will implement the actual logic
        offset = (page - 1) * limit
        user_id = uuid.UUID(user_id)
        query = """
        SELECT conversation_id, user_id, other_user_id, last_message_at 
        FROM conversations_by_user 
        WHERE user_id = %s 
        LIMIT %s;
        """
        rows = cassandra_client.execute(query, (user_id, offset + limit))
        
        # Apply pagination manually
        paginated_rows = rows[offset:offset + limit]
        
        return paginated_rows
        raise NotImplementedError("This method needs to be implemented")
    
    @staticmethod
    async def get_conversation(*args, **kwargs):
        """
        Get a conversation by ID.
        
        Students should decide what parameters are needed and what data to return.
        """
        # This is a stub - students will implement the actual logic
        raise NotImplementedError("This method needs to be implemented")
    
    @staticmethod
    async def create_or_get_conversation(*args, **kwargs):
        """
        Get an existing conversation between two users or create a new one.
        
        Students should decide how to handle this operation efficiently.
        """
        # This is a stub - students will implement the actual logic
        raise NotImplementedError("This method needs to be implemented") 