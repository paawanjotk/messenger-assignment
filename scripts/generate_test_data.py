"""
Script to generate test data for the Messenger application.
This script is a skeleton for students to implement.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_test_data(session):
    logger.info("Generating test data...")

    # 1. Generate users
    user_ids = []
    for i in range(NUM_USERS):
        user_id = uuid.uuid4()
        name = f"User{i+1}"
        email = f"user{i+1}@test.com"
        profile_image = f"https://picsum.photos/seed/user{i+1}/200"
        created_at = datetime.utcnow() - timedelta(days=random.randint(1, 100))

        session.execute("""
            INSERT INTO users (user_id, name, email, profile_image, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, name, email, profile_image, created_at))

        user_ids.append(user_id)
    
    logger.info(f"Inserted {NUM_USERS} users.")

    # 2. Create conversations between random pairs
    for _ in range(NUM_CONVERSATIONS):
        user_pair = random.sample(user_ids, 2)
        sender_id, receiver_id = user_pair[0], user_pair[1]
        conversation_id = uuid.uuid4()
        num_messages = random.randint(5, MAX_MESSAGES_PER_CONVERSATION)
        last_message_time = None

        for i in range(num_messages):
            created_at = datetime.utcnow() - timedelta(minutes=random.randint(0, 10000))
            message_id = uuid.uuid4()
            content = f"Hello from {sender_id} to {receiver_id} — msg {i+1}"

            # Insert into messages_by_conversation
            session.execute("""
                INSERT INTO messages_by_conversation (
                    conversation_id, created_at, message_id, sender_id, receiver_id, content
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (conversation_id, created_at, message_id, sender_id, receiver_id, content))

            # Insert into messages_by_id
            session.execute("""
                INSERT INTO messages_by_id (
                    message_id, conversation_id, sender_id, receiver_id, content, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (message_id, conversation_id, sender_id, receiver_id, content, created_at))

            # Track the latest message time for convo
            if not last_message_time or created_at > last_message_time:
                last_message_time = created_at

        # Update conversations_by_user for both users
        for user_id, other_user_id in [(sender_id, receiver_id), (receiver_id, sender_id)]:
            session.execute("""
                INSERT INTO conversations_by_user (
                    user_id, last_message_at, conversation_id, other_user_id
                ) VALUES (%s, %s, %s, %s)
            """, (user_id, last_message_time, conversation_id, other_user_id))

    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages.")
    logger.info(f"User IDs range from 1 to {NUM_USERS}. Use them for testing API endpoints.")


def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main() 