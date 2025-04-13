CREATE TABLE messages_by_conversation (
  conversation_id UUID,
  message_id TIMEUUID,
  sender_id UUID,
  receiver_id UUID,
  content TEXT,
  created_at TIMESTAMP,
  PRIMARY KEY ((conversation_id), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

CREATE TABLE conversations_by_user (
  user_id UUID,
  conversation_id UUID,
  other_user_id UUID,
  last_message TEXT,
  last_message_time TIMESTAMP,
  PRIMARY KEY ((user_id), last_message_time)
) WITH CLUSTERING ORDER BY (last_message_time DESC);

CREATE TABLE messages_by_id (
  message_id TIMEUUID PRIMARY KEY,
  conversation_id UUID,
  sender_id UUID,
  receiver_id UUID,
  content TEXT,
  created_at TIMESTAMP
);