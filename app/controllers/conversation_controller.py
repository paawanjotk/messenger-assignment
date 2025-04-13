from fastapi import HTTPException, status
from app.models.cassandra_models import ConversationModel
from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse

class ConversationController:
    """
    Controller for handling conversation operations
    This is a stub that students will implement
    """
    
    async def get_user_conversations(
        self, 
        user_id: str, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedConversationResponse:
        """
        Get all conversations for a user with pagination
        
        Args:
            user_id: ID of the user
            page: Page number
            limit: Number of conversations per page
            
        Returns:
            Paginated list of conversations
            
        Raises:
            HTTPException: If user not found or access denied
        """
        # This is a stub - students will implement the actual logic
        try:
            rows = await ConversationModel.get_user_conversations(user_id, page, limit)
            print(rows)
            conversations = [
                ConversationResponse(
                    id=str(row["conversation_id"]),
                    user1_id=str(row["user_id"]),
                    user2_id=str(row["other_user_id"]),
                    last_message_at=row["last_message_at"]
                )
                for row in rows
            ]

            return PaginatedConversationResponse(
                data=conversations,
                page=page,
                limit=limit,
                total=len(conversations)  
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error fetching conversations: {str(e)}"
            )
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Method not implemented"
        )
    
    async def get_conversation(self, conversation_id: int) -> ConversationResponse:
        """
        Get a specific conversation by ID
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            Conversation details
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        # This is a stub - students will implement the actual logic
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Method not implemented"
        ) 