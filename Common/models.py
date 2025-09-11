from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator
from Common.logger import CustomLogger

logger = CustomLogger().get_logger()


class Animal(BaseModel):
    id: int = Field(..., description="Unique animal identifier")
    name: str = Field(..., description="Animal name")
    # Fields that need transformation
    friends: Union[str, List[str]] = Field(
        default_factory=list, 
        description="Animal friends - can be CSV string or list"
    )
    born_at: Optional[Union[str, datetime]] = Field(
        None, 
        description="Birth timestamp - can be string or datetime"
    )
    
    class Config:
        """Pydantic configuration."""
        # Allow extra fields that might come from the API
        extra = "allow"
        # Use enum values instead of enum objects
        use_enum_values = True
        # Allow both string and datetime for born_at during parsing
        arbitrary_types_allowed = True
    
    @validator('friends', pre=True, always=True)
    def validate_friends(cls, v):
        if v is None:
            return []
        return v
    
    @validator('born_at', pre=True, always=True)
    def validate_born_at(cls, v):
        if v is None or v == "":
            return None
            
        # Keep original value for now - transformation happens separately
        return v
    
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Animal':
        try:
            return cls(**data)
        except Exception as e:
            logger.error(f"Error creating Animal from data {data}: {e}")
            raise
    
    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(
            exclude_none=False,  # Include None values
            by_alias=True,       # Use field aliases if defined
        )

    
    def __str__(self) -> str:
        # species_info = f" ({self.species})" if self.species else ""
        return f"Animal[{self.id}]: {self.name}"
    
    def __repr__(self) -> str:
        return f"Animal(id='{self.id}', name='{self.name}')"





  