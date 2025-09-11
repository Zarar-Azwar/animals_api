from typing import List, Optional, Union, Dict, Any
from datetime import datetime
import dateutil.parser
from dateutil import tz

from Common.models import Animal

from Common.logger import CustomLogger


logger = CustomLogger().get_logger()


class AnimalTransformer:
    """
    Handles transformation of animal data fields.
    
    This class provides methods to transform animal data according to
    the requirements for the home endpoint.
    """
    
    def __init__(self):
        """Initialize the transformer."""
        self.stats = {
            'friends_transformed': 0,
            'born_at_transformed': 0,
            'transformation_errors': 0
        }
    
    def transform(self, animal: Animal) -> Animal:
        """
        Transform a single animal record.

        This method takes an Animal object and applies transformations on the
        friends and born_at fields according to the requirements for the home
        endpoint. The transformed data is then used to create a new Animal
        object, which is returned.

        If any errors occur during transformation, the exception is caught,
        logged, and re-raised.

        Args:
            animal (Animal): Animal record to transform

        Returns:
            Animal: Transformed Animal record
        """
        try:
            # Create a copy to avoid modifying the original
            animal_dict = animal.to_dict()
            
            # Transform friends field
            if 'friends' in animal_dict:
                animal_dict['friends'] = self.transform_friends(animal_dict['friends'])
            
            # Transform born_at field  
            if 'born_at' in animal_dict:
                animal_dict['born_at'] = self.transform_born_at(animal_dict['born_at'])
            
            # Create new Animal instance with transformed data
            return Animal.from_dict(animal_dict)
            
        except Exception as e:
            logger.error(f"Error transforming animal {animal.id}: {e}")
            self.stats['transformation_errors'] += 1
            raise
    
    def transform_friends(self, friends: Union[str, List[str], None]) -> List[str]:
        """
        Transform friends field from either string (CSV) or list of strings into a
        cleaned list of strings.

        If the input is None or empty, an empty list is returned. If the input is
        already a list, it is cleaned up by removing any whitespace and filtering
        out empty strings. If the input is a string, it is split by comma and
        cleaned up.

        If any errors occur during transformation, the exception is caught, logged,
        and re-raised.

        Args:
            friends (Union[str, List[str], None]): Friends field to transform

        Returns:
            List[str]: Transformed list of friends
        """
        try:
            # Handle None or empty cases
            if not friends:
                return []
            
            # If already a list, clean and return
            if isinstance(friends, list):
                # Clean up any whitespace and filter out empty strings
                cleaned_friends = [friend.strip() for friend in friends if friend and friend.strip()]
                logger.debug(f"Friends already list: {len(cleaned_friends)} friends")
                return cleaned_friends
            
            # Handle string case (CSV)
            if isinstance(friends, str):
                friends = friends.strip()
                
                # Handle empty string
                if not friends:
                    return []
                
                # Split by comma and clean up
                friend_list = [
                    friend.strip() 
                    for friend in friends.split(',') 
                    if friend.strip()
                ]
                
                self.stats['friends_transformed'] += 1
                logger.debug(f"Transformed friends CSV '{friends}' to list: {friend_list}")
                return friend_list
            
            # Handle unexpected types
            logger.warning(f"Unexpected friends type: {type(friends)}, converting to string")
            return self.transform_friends(str(friends))
            
        except Exception as e:
            logger.error(f"Error transforming friends '{friends}': {e}")
            return []
    
    def transform_born_at(self, born_at: Union[str, datetime, None]) -> Optional[str]:
        """
        Transform born_at field from either string (ISO8601), datetime object, or None.

        If the input is None or empty, None is returned. If the input is already a
        datetime object, it is converted to UTC and formatted as an ISO8601 string. If
        the input is a string, it is parsed to a datetime using dateutil and then
        converted to UTC and formatted as an ISO8601 string.

        If any errors occur during transformation, the exception is caught, logged, and
        re-raised.

        Args:
            born_at (Union[str, datetime, None]): Born_at field to transform

        Returns:
            Optional[str]: Transformed ISO8601 string or None if invalid
        """
        try:
            # Handle None or empty cases
            if not born_at:
                return None
            
            # If already a datetime, convert to UTC and format
            if isinstance(born_at, datetime):
                # Convert to UTC if timezone-aware, assume UTC if naive
                if born_at.tzinfo is None:
                    # Naive datetime - assume it's already UTC
                    utc_dt = born_at.replace(tzinfo=tz.UTC)
                else:
                    # Convert to UTC
                    utc_dt = born_at.astimezone(tz.UTC)
                
                iso_string = utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                self.stats['born_at_transformed'] += 1
                logger.debug(f"Transformed datetime to ISO8601: {iso_string}")
                return iso_string
            
            # Handle string case
            if isinstance(born_at, str):
                born_at = born_at.strip()
                
                # Handle empty string
                if not born_at:
                    return None
                
                try:
                    # Parse the string to datetime using dateutil
                    parsed_dt = dateutil.parser.parse(born_at)
                    
                    # Convert to UTC
                    if parsed_dt.tzinfo is None:
                        # Naive datetime - assume UTC
                        utc_dt = parsed_dt.replace(tzinfo=tz.UTC)
                    else:
                        # Convert to UTC
                        utc_dt = parsed_dt.astimezone(tz.UTC)
                    
                    iso_string = utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    self.stats['born_at_transformed'] += 1
                    logger.debug(f"Transformed born_at '{born_at}' to ISO8601: {iso_string}")
                    return iso_string
                    
                except (ValueError, TypeError, dateutil.parser.ParserError) as e:
                    logger.error(f"Failed to parse born_at string '{born_at}': {e}")
                    return None
            
            # Handle unexpected types
            logger.warning(f"Unexpected born_at type: {type(born_at)}, attempting string conversion")
            return self.transform_born_at(str(born_at))
            
        except Exception as e:
            logger.error(f"Error transforming born_at '{born_at}': {e}")
            return None
    
    def transform_batch(self, animals: List[Animal]) -> List[Animal]:
        """
        Transform a batch of Animal objects.

        This method takes a list of Animal objects and applies the transformation
        to each one, returning a new list of transformed Animal objects. If any
        errors occur during transformation, the exception is caught, logged, and
        ignored, allowing the process to continue with other animals in the
        batch.

        Args:
            animals (List[Animal]): List of Animal objects to transform

        Returns:
            List[Animal]: List of transformed Animal objects
        """
        transformed_animals = []
        
        for animal in animals:
            try:
                transformed_animal = self.transform(animal)
                transformed_animals.append(transformed_animal)
            except Exception as e:
                logger.error(f"Skipping animal {animal.id} due to transformation error: {e}")
                # Continue processing other animals rather than failing the entire batch
                continue
                
        return transformed_animals
    
    def get_stats(self) -> Dict[str, int]:
        return self.stats.copy()
    
    def reset_stats(self):
        self.stats = {
            'friends_transformed': 0,
            'born_at_transformed': 0,
            'transformation_errors': 0
        }
    
    def validate_transformation(self, original: Animal, transformed: Animal) -> bool:
        try:
            # Basic validation - ensure core fields are preserved
            if original.id != transformed.id:
                logger.error(f"ID mismatch: {original.id} != {transformed.id}")
                return False
                
            if original.name != transformed.name:
                logger.error(f"Name mismatch: {original.name} != {transformed.name}")
                return False
            
            # Validate friends transformation
            if hasattr(transformed, 'friends') and transformed.friends is not None:
                if not isinstance(transformed.friends, list):
                    logger.error(f"Friends not transformed to list: {type(transformed.friends)}")
                    return False
            
            # Validate born_at transformation
            if hasattr(transformed, 'born_at') and transformed.born_at is not None:
                if not isinstance(transformed.born_at, str):
                    logger.error(f"born_at not transformed to string: {type(transformed.born_at)}")
                    return False
                
                # Check if it's a valid ISO8601 format
                try:
                    datetime.fromisoformat(transformed.born_at.replace('Z', '+00:00'))
                except ValueError:
                    logger.error(f"born_at not in valid ISO8601 format: {transformed.born_at}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating transformation: {e}")
            return False


# class TransformationError(Exception):
#     """Custom exception for transformation errors."""
    
#     def __init__(self, message: str, animal_id: str = None, field: str = None):
#         self.message = message
#         self.animal_id = animal_id
#         self.field = field
#         super().__init__(self.message)
    
#     def __str__(self):
#         parts = [self.message]
#         if self.animal_id:
#             parts.append(f"animal_id={self.animal_id}")
#         if self.field:
#             parts.append(f"field={self.field}")
#         return f"TransformationError({', '.join(parts)})"






