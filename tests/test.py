
import pytest
from datetime import datetime
from dateutil import tz

from application.transformer import AnimalTransformer
from Common.utils import normalize_csv_string, format_datetime_iso8601
from Common.models import Animal


class TestAnimalTransformer:
    """Test cases for AnimalTransformer class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = AnimalTransformer()
    
    def test_transform_friends_csv_string(self):
        """Test transforming friends from CSV string."""
        # Basic CSV string
        result = self.transformer.transform_friends("Alice,Bob,Charlie")
        assert result == ["Alice", "Bob", "Charlie"]
        
        # CSV with whitespace
        result = self.transformer.transform_friends("Alice, Bob , Charlie ")
        assert result == ["Alice", "Bob", "Charlie"]
        
        # Single friend
        result = self.transformer.transform_friends("Alice")
        assert result == ["Alice"]
        
        # Empty string
        result = self.transformer.transform_friends("")
        assert result == []
        
        # String with empty items
        result = self.transformer.transform_friends("Alice,,Bob, ,Charlie")
        assert result == ["Alice", "Bob", "Charlie"]
    
    def test_transform_friends_list(self):
        """Test transforming friends when already a list."""
        # Clean list
        result = self.transformer.transform_friends(["Alice", "Bob", "Charlie"])
        assert result == ["Alice", "Bob", "Charlie"]
        
        # List with whitespace
        result = self.transformer.transform_friends([" Alice ", "Bob", " Charlie "])
        assert result == ["Alice", "Bob", "Charlie"]
        
        # List with empty strings
        result = self.transformer.transform_friends(["Alice", "", "Bob", " ", "Charlie"])
        assert result == ["Alice", "Bob", "Charlie"]
        
        # Empty list
        result = self.transformer.transform_friends([])
        assert result == []
    
    def test_transform_friends_none(self):
        """Test transforming friends when None."""
        result = self.transformer.transform_friends(None)
        assert result == []
    
    def test_transform_born_at_string(self):
        """Test transforming born_at from string."""
        # ISO format
        result = self.transformer.transform_born_at("2020-01-15T10:30:00Z")
        assert result == "2020-01-15T10:30:00Z"
        
        # Date without time
        result = self.transformer.transform_born_at("2020-01-15")
        assert result == "2020-01-15T00:00:00Z"
        
        # With timezone
        result = self.transformer.transform_born_at("2020-01-15T10:30:00-05:00")
        assert result == "2020-01-15T15:30:00Z"  # Converted to UTC
        
        # Different format
        result = self.transformer.transform_born_at("2020-01-15 10:30:00")
        assert result == "2020-01-15T10:30:00Z"
    
    def test_transform_born_at_datetime(self):
        """Test transforming born_at from datetime object."""
        # Naive datetime (assume UTC)
        dt = datetime(2020, 1, 15, 10, 30, 0)
        result = self.transformer.transform_born_at(dt)
        assert result == "2020-01-15T10:30:00Z"
        
        # UTC datetime
        dt = datetime(2020, 1, 15, 10, 30, 0, tzinfo=tz.UTC)
        result = self.transformer.transform_born_at(dt)
        assert result == "2020-01-15T10:30:00Z"
        
        # Timezone-aware datetime
        est = tz.gettz('America/New_York')
        dt = datetime(2020, 1, 15, 10, 30, 0, tzinfo=est)
        result = self.transformer.transform_born_at(dt)
        assert result == "2020-01-15T15:30:00Z"  # Converted to UTC
    
    def test_transform_born_at_none_or_empty(self):
        """Test transforming born_at when None or empty."""
        result = self.transformer.transform_born_at(None)
        assert result is None
        
        result = self.transformer.transform_born_at("")
        assert result is None
        
        result = self.transformer.transform_born_at("   ")
        assert result is None
    
    def test_transform_born_at_invalid_string(self):
        """Test transforming born_at with invalid string."""
        result = self.transformer.transform_born_at("not-a-date")
        assert result is None
        
        result = self.transformer.transform_born_at("2020-13-45")  # Invalid date
        assert result is None
    
    def test_transform_animal_complete(self):
        """Test transforming a complete animal."""
        animal_data = {
            'id': 1,
            'name': 'Fluffy',
            'friends': 'Alice,Bob,Charlie',
            'born_at': '2020-01-15T10:30:00Z'
        }
        
        animal = Animal.from_dict(animal_data)
        transformed = self.transformer.transform(animal)
        
        assert transformed.id == 1
        assert transformed.name == 'Fluffy'
        assert transformed.friends == ['Alice', 'Bob', 'Charlie']
        assert transformed.born_at == '2020-01-15T10:30:00Z'
    
    def test_transform_animal_partial(self):
        """Test transforming animal with only some fields."""
        animal_data = {
            'id': 2,
            'name': 'Rex',
            'friends': 'Alice,Bob'
        }
        
        animal = Animal.from_dict(animal_data)
        transformed = self.transformer.transform(animal)
        
        assert transformed.id == 2
        assert transformed.name == 'Rex'
        assert transformed.friends == ['Alice', 'Bob']
        assert transformed.born_at is None
    
    def test_transform_batch(self):
        """Test transforming a batch of animals."""
        animals_data = [
            {'id': 1, 'name': 'Fluffy', 'friends': 'Alice,Bob'},
            {'id': 2, 'name': 'Rex', 'born_at': '2020-01-15'},
            {'id': 3, 'name': 'Whiskers', 'friends': 'Charlie', 'born_at': '2020-02-10T14:00:00Z'}
        ]
        
        animals = [Animal.from_dict(data) for data in animals_data]
        transformed_animals = self.transformer.transform_batch(animals)
        
        assert len(transformed_animals) == 3
        assert transformed_animals[0].friends == ['Alice', 'Bob']
        assert transformed_animals[1].born_at == '2020-01-15T00:00:00Z'
        assert transformed_animals[2].friends == ['Charlie']
        assert transformed_animals[2].born_at == '2020-02-10T14:00:00Z'
    
    def test_stats_tracking(self):
        """Test that transformation stats are tracked correctly."""
        # Reset stats
        self.transformer.reset_stats()
        initial_stats = self.transformer.get_stats()
        assert initial_stats['friends_transformed'] == 0
        assert initial_stats['born_at_transformed'] == 0
        
        # Transform friends
        self.transformer.transform_friends("Alice,Bob")
        stats = self.transformer.get_stats()
        assert stats['friends_transformed'] == 1
        
        # Transform born_at
        self.transformer.transform_born_at("2020-01-15")
        stats = self.transformer.get_stats()
        assert stats['born_at_transformed'] == 1
    
    def test_validation_success(self):
        """Test successful transformation validation."""
        original_data = {
            'id': 1,
            'name': 'Fluffy',
            'friends': 'Alice,Bob',
            'born_at': '2020-01-15'
        }
        
        original = Animal.from_dict(original_data)
        transformed = self.transformer.transform(original)
        
        is_valid = self.transformer.validate_transformation(original, transformed)
        assert is_valid is True
    
    def test_validation_failure(self):
        """Test transformation validation failure."""
        original = Animal.from_dict({'id': 1, 'name': 'Fluffy'})
        
        # Create invalid transformed data
        transformed_data = original.to_dict()
        transformed_data['id'] = 12  # ID mismatch
        transformed = Animal.from_dict(transformed_data)
        
        is_valid = self.transformer.validate_transformation(original, transformed)
        assert is_valid is False


class TestUtilityFunctions:
    """Test utility functions."""
    
    def test_normalize_csv_string(self):
        """Test CSV string normalization."""
        assert normalize_csv_string("a,b,c") == ["a", "b", "c"]
        assert normalize_csv_string("a, b , c ") == ["a", "b", "c"]
        assert normalize_csv_string("a,,b") == ["a", "b"]
        assert normalize_csv_string("") == []
        assert normalize_csv_string("   ") == []
    
    def test_format_datetime_iso8601(self):
        """Test ISO8601 datetime formatting."""
        # UTC datetime
        dt = datetime(2020, 1, 15, 10, 30, 0, tzinfo=tz.UTC)
        result = format_datetime_iso8601(dt)
        assert result == "2020-01-15T10:30:00Z"
        
        # Naive datetime (assume UTC)
        dt = datetime(2020, 1, 15, 10, 30, 0)
        result = format_datetime_iso8601(dt)
        assert result == "2020-01-15T10:30:00Z"


if __name__ == "__main__":
    pytest.main([__file__])