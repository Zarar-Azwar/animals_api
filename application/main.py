import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

from application.api_client import AnimalAPIClient
from application.transformer import AnimalTransformer
from Common.models import Animal
from Common.logger import CustomLogger
from Common.configs import EnvConfig

env_config = EnvConfig()


class AnimalETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self, base_url: str = "http://localhost:3123", batch_size: int = 100):
        """
        Initialize the AnimalETLPipeline.
        
        Args:
            base_url (str): Animals API base URL (default: http://localhost:3123)
            batch_size (int): Batch size for pipeline processing (default: 100)
        """
        self.logger = CustomLogger().get_logger()
        self.api_client = AnimalAPIClient(base_url)
        self.transformer = AnimalTransformer()
        self.batch_size = batch_size
        
    async def run_pipeline(self) -> Dict[str, Any]:
        """
        Execute the complete ETL (Extract, Transform, Load) pipeline 
        for processing animal data.

        The pipeline consists of four major steps:
        1. **Extract (IDs)** – Fetch all available animal IDs.
        2. **Extract (Details)** – Retrieve detailed animal information for the fetched IDs.
        3. **Transform** – Apply transformations and normalization to the animal data.
        4. **Load** – Send transformed data to the target system in batches.

        Throughout the process, execution statistics are recorded, 
        including counts of fetched, transformed, and loaded animals,
        as well as the number of batches processed.

        Returns:
            Dict[str, Any]: Dictionary containing pipeline execution statistics:
                - ``start_time`` (str): ISO 8601 timestamp when the pipeline started.
                - ``end_time`` (str): ISO 8601 timestamp when the pipeline ended.
                - ``duration_seconds`` (float): Total execution time in seconds.
                - ``animals_fetched`` (int): Number of animals successfully fetched.
                - ``animals_transformed`` (int): Number of animals transformed.
                - ``animals_loaded`` (int): Number of animals loaded into the system.
                - ``batches_loaded`` (int): Number of batches successfully processed.
                - ``errors`` (List[str]): List of error messages encountered (empty if successful).

        Raises:
            Exception: If any step in the pipeline fails, 
            the exception is logged, added to the stats, and re-raised.
        """
        self.logger.info("Starting Animal ETL Pipeline")
        start_time = datetime.now()
        stats = {
            'start_time': start_time.isoformat(),
            'animals_fetched': 0,
            'animals_transformed': 0,
            'animals_loaded': 0,
            'batches_loaded': 0,
            'errors': []
        }
        
        try:
            # Step 1: Extract - Fetch all animal IDs
            self.logger.info("Step 1: Fetching animal list...")
            animal_ids = await self._fetch_all_animal_ids()
            self.logger.info(f"Found {len(animal_ids)} animals to process")
            
            # Step 2: Extract - Fetch detailed animal data
            self.logger.info("Step 2: Fetching animal details...")
            animals = await self._fetch_all_animal_details(animal_ids)
            
            stats['animals_fetched'] = len(animals)
            self.logger.info(f"Successfully fetched details for {len(animals)} animals")
            
            # Step 3: Transform - Process the animal data
            self.logger.info("Step 3: Transforming animal data...")
            transformed_animals = self._transform_animals(animals)
            stats['animals_transformed'] = len(transformed_animals)
            self.logger.info(f"Transformed {len(transformed_animals)} animals")
            
            # Step 4: Load - Send data in batches
            self.logger.info("Step 4: Loading animal data...")
            loaded_count, batch_count = await self._load_animals_in_batches(transformed_animals)
            stats['animals_loaded'] = loaded_count
            stats['batches_loaded'] = batch_count
            
            end_time = datetime.now()
            duration = end_time - start_time
            stats['end_time'] = end_time.isoformat()
            stats['duration_seconds'] = duration.total_seconds()
            await self.api_client.close()
            
            self.logger.info(f"Pipeline completed successfully in {duration.total_seconds():.2f} seconds")
            self.logger.info(f"Final stats: {json.dumps(stats, indent=2)}")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Pipeline failed with error: {e}")
            stats['errors'].append(str(e))
            stats['end_time'] = datetime.now().isoformat()
            raise
            
    async def _fetch_all_animal_ids(self) -> List[str]:
        """
        Fetch all animal IDs from the API using pagination.

        This method iteratively requests pages of animal data from the API
        until no more items are returned. Each page is processed to extract
        the animal IDs, which are aggregated into a single list.

        Returns:
            List[str]: A list of all animal IDs retrieved from the API.

        Raises:
            Exception: If any API request fails or the response format
            does not contain the expected structure, the exception is
            logged and re-raised.
        """
        all_ids = []
        page = 1
        
        while True:
            try:
                response = await self.api_client.list_animals(page=page)
                animals = response.get('items', [])
                
                if not animals:
                    break
                    
                ids = [animal['id'] for animal in animals]
                all_ids.extend(ids)
                self.logger.info(f"Page {page}: Found {len(ids)} animals")
                    
                page += 1
                
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {e}")
                raise
                
        return all_ids
    
    async def _fetch_all_animal_details(self, animal_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch detailed information for all animals given their IDs.

        This method uses concurrency with a semaphore to limit the number
        of simultaneous API requests. Each animal ID is processed by
        requesting detailed data from the API client. Failed requests are
        logged and excluded from the results.

        Args:
            animal_ids (List[str]): List of animal IDs to fetch details for.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing detailed
            animal data. Only successfully retrieved animals are included.

        Notes:
            - Concurrency is limited to 10 requests at a time to avoid 
            overwhelming the API.
            - If a request fails, it is logged and excluded from the result set.
            - Exceptions raised during `asyncio.gather` are caught and logged.

        Raises:
            Exception: If there are unexpected failures during the fetching 
            process that cannot be handled within the tasks.
        """
        animals = []
        
        # Use semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(10)
        
        async def fetch_single_animal(animal_id: str) -> Optional[Dict[str, Any]]:
            async with semaphore:
                try:
                    animal = await self.api_client.get_animal_details(animal_id)
                    return animal
                except Exception as e:
                    self.logger.error(f"Failed to fetch details for animal {animal_id}: {e}")
                    return None
        
        # Fetch all animal details concurrently
        tasks = [fetch_single_animal(animal_id) for animal_id in animal_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out failed requests and exceptions
        for result in results:
            if isinstance(result, dict) and result is not None:
                animals.append(result)
            elif isinstance(result, Exception):
                self.logger.error(f"Exception in fetch task: {result}")
                
        return animals
    
    def _transform_animals(self, animals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform raw animal data into the required output format.

        For each animal record:
        1. Validate and normalize the data using the `Animal` Pydantic model.
        2. Apply domain-specific transformations via the configured transformer.
        3. Convert the transformed object back into a dictionary for API usage.

        Args:
            animals (List[Dict[str, Any]]): List of raw animal data dictionaries
            fetched from the API.

        Returns:
            List[Dict[str, Any]]: List of transformed animal dictionaries ready
            for loading into the target system.

        Notes:
            - Animals that fail validation or transformation are logged and skipped.
            - Errors during processing of a single animal do not halt the pipeline.
        """
        transformed = []
        
        for animal_data in animals:
            try:
                # Create Animal model instance
                animal = Animal.from_dict(animal_data)
                
                # Transform the data
                transformed_animal = self.transformer.transform(animal)
                
                # Convert back to dict for API
                transformed.append(transformed_animal.to_dict())
                
            except Exception as e:
                self.logger.error(f"Error transforming animal {animal_data.get('id', 'unknown')}: {e}")
                # Continue processing other animals
                continue
                
        return transformed
    
    async def _load_animals_in_batches(self, animals: List[Dict[str, Any]]) -> tuple[int, int]:
        """
        Load transformed animal data into the home endpoint in batches.

        Animals are divided into chunks of size ``self.batch_size`` and sent
        sequentially to the API. Each batch is logged before and after loading.
        Failures in individual batches are logged and skipped, allowing the 
        process to continue with subsequent batches.

        Args:
            animals (List[Dict[str, Any]]): List of transformed animal data
            dictionaries ready for loading.

        Returns:
            tuple[int, int]: A tuple containing:
                - ``total_loaded`` (int): Total number of animals successfully loaded.
                - ``batch_count`` (int): Number of batches successfully processed.

        Notes:
            - Batch size is controlled by ``self.batch_size``.
            - Errors in a single batch do not stop the pipeline; 
            subsequent batches will still be attempted.
        """
        total_loaded = 0
        batch_count = 0
        
        # Process animals in batches
        for i in range(0, len(animals), self.batch_size):
            batch = animals[i:i + self.batch_size]
            batch_num = batch_count + 1
            
            try:
                self.logger.info(f"Loading batch {batch_num} with {len(batch)} animals")
                await self.api_client.load_animals_home(batch)
                total_loaded += len(batch)
                batch_count += 1
                self.logger.info(f"Successfully loaded batch {batch_num}")
                
            except Exception as e:
                self.logger.error(f"Failed to load batch {batch_num}: {e}")
                # Continue with next batch rather than failing completely
                continue
                
        return total_loaded, batch_count


async def main():
    """Main entry point."""
    try:
        # Create and run the pipeline
        pipeline = AnimalETLPipeline(env_config.get("BASE_URL","http://localhost:3123"), int(env_config.get("BATCH_SIZE", 100)))
        stats = await pipeline.run_pipeline()
        
        pipeline.logger.info("\n" + "="*50)
        pipeline.logger.info("PIPELINE EXECUTION SUMMARY")
        pipeline.logger.info("="*50)
        pipeline.logger.info(f"Animals fetched: {stats['animals_fetched']}")
        pipeline.logger.info(f"Animals transformed: {stats['animals_transformed']}")
        pipeline.logger.info(f"Animals loaded: {stats['animals_loaded']}")
        pipeline.logger.info(f"Batches loaded: {stats['batches_loaded']}")
        pipeline.logger.info(f"Duration: {stats['duration_seconds']:.2f} seconds")

        if stats['errors']:
            pipeline.logger.error(f"Errors encountered: {len(stats['errors'])}")

        pipeline.logger.info("="*50)

        
    except Exception as e:
        pipeline.logger.error(f"Pipeline execution failed: {e}")
        return 1
        
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)