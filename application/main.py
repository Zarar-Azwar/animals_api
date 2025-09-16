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
    
    def __init__(self, base_url: str = "http://localhost:3123", batch_size: int = 100, concurrency: int = 5):
        """
        Initialize the AnimalETLPipeline.
        
        Args:
            base_url (str): Animals API base URL.
            batch_size (int): Number of animals to process in each batch.
            concurrency (int): Number of concurrent ETL workers.
        """
        self.logger = CustomLogger().get_logger()
        self.api_client = AnimalAPIClient(base_url)
        self.transformer = AnimalTransformer()
        self.batch_size = batch_size
        self.concurrency = concurrency
        self.stats_lock = asyncio.Lock()

    async def run_pipeline(self) -> Dict[str, Any]:
        """
        Execute the ETL pipeline by processing data in concurrent batches.

        This method implements a concurrent producer-consumer pattern:
        1. **Producer**: Fetches pages of animal summaries and puts their IDs into a queue.
        2. **Consumers (Workers)**: Concurrently pull batches of IDs from the queue
           and perform the full ETL process (fetch details, transform, load) for each batch.

        Returns:
            Dict[str, Any]: A dictionary with statistics about the pipeline execution.
        """
        self.logger.info(f"Starting Animal ETL Pipeline with {self.concurrency} concurrent workers")
        start_time = datetime.now()
        stats = {
            'start_time': start_time.isoformat(),
            'animals_fetched': 0,
            'animals_transformed': 0,
            'animals_loaded': 0,
            'batches_processed': 0,
            'errors': []
        }

        queue = asyncio.Queue(maxsize=self.concurrency * 2)

        # Start the producer and consumer tasks
        producer = asyncio.create_task(self._produce_animal_ids(queue))
        consumers = [
            asyncio.create_task(self._consume_and_process(queue, stats))
            for _ in range(self.concurrency)
        ]

        # Wait for the producer to finish
        await producer

        # Wait for the consumers to finish processing all items in the queue
        await queue.join()

        # Cancel the consumer tasks
        for consumer in consumers:
            consumer.cancel()
        
        # Wait for consumer cancellation to complete
        await asyncio.gather(*consumers, return_exceptions=True)

        end_time = datetime.now()
        duration = end_time - start_time
        stats['end_time'] = end_time.isoformat()
        stats['duration_seconds'] = duration.total_seconds()
        await self.api_client.close()

        self.logger.info(f"Pipeline completed in {duration.total_seconds():.2f} seconds")
        self.logger.info(f"Final stats: {json.dumps(stats, indent=2)}")

        return stats

    async def _produce_animal_ids(self, queue: asyncio.Queue):
        """
        Producer: Fetches animal IDs page by page and puts them into the queue.
        """
        page = 1
        while True:
            try:
                self.logger.info(f"Fetching animal list page {page}...")
                response = await self.api_client.list_animals(page=page, per_page=self.batch_size)
                animal_summaries = response.get('items', [])

                if not animal_summaries:
                    self.logger.info("Producer: No more animals to process.")
                    break

                animal_ids = [animal['id'] for animal in animal_summaries]
                await queue.put(animal_ids)
                self.logger.info(f"Producer: Queued batch of {len(animal_ids)} animals from page {page}.")
                page += 1
            except Exception as e:
                self.logger.error(f"Producer failed on page {page}: {e}")
                break

    async def _consume_and_process(self, queue: asyncio.Queue, stats: Dict[str, Any]):
        """
        Consumer: Continuously takes batches of animal IDs from the queue and processes them.
        """
        while True:
            try:
                animal_ids = await queue.get()
                batch_num = stats['batches_processed'] + 1
                self.logger.info(f"Worker starting to process batch {batch_num}...")
                
                await self._process_batch(animal_ids, stats)
                
                queue.task_done()
                self.logger.info(f"Worker finished processing batch {batch_num}.")
            except asyncio.CancelledError:
                self.logger.info("Worker task was cancelled.")
                break
            except Exception as e:
                self.logger.error(f"Consumer worker failed: {e}")
                async with self.stats_lock:
                    stats['errors'].append(str(e))
                queue.task_done()


    async def _process_batch(self, animal_ids: List[str], stats: Dict[str, Any]):
        """
        Processes a single batch of animals: fetch details, transform, and load.
        """
        try:
            # Step 1: Extract details for the current batch
            detailed_animals = await self._fetch_animal_details(animal_ids)
            
            # Step 2: Transform the current batch
            transformed_animals = self._transform_animals(detailed_animals)
            
            # Step 3: Load the current batch
            if transformed_animals:
                await self._load_batch(transformed_animals)
            
            # Update stats safely
            async with self.stats_lock:
                stats['animals_fetched'] += len(detailed_animals)
                stats['animals_transformed'] += len(transformed_animals)
                stats['animals_loaded'] += len(transformed_animals)
                stats['batches_processed'] += 1

        except Exception as e:
            self.logger.error(f"Failed to process a batch of {len(animal_ids)} animals: {e}")
            async with self.stats_lock:
                stats['errors'].append(f"Batch processing failed: {e}")

    async def _fetch_animal_details(self, animal_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch detailed information for a batch of animals given their IDs.
        """
        animals = []
        semaphore = asyncio.Semaphore(10) # Limit concurrency for detail fetching within a batch

        async def fetch_single_animal(animal_id: str) -> Optional[Dict[str, Any]]:
            async with semaphore:
                try:
                    return await self.api_client.get_animal_details(animal_id)
                except Exception as e:
                    self.logger.error(f"Failed to fetch details for animal {animal_id}: {e}")
                    return None
        
        tasks = [fetch_single_animal(animal_id) for animal_id in animal_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, dict):
                animals.append(result)
            elif result is not None:
                self.logger.error(f"Exception in fetch task: {result}")
                
        return animals
    
    def _transform_animals(self, animals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform raw animal data into the required output format.
        """
        transformed = []
        for animal_data in animals:
            try:
                animal = Animal.from_dict(animal_data)
                transformed_animal = self.transformer.transform(animal)
                transformed.append(transformed_animal.to_dict())
            except Exception as e:
                self.logger.error(f"Error transforming animal {animal_data.get('id', 'unknown')}: {e}")
                continue
        return transformed
    
    async def _load_batch(self, animals: List[Dict[str, Any]]) -> None:
        """
        Load a single batch of transformed animal data to the home endpoint.
        """
        try:
            await self.api_client.load_animals_home(animals)
        except Exception as e:
            self.logger.error(f"Failed to load a batch of {len(animals)} animals: {e}")
            raise


async def main():
    """Main entry point."""
    try:
        # Create and run the pipeline
        pipeline = AnimalETLPipeline(
            env_config.get("BASE_URL", "http://localhost:3123"), 
            int(env_config.get("BATCH_SIZE", 100)),
            int(env_config.get("CONCURRENCY", 5))
        )
        stats = await pipeline.run_pipeline()
        
        pipeline.logger.info("\n" + "="*50)
        pipeline.logger.info("PIPELINE EXECUTION SUMMARY")
        pipeline.logger.info("="*50)
        pipeline.logger.info(f"Animals fetched: {stats['animals_fetched']}")
        pipeline.logger.info(f"Animals transformed: {stats['animals_transformed']}")
        pipeline.logger.info(f"Animals loaded: {stats['animals_loaded']}")
        pipeline.logger.info(f"Batches processed: {stats['batches_processed']}")
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
