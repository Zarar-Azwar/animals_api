import logging
from logging.handlers import RotatingFileHandler

class CustomLogger:
    def __init__(self, name: str="animal_apis", log_file: str = "app.log", level: int = logging.INFO):
        """
        Initialize a custom logger.
        
        Args:
            name (str): Logger name
            log_file (str): Path to log file
            level (int): Logging level
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Avoid duplicate handlers if logger is created multiple times
        if not self.logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)

            # File handler with rotation
            file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5)
            file_handler.setLevel(level)

            # Formatter
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)

            # Add handlers
            self.logger.addHandler(console_handler)
            self.logger.addHandler(file_handler)

    def get_logger(self):
        """Return the configured logger."""
        return self.logger


# ---------------- Example Usage ----------------
if __name__ == "__main__":
    logger = CustomLogger("MyAppLogger", "myapp.log", logging.DEBUG).get_logger()
    
    logger.debug("This is a debug message.")
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")