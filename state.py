import os
from loguru import logger
from typing import Optional

class StateManager:
    """Manages the persistence of the last processed offset (e.g., timestamp or ID)."""

    def __init__(self, offset_file: str):
        self.offset_file = offset_file

    def get_last_offset(self) -> Optional[str]:
        """Reads the last processed offset from a file."""
        if not os.path.exists(self.offset_file):
            logger.info(f"Offset file {self.offset_file} not found. Starting from scratch.")
            return None
        
        try:
            with open(self.offset_file, "r") as f:
                offset = f.read().strip()
                if not offset:
                    return None
                logger.debug(f"Last processed offset retrieved: {offset}")
                return offset
        except Exception as e:
            logger.error(f"Error reading offset file: {e}")
            return None

    def save_last_offset(self, offset: str):
        """Saves the last processed offset to a file."""
        try:
            with open(self.offset_file, "w") as f:
                f.write(str(offset))
            logger.debug(f"Saved last processed offset: {offset}")
        except Exception as e:
            logger.error(f"Error saving offset file: {e}")
