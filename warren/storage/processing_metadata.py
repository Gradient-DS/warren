from typing import Protocol, Dict


class ProcessingMetadatStoreInterface(Protocol):
    """
    Storing processing metadata for a specific worker.
    """

    def store(self, worker_type: str, job_id: str, processing_metadata: Dict):
        """
        Store processing metadata for a specific worker.
        """
        ...

    def exists(
        self,
        worker_type: str,
        job_id: str,
    ) -> bool:
        """
        Check if processing metadata exists for a specific worker.
        """
        ...

    def get(
        self,
        worker_type: str,
        job_id: str,
    ) -> Dict:
        """
        Get processing metadata for a specific worker.

        :raises ProcessingMetadataNotFound: If processing metadata does not exist.
        """
        ...
