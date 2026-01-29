"""Base repository for TinyDB operations."""

import os
from typing import Any

from tinydb import Query, TinyDB


class BaseRepository:
    """Base class for data repositories using TinyDB."""

    def __init__(self, db_path: str, table_name: str = "typhoons"):
        """Initialize the repository with TinyDB connection.

        Args:
            db_path: Path to the TinyDB JSON file
            table_name: Name of the table to use (default: 'typhoons')
        """
        # Ensure database directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Initialize TinyDB
        self.db = TinyDB(db_path)
        self.table = self.db.table(table_name)
        self.db_path = db_path

    def get_all(self) -> list[dict[str, Any]]:
        """Get all records from the table."""
        return self.table.all()

    def get_by_id(self, doc_id: int) -> dict[str, Any] | None:
        """Get a record by document ID."""
        return self.table.get(doc_id=doc_id)

    def get_by_field(self, field: str, value: Any) -> dict[str, Any] | None:
        """Get a single record by field value.

        Args:
            field: Field name to query
            value: Value to match

        Returns:
            First matching record or None
        """
        Q = Query()
        return self.table.get(Q[field] == value)

    def get_all_by_field(self, field: str, value: Any) -> list[dict[str, Any]]:
        """Get all records matching a field value.

        Args:
            field: Field name to query
            value: Value to match

        Returns:
            List of matching records
        """
        Q = Query()
        return self.table.search(Q[field] == value)

    def insert(self, data: dict[str, Any]) -> int:
        """Insert a new record.

        Args:
            data: Record data to insert

        Returns:
            Document ID of inserted record
        """
        return self.table.insert(data)

    def update(self, doc_id: int, data: dict[str, Any]) -> list[int]:
        """Update a record by document ID.

        Args:
            doc_id: Document ID to update
            data: New data

        Returns:
            List of updated document IDs
        """
        return self.table.update(data, doc_ids=[doc_id])

    def delete_by_id(self, doc_id: int) -> list[int]:
        """Delete a record by document ID.

        Args:
            doc_id: Document ID to delete

        Returns:
            List of deleted document IDs
        """
        return self.table.remove(doc_ids=[doc_id])

    def delete_by_field(self, field: str, value: Any) -> list[int]:
        """Delete records by field value.

        Args:
            field: Field name to query
            value: Value to match

        Returns:
            List of deleted document IDs
        """
        Q = Query()
        return self.table.remove(Q[field] == value)

    def truncate(self):
        """Clear all records from the table."""
        self.table.truncate()

    def close(self):
        """Close the database connection."""
        self.db.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
