"""
Database connection and session management for Reg 153 Chemical Matcher.

Provides SQLAlchemy engine, session factory, and connection pooling
with transaction management helpers for SQLite.
"""

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional
from sqlalchemy import create_engine, event, Engine, pool
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

from .models import Base


# Default database path (relative to project root)
DEFAULT_DB_PATH = "data/reg153_matcher.db"


class DatabaseManager:
    """
    Manages database connections and sessions.
    
    Provides engine creation, session management, and connection pooling
    configuration for SQLite with proper transaction handling.
    """
    
    def __init__(
        self,
        db_path: Optional[str] = None,
        echo: bool = False,
        check_same_thread: bool = False,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
    ):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file (absolute or relative to project root)
            echo: If True, SQL statements will be logged
            check_same_thread: SQLite's thread safety check (False for multi-threaded apps)
            pool_size: Number of connections to keep in the pool
            max_overflow: Maximum number of connections to create beyond pool_size
            pool_timeout: Seconds to wait before giving up on getting a connection
        """
        self.db_path = db_path or DEFAULT_DB_PATH
        self.echo = echo
        
        # Ensure database directory exists
        db_file = Path(self.db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Create database URL
        self.database_url = f"sqlite:///{self.db_path}"
        
        # Connection arguments for SQLite
        connect_args = {"check_same_thread": check_same_thread}
        
        # For in-memory databases or testing, use StaticPool to prevent database loss
        if ":memory:" in self.db_path or db_path == "":
            poolclass = StaticPool
            connect_args["check_same_thread"] = False
        else:
            # For file-based databases, use QueuePool for connection pooling
            poolclass = pool.QueuePool
        
        # Create engine
        engine_kwargs = dict(
            echo=self.echo,
            connect_args=connect_args,
            poolclass=poolclass,
        )
        if poolclass != StaticPool:
            engine_kwargs.update(
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_pre_ping=True,
            )
        
        self.engine = create_engine(self.database_url, **engine_kwargs)
        
        # Enable foreign key constraints (disabled by default in SQLite)
        self._configure_sqlite()
        
        # Create session factory
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    
    def _configure_sqlite(self) -> None:
        """Configure SQLite-specific settings."""
        
        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(dbapi_conn, connection_record):
            """Enable foreign keys and set performance optimizations."""
            cursor = dbapi_conn.cursor()
            # Enable foreign key constraints
            cursor.execute("PRAGMA foreign_keys=ON")
            # Performance optimizations
            cursor.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging
            cursor.execute("PRAGMA synchronous=NORMAL")  # Balance safety and speed
            cursor.execute("PRAGMA cache_size=-64000")  # 64MB cache
            cursor.execute("PRAGMA temp_store=MEMORY")  # Keep temp tables in memory
            cursor.close()
    
    def create_all_tables(self) -> None:
        """Create all tables defined in models."""
        Base.metadata.create_all(bind=self.engine)
    
    def drop_all_tables(self) -> None:
        """Drop all tables (use with caution!)."""
        Base.metadata.drop_all(bind=self.engine)
    
    def get_session(self) -> Session:
        """
        Get a new database session.
        
        Returns:
            SQLAlchemy Session instance
            
        Note:
            The caller is responsible for closing the session.
        """
        return self.SessionLocal()
    
    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """
        Provide a transactional scope with automatic commit/rollback.
        
        Usage:
            with db_manager.session_scope() as session:
                session.add(new_analyte)
                # Automatically commits on success, rolls back on exception
        
        Yields:
            SQLAlchemy Session within a transaction context
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def close(self) -> None:
        """Close all connections and dispose of the engine."""
        self.engine.dispose()


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def init_db(
    db_path: Optional[str] = None,
    echo: bool = False,
    check_same_thread: bool = False,
    **kwargs
) -> DatabaseManager:
    """
    Initialize the global database manager.
    
    Args:
        db_path: Path to SQLite database file
        echo: If True, SQL statements will be logged
        check_same_thread: SQLite's thread safety check
        **kwargs: Additional arguments passed to DatabaseManager
    
    Returns:
        DatabaseManager instance
    """
    global _db_manager
    _db_manager = DatabaseManager(
        db_path=db_path,
        echo=echo,
        check_same_thread=check_same_thread,
        **kwargs
    )
    return _db_manager


def get_db_manager() -> DatabaseManager:
    """
    Get the global database manager instance.
    
    Returns:
        DatabaseManager instance
        
    Raises:
        RuntimeError: If database has not been initialized
    """
    if _db_manager is None:
        raise RuntimeError(
            "Database not initialized. Call init_db() first."
        )
    return _db_manager


def get_session() -> Session:
    """
    Get a new database session from the global manager.
    
    Returns:
        SQLAlchemy Session instance
        
    Raises:
        RuntimeError: If database has not been initialized
    """
    return get_db_manager().get_session()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """
    Provide a transactional scope from the global manager.
    
    Usage:
        from src.database.connection import session_scope
        
        with session_scope() as session:
            analyte = session.query(Analyte).filter_by(cas_number="67-64-1").first()
    
    Yields:
        SQLAlchemy Session within a transaction context
    """
    with get_db_manager().session_scope() as session:
        yield session


def create_test_db() -> DatabaseManager:
    """
    Create an in-memory database for testing.
    
    Returns:
        DatabaseManager instance with in-memory database
    """
    db = DatabaseManager(
        db_path=":memory:",
        echo=False,
        check_same_thread=False,
    )
    db.create_all_tables()
    return db


# Transaction management helpers

def execute_in_transaction(func):
    """
    Decorator to execute a function within a database transaction.
    
    Usage:
        @execute_in_transaction
        def add_analyte(session: Session, cas: str, name: str):
            analyte = Analyte(cas_number=cas, preferred_name=name)
            session.add(analyte)
            return analyte
    """
    def wrapper(*args, **kwargs):
        with session_scope() as session:
            # Inject session as first argument if not provided
            if 'session' not in kwargs and (not args or not isinstance(args[0], Session)):
                return func(session, *args, **kwargs)
            return func(*args, **kwargs)
    return wrapper


def bulk_insert_in_chunks(
    session: Session,
    model_class,
    records: list[dict],
    chunk_size: int = 1000
) -> int:
    """
    Insert records in chunks for better performance.
    
    Args:
        session: SQLAlchemy session
        model_class: SQLAlchemy model class
        records: List of dictionaries with model attributes
        chunk_size: Number of records per chunk
    
    Returns:
        Total number of records inserted
    """
    total_inserted = 0
    
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        session.bulk_insert_mappings(model_class, chunk)
        session.flush()  # Flush each chunk
        total_inserted += len(chunk)
    
    return total_inserted


if __name__ == "__main__":
    # Example usage
    from .models import Analyte, AnalyteType
    
    db = DatabaseManager("test.db", echo=True)
    db.create_all_tables()
    
    with db.session_scope() as session:
        # Create a test analyte
        analyte = Analyte(
            analyte_id="REG153_TEST_001",
            preferred_name="Test Benzene",
            analyte_type=AnalyteType.SINGLE_SUBSTANCE,
            cas_number="71-43-2",
            chemical_group="BTEX",
            molecular_formula="C6H6"
        )
        session.add(analyte)
    
    print("Database initialized successfully!")
    db.close()
