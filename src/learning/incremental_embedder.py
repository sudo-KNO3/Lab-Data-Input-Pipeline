"""
Incremental embedding updates for Layer 2 learning.

Enables adding new terms to the FAISS index without full rebuild,
supporting continuous vocabulary expansion with semantic matching.
"""

import logging
from pathlib import Path
from typing import Optional
import numpy as np
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import select

try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False
    logging.warning("FAISS not available. Incremental embedding updates disabled.")

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    logging.warning("sentence-transformers not available. Embedding generation disabled.")

from ..database.models import EmbeddingsMetadata

logger = logging.getLogger(__name__)


class IncrementalEmbedder:
    """
    Manages incremental updates to the FAISS embedding index.
    
    This enables Layer 2 learning: adding new terms to the semantic
    matching index without requiring a full rebuild.
    """
    
    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        faiss_index_path: Optional[Path] = None,
        vectors_path: Optional[Path] = None,
        metadata_path: Optional[Path] = None,
        save_frequency: int = 100
    ):
        """
        Initialize the incremental embedder.
        
        Args:
            model_name: Name of the sentence-transformers model
            faiss_index_path: Path to FAISS index file
            vectors_path: Path to numpy vectors file
            metadata_path: Path to metadata file
            save_frequency: Save index after this many additions
        
        Raises:
            ImportError: If required libraries are not available
        """
        if not FAISS_AVAILABLE:
            raise ImportError("FAISS is required for incremental embedding. Install with: pip install faiss-cpu")
        
        if not SENTENCE_TRANSFORMERS_AVAILABLE:
            raise ImportError("sentence-transformers is required. Install with: pip install sentence-transformers")
        
        self.model_name = model_name
        self.model = SentenceTransformer(model_name)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        
        self.faiss_index_path = faiss_index_path
        self.vectors_path = vectors_path
        self.metadata_path = metadata_path
        self.save_frequency = save_frequency
        
        self.faiss_index: Optional[faiss.Index] = None
        self.vectors: Optional[np.ndarray] = None
        self.metadata_list: list[dict] = []
        
        self.additions_since_save = 0
        
        logger.info(f"IncrementalEmbedder initialized with model: {model_name}")
    
    def load_existing_index(self) -> bool:
        """
        Load existing FAISS index and metadata.
        
        Returns:
            True if successfully loaded, False if no index exists
        """
        if self.faiss_index_path and self.faiss_index_path.exists():
            try:
                self.faiss_index = faiss.read_index(str(self.faiss_index_path))
                logger.info(f"Loaded FAISS index from {self.faiss_index_path}")
                
                # Load vectors if available
                if self.vectors_path and self.vectors_path.exists():
                    self.vectors = np.load(str(self.vectors_path))
                    logger.info(f"Loaded vectors from {self.vectors_path}")
                
                return True
            except Exception as e:
                logger.error(f"Failed to load existing index: {e}")
                return False
        else:
            logger.info("No existing index found, will create new one")
            self._initialize_new_index()
            return False
    
    def _initialize_new_index(self) -> None:
        """Initialize a new FAISS index."""
        # Use IndexFlatIP (inner product) for cosine similarity search
        # Must match SemanticMatcher's index type
        self.faiss_index = faiss.IndexFlatIP(self.embedding_dim)
        self.vectors = np.zeros((0, self.embedding_dim), dtype=np.float32)
        logger.info(f"Initialized new FAISS index with dimension {self.embedding_dim}")
    
    def add_term(
        self,
        text: str,
        analyte_id: str,
        db_session: Session,
        synonym_id: Optional[int] = None
    ) -> None:
        """
        Add a new term to the embedding index.
        
        Args:
            text: Text to embed
            analyte_id: Associated analyte ID
            db_session: Database session
            synonym_id: Optional synonym ID for tracking
        
        Raises:
            ValueError: If index not loaded
        """
        if self.faiss_index is None:
            raise ValueError("Index not loaded. Call load_existing_index() first.")
        
        try:
            # Generate embedding
            embedding = self.model.encode([text], convert_to_numpy=True)[0]
            embedding = embedding.astype(np.float32).reshape(1, -1)
            
            # Add to FAISS index
            self.faiss_index.add(embedding)
            
            # Update vectors array
            if self.vectors is None:
                self.vectors = embedding
            else:
                self.vectors = np.vstack([self.vectors, embedding])
            
            # Get the index of the newly added vector
            embedding_index = self.faiss_index.ntotal - 1
            
            # Add metadata to database (generate a simple hash for model_hash)
            import hashlib
            model_hash = hashlib.md5(self.model_name.encode()).hexdigest()[:16]
            
            metadata = EmbeddingsMetadata(
                analyte_id=analyte_id,
                synonym_id=synonym_id,
                text_content=text,
                embedding_index=embedding_index,
                model_name=self.model_name,
                model_hash=model_hash
            )
            db_session.add(metadata)
            db_session.commit()
            self.additions_since_save += 1
            
            logger.debug(f"Added term to index: '{text}' (index={embedding_index})")
            
            # Auto-save if reached frequency threshold
            if self.additions_since_save >= self.save_frequency:
                self.save_incremental_update()
            
        except Exception as e:
            logger.error(f"Failed to add term '{text}': {e}")
            db_session.rollback()
            raise
    
    def save_incremental_update(self) -> None:
        """
        Save the updated FAISS index and vectors to disk.
        
        This is called automatically after save_frequency additions,
        but can also be called manually.
        """
        if self.faiss_index is None:
            logger.warning("No index to save")
            return
        
        try:
            # Save FAISS index
            if self.faiss_index_path:
                self.faiss_index_path.parent.mkdir(parents=True, exist_ok=True)
                faiss.write_index(self.faiss_index, str(self.faiss_index_path))
                logger.info(f"Saved FAISS index to {self.faiss_index_path}")
            
            # Save vectors
            if self.vectors_path and self.vectors is not None:
                self.vectors_path.parent.mkdir(parents=True, exist_ok=True)
                np.save(str(self.vectors_path), self.vectors)
                logger.info(f"Saved vectors to {self.vectors_path}")
            
            self.additions_since_save = 0
            logger.info(f"Incremental update saved successfully (total vectors: {self.faiss_index.ntotal})")
            
        except Exception as e:
            logger.error(f"Failed to save incremental update: {e}")
            raise
    
    def get_index_stats(self) -> dict:
        """
        Get statistics about the current index.
        
        Returns:
            Dictionary with index statistics
        """
        if self.faiss_index is None:
            return {'loaded': False}
        
        return {
            'loaded': True,
            'total_vectors': self.faiss_index.ntotal,
            'embedding_dim': self.embedding_dim,
            'model_name': self.model_name,
            'additions_since_save': self.additions_since_save,
            'save_frequency': self.save_frequency
        }
    
    def bulk_add_terms(
        self,
        terms: list[tuple[str, str]],
        db_session: Session
    ) -> dict[str, int]:
        """
        Add multiple terms in bulk.
        
        Args:
            terms: List of (text, analyte_id) tuples
            db_session: Database session
        
        Returns:
            Statistics dictionary
        """
        stats = {'added': 0, 'errors': 0}
        
        logger.info(f"Starting bulk addition of {len(terms)} terms")
        
        for text, analyte_id in terms:
            try:
                self.add_term(text, analyte_id, db_session)
                stats['added'] += 1
            except Exception as e:
                logger.error(f"Error adding term '{text}': {e}")
                stats['errors'] += 1
        
        # Final save
        if self.additions_since_save > 0:
            self.save_incremental_update()
        
        logger.info(f"Bulk addition complete: {stats['added']} added, {stats['errors']} errors")
        
        return stats
