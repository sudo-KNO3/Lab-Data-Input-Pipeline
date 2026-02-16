"""
Semantic matching using sentence transformers and FAISS.

Provides vector similarity search for chemical names using
pre-trained sentence embeddings and efficient FAISS indexing.
"""

import os
import json
import logging
import threading
from typing import List, Optional, Dict, Any
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

from .types import Match, MatchMethod, EmbeddingConfig

logger = logging.getLogger(__name__)


class SemanticMatcher:
    """
    FAISS-based semantic matcher for chemical names.
    
    Loads a sentence-transformers model and FAISS index for fast
    similarity search across all synonyms.
    
    Thread-safe for reads, uses lock for incremental additions.
    """
    
    def __init__(
        self,
        config: Optional[EmbeddingConfig] = None,
        base_path: str = "."
    ):
        """
        Initialize semantic matcher.
        
        Args:
            config: Embedding configuration (uses defaults if None)
            base_path: Base directory for resolving relative paths
        """
        self.config = config or EmbeddingConfig()
        self.base_path = base_path
        
        # Model and index (loaded lazily)
        self.model: Optional[SentenceTransformer] = None
        self.index: Optional[faiss.IndexFlatIP] = None  # Inner product (cosine after normalization)
        self.metadata: Dict[int, Dict[str, Any]] = {}  # Maps FAISS index -> synonym metadata
        
        # Thread safety for incremental additions
        self._index_lock = threading.Lock()
        
        # Load on initialization
        self._load_model()
        self._load_index()
    
    def _load_model(self):
        """Load sentence transformer model."""
        try:
            logger.info(f"Loading sentence transformer model: {self.config.model_name}")
            self.model = SentenceTransformer(self.config.model_name)
            logger.info(f"Model loaded successfully (dim={self.config.embedding_dim})")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def _load_index(self):
        """Load FAISS index and metadata from disk."""
        index_path = os.path.join(self.base_path, self.config.faiss_index_path)
        metadata_path = os.path.join(self.base_path, self.config.metadata_path)
        
        if not os.path.exists(index_path):
            logger.warning(f"FAISS index not found at {index_path}. Creating empty index.")
            self.index = faiss.IndexFlatIP(self.config.embedding_dim)
            return
        
        try:
            # Load FAISS index
            logger.info(f"Loading FAISS index from {index_path}")
            self.index = faiss.read_index(index_path)
            logger.info(f"FAISS index loaded: {self.index.ntotal} vectors")
            
            # Load metadata
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    metadata_list = json.load(f)
                    # Convert list to dict (index -> metadata)
                    self.metadata = {item['faiss_index']: item for item in metadata_list}
                logger.info(f"Loaded metadata for {len(self.metadata)} synonyms")
            else:
                logger.warning(f"Metadata file not found at {metadata_path}")
                
        except Exception as e:
            logger.error(f"Failed to load FAISS index: {e}")
            raise
    
    def encode_query(self, text: str) -> np.ndarray:
        """
        Encode query text to embedding vector.
        
        Args:
            text: Query text (already normalized)
            
        Returns:
            Normalized embedding vector (L2 norm = 1)
        """
        if not self.model:
            raise RuntimeError("Model not loaded")
        
        # Encode
        embedding = self.model.encode([text], convert_to_numpy=True)[0]
        
        # L2 normalize for cosine similarity via inner product
        if self.config.normalize_l2:
            norm = np.linalg.norm(embedding)
            if norm > 0:
                embedding = embedding / norm
        
        return embedding.astype('float32')
    
    def search(
        self,
        query_embedding: np.ndarray,
        top_k: int = 5,
        threshold: float = 0.75
    ) -> List[Match]:
        """
        Search FAISS index for similar vectors.
        
        Args:
            query_embedding: Query vector (should be L2 normalized)
            top_k: Number of results to return
            threshold: Minimum cosine similarity
            
        Returns:
            List of Match objects ordered by similarity
        """
        if not self.index or self.index.ntotal == 0:
            logger.warning("FAISS index is empty")
            return []
        
        # Ensure query is 2D array
        if query_embedding.ndim == 1:
            query_embedding = query_embedding.reshape(1, -1)
        
        # Search
        distances, indices = self.index.search(query_embedding, top_k)
        
        # Convert to matches
        matches = []
        for dist, idx in zip(distances[0], indices[0]):
            # Skip invalid indices
            if idx < 0 or idx >= len(self.metadata):
                continue
            
            # Cosine similarity (distance is inner product after L2 norm)
            similarity = float(dist)
            
            # Apply threshold
            if similarity < threshold:
                continue
            
            # Get metadata
            meta = self.metadata.get(idx, {})
            
            # Raw score passthrough — preserves distance geometry
            # and margin integrity for downstream gating decisions.
            # Step-function binning was removed to maintain
            # discrimination needed for margin-based acceptance.
            # Clamp to [0, 1] to guard against FP imprecision.
            confidence = max(0.0, min(1.0, similarity))
            
            match = Match(
                analyte_id=meta.get('analyte_id', -1),
                analyte_name=meta.get('analyte_name', 'Unknown'),
                cas_number=meta.get('cas_number'),
                confidence=confidence,
                method=MatchMethod.SEMANTIC,
                synonym_matched=meta.get('synonym_norm'),
                synonym_id=meta.get('synonym_id'),
                similarity_score=similarity,
                metadata={
                    "cosine_similarity": similarity,
                    "faiss_index": int(idx),
                    "synonym_raw": meta.get('synonym_raw'),
                    "synonym_source": meta.get('harvest_source'),
                }
            )
            matches.append(match)
        
        return matches
    
    def match_semantic(
        self,
        query: str,
        top_k: int = 5,
        threshold: float = 0.75
    ) -> List[Match]:
        """
        Complete semantic matching pipeline.
        
        Encodes query and searches FAISS index in one call.
        
        Args:
            query: Raw query text (will be normalized internally)
            top_k: Number of results
            threshold: Minimum similarity
            
        Returns:
            List of Match objects
        """
        from src.normalization.text_normalizer import normalize_text
        
        # Normalize query
        query_norm = normalize_text(query)
        if not query_norm:
            return []
        
        # Encode
        query_embedding = self.encode_query(query_norm)
        
        # Search
        return self.search(query_embedding, top_k, threshold)
    
    def add_embeddings(
        self,
        texts: List[str],
        metadata_list: List[Dict[str, Any]]
    ):
        """
        Add new embeddings to the index incrementally.
        
        Thread-safe operation for online learning.
        
        Args:
            texts: List of texts to embed
            metadata_list: Corresponding metadata for each text
        """
        if not self.model or not self.index:
            raise RuntimeError("Model or index not initialized")
        
        if len(texts) != len(metadata_list):
            raise ValueError("texts and metadata_list must have same length")
        
        # Encode texts
        embeddings = self.model.encode(texts, convert_to_numpy=True)
        
        # L2 normalize
        if self.config.normalize_l2:
            norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
            embeddings = embeddings / np.maximum(norms, 1e-12)
        
        embeddings = embeddings.astype('float32')
        
        # Add to index (thread-safe)
        with self._index_lock:
            current_size = self.index.ntotal
            self.index.add(embeddings)
            
            # Update metadata
            for i, meta in enumerate(metadata_list):
                meta['faiss_index'] = current_size + i
                self.metadata[current_size + i] = meta
        
        logger.info(f"Added {len(texts)} embeddings to index (total: {self.index.ntotal})")
    
    def save_index(
        self,
        faiss_path: Optional[str] = None,
        metadata_path: Optional[str] = None
    ):
        """
        Save FAISS index and metadata to disk.
        
        Args:
            faiss_path: Path to save FAISS index (uses config default if None)
            metadata_path: Path to save metadata (uses config default if None)
        """
        if not self.index:
            raise RuntimeError("Index not initialized")
        
        faiss_path = faiss_path or os.path.join(self.base_path, self.config.faiss_index_path)
        metadata_path = metadata_path or os.path.join(self.base_path, self.config.metadata_path)
        
        # Ensure directories exist
        os.makedirs(os.path.dirname(faiss_path), exist_ok=True)
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
        
        # Save FAISS index
        with self._index_lock:
            faiss.write_index(self.index, faiss_path)
            logger.info(f"Saved FAISS index to {faiss_path}")
            
            # Save metadata
            metadata_list = [self.metadata[i] for i in sorted(self.metadata.keys())]
            with open(metadata_path, 'w') as f:
                json.dump(metadata_list, f, indent=2)
            logger.info(f"Saved metadata to {metadata_path}")
