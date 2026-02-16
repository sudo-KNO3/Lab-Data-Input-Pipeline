"""
Generate embeddings for all synonyms and create FAISS index.

This script:
1. Loads all synonyms from the database
2. Encodes them using sentence-transformers
3. Creates a FAISS IndexFlatIP (inner product / cosine similarity)
4. Saves embeddings and index to disk
5. Updates the embeddings_metadata table
"""

import os
import sys
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer
from sqlalchemy import select

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import get_session
from src.database.models import Synonym, Analyte, EmbeddingMetadata
from src.matching.types import EmbeddingConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def compute_file_hash(file_path: str) -> str:
    """Compute SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    
    with open(file_path, "rb") as f:
        # Read in chunks to handle large files
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    
    return sha256_hash.hexdigest()


def main():
    """Generate embeddings and create FAISS index."""
    logger.info("=" * 80)
    logger.info("EMBEDDING GENERATION AND FAISS INDEX CREATION")
    logger.info("=" * 80)
    
    # Configuration
    config = EmbeddingConfig()
    base_path = project_root
    
    # Paths
    vectors_path = os.path.join(base_path, config.vectors_path)
    faiss_path = os.path.join(base_path, config.faiss_index_path)
    metadata_path = os.path.join(base_path, config.metadata_path)
    
    # Ensure directories exist
    os.makedirs(os.path.dirname(vectors_path), exist_ok=True)
    os.makedirs(os.path.dirname(faiss_path), exist_ok=True)
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
    
    # Step 1: Load synonyms from database
    logger.info("Step 1: Loading synonyms from database...")
    
    session = next(get_session())
    
    try:
        # Query all synonyms with their analytes
        stmt = (
            select(Synonym, Analyte)
            .join(Analyte)
            .order_by(Synonym.id)
        )
        
        results = session.execute(stmt).all()
        
        if not results:
            logger.error("No synonyms found in database!")
            return
        
        logger.info(f"Loaded {len(results)} synonyms")
        
        # Prepare data
        synonym_data = []
        texts_to_embed = []
        
        for synonym, analyte in results:
            synonym_data.append({
                'synonym_id': synonym.id,
                'analyte_id': analyte.id,
                'synonym_raw': synonym.synonym_raw,
                'synonym_norm': synonym.synonym_norm,
                'analyte_name': analyte.preferred_name,
                'cas_number': analyte.cas_number,
                'harvest_source': synonym.harvest_source,
            })
            texts_to_embed.append(synonym.synonym_norm)
        
        logger.info(f"Prepared {len(texts_to_embed)} texts for embedding")
        
        # Step 2: Load sentence transformer model
        logger.info(f"Step 2: Loading model '{config.model_name}'...")
        model = SentenceTransformer(config.model_name)
        logger.info("Model loaded successfully")
        
        # Step 3: Encode synonyms in batches
        logger.info("Step 3: Encoding synonyms...")
        
        batch_size = 32
        all_embeddings = []
        
        for i in range(0, len(texts_to_embed), batch_size):
            batch = texts_to_embed[i:i + batch_size]
            batch_embeddings = model.encode(
                batch,
                convert_to_numpy=True,
                show_progress_bar=True
            )
            all_embeddings.append(batch_embeddings)
            
            if (i + batch_size) % 1000 == 0:
                logger.info(f"Encoded {i + batch_size}/{len(texts_to_embed)} synonyms")
        
        # Concatenate all embeddings
        embeddings = np.vstack(all_embeddings).astype('float32')
        logger.info(f"Generated embeddings: shape={embeddings.shape}, dtype={embeddings.dtype}")
        
        # Step 4: L2 normalize for cosine similarity via inner product
        logger.info("Step 4: Normalizing vectors (L2)...")
        
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        embeddings = embeddings / np.maximum(norms, 1e-12)
        
        logger.info("Vectors normalized")
        
        # Step 5: Save raw embeddings
        logger.info(f"Step 5: Saving embeddings to {vectors_path}...")
        np.save(vectors_path, embeddings)
        logger.info(f"Embeddings saved: {os.path.getsize(vectors_path) / (1024**2):.2f} MB")
        
        # Step 6: Create FAISS index
        logger.info("Step 6: Creating FAISS IndexFlatIP...")
        
        dimension = embeddings.shape[1]
        index = faiss.IndexFlatIP(dimension)  # Inner product (cosine after normalization)
        
        # Add vectors to index
        index.add(embeddings)
        
        logger.info(f"FAISS index created: {index.ntotal} vectors, dimension={dimension}")
        
        # Step 7: Save FAISS index
        logger.info(f"Step 7: Saving FAISS index to {faiss_path}...")
        faiss.write_index(index, faiss_path)
        logger.info(f"FAISS index saved: {os.path.getsize(faiss_path) / (1024**2):.2f} MB")
        
        # Step 8: Create metadata mapping
        logger.info("Step 8: Creating metadata mapping...")
        
        metadata_list = []
        for i, data in enumerate(synonym_data):
            metadata_list.append({
                'faiss_index': i,
                **data
            })
        
        # Save metadata
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata_list, f, indent=2)
        
        logger.info(f"Metadata saved: {len(metadata_list)} entries")
        
        # Step 9: Compute hashes
        logger.info("Step 9: Computing file hashes...")
        
        vectors_hash = compute_file_hash(vectors_path)
        faiss_hash = compute_file_hash(faiss_path)
        
        logger.info(f"Vectors hash: {vectors_hash[:16]}...")
        logger.info(f"FAISS hash: {faiss_hash[:16]}...")
        
        # Step 10: Update database metadata
        logger.info("Step 10: Updating embeddings_metadata table...")
        
        # Clear old embeddings metadata
        session.query(EmbeddingMetadata).delete()
        
        # Insert new metadata
        for i, data in enumerate(synonym_data):
            embedding_meta = EmbeddingMetadata(
                synonym_id=data['synonym_id'],
                analyte_id=data['analyte_id'],
                text_embedded=data['synonym_norm'],
                model_name=config.model_name,
                model_version="v1.0",
                embedding_dim=dimension,
                file_path=config.faiss_index_path,
                vector_index=i,
            )
            session.add(embedding_meta)
        
        session.commit()
        logger.info(f"Updated {len(synonym_data)} embedding metadata records")
        
        # Summary
        logger.info("=" * 80)
        logger.info("SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total synonyms embedded: {len(texts_to_embed)}")
        logger.info(f"Model: {config.model_name}")
        logger.info(f"Embedding dimension: {dimension}")
        logger.info(f"FAISS index type: IndexFlatIP (inner product)")
        logger.info(f"Vectors file: {vectors_path}")
        logger.info(f"FAISS index file: {faiss_path}")
        logger.info(f"Metadata file: {metadata_path}")
        logger.info(f"Vectors hash: {vectors_hash}")
        logger.info(f"FAISS hash: {faiss_hash}")
        logger.info("=" * 80)
        logger.info("✓ Embedding generation complete!")
        
    except Exception as e:
        logger.error(f"Error during embedding generation: {e}", exc_info=True)
        session.rollback()
        raise
    
    finally:
        session.close()


if __name__ == '__main__':
    main()
