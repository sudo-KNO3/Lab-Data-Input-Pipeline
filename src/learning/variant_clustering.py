"""
Variant clustering for Layer 4 learning.

Groups similar unknown variants to facilitate batch validation
and discover systematic naming patterns.
"""

import logging
from typing import List, Optional
import numpy as np

try:
    from Levenshtein import ratio as levenshtein_ratio
    LEVENSHTEIN_AVAILABLE = True
except ImportError:
    LEVENSHTEIN_AVAILABLE = False
    logging.warning("python-Levenshtein not available. Using fallback similarity.")

from sqlalchemy.orm import Session
from sqlalchemy import select

from ..database.models import MatchDecision, Analyte
from ..normalization.text_normalizer import TextNormalizer

logger = logging.getLogger(__name__)


class VariantClusterer:
    """
    Clusters similar unknown variants for batch validation.
    
    This enables Layer 4 learning: identifying systematic patterns
    in unknown variants to facilitate efficient batch validation.
    """
    
    def __init__(self, similarity_threshold: float = 0.85):
        """
        Initialize the variant clusterer.
        
        Args:
            similarity_threshold: Minimum similarity for clustering (0-1)
        """
        if not 0.0 <= similarity_threshold <= 1.0:
            raise ValueError("Similarity threshold must be between 0 and 1")
        
        self.similarity_threshold = similarity_threshold
        self.normalizer = TextNormalizer()
        logger.info(f"VariantClusterer initialized with threshold={similarity_threshold}")
    
    def cluster_similar_unknowns(
        self,
        unknown_terms: List[str],
        similarity_threshold: Optional[float] = None
    ) -> List[dict]:
        """
        Cluster similar unknown terms together.
        
        Args:
            unknown_terms: List of unknown variant text strings
            similarity_threshold: Override default threshold
        
        Returns:
            List of cluster dictionaries with structure:
            {
                'anchor': 'main variant',
                'similar_variants': [(variant, score), ...],
                'cluster_size': int,
                'avg_similarity': float
            }
        """
        if not unknown_terms:
            logger.warning("No unknown terms provided for clustering")
            return []
        
        threshold = similarity_threshold or self.similarity_threshold
        
        logger.info(f"Clustering {len(unknown_terms)} unknown terms with threshold={threshold}")
        
        # Normalize all terms
        normalized_terms = [(term, self.normalizer.normalize(term)) for term in unknown_terms]
        
        # Compute similarity matrix
        similarity_matrix = self._compute_similarity_matrix(
            [norm for _, norm in normalized_terms]
        )
        
        # Perform clustering using simple agglomerative approach
        clusters = self._agglomerative_cluster(
            normalized_terms,
            similarity_matrix,
            threshold
        )
        
        logger.info(f"Formed {len(clusters)} clusters from {len(unknown_terms)} terms")
        
        return clusters
    
    def _compute_similarity_matrix(self, terms: List[str]) -> np.ndarray:
        """
        Compute pairwise similarity matrix for terms.
        
        Args:
            terms: List of normalized term strings
        
        Returns:
            NxN similarity matrix
        """
        n = len(terms)
        similarity_matrix = np.zeros((n, n), dtype=np.float32)
        
        for i in range(n):
            for j in range(i, n):
                if i == j:
                    similarity_matrix[i, j] = 1.0
                else:
                    sim = self._compute_similarity(terms[i], terms[j])
                    similarity_matrix[i, j] = sim
                    similarity_matrix[j, i] = sim
        
        return similarity_matrix
    
    def _compute_similarity(self, term1: str, term2: str) -> float:
        """
        Compute similarity between two terms.
        
        Args:
            term1: First term
            term2: Second term
        
        Returns:
            Similarity score (0-1)
        """
        if LEVENSHTEIN_AVAILABLE:
            return levenshtein_ratio(term1, term2)
        else:
            # Fallback: simple character-level similarity
            return self._simple_similarity(term1, term2)
    
    def _simple_similarity(self, term1: str, term2: str) -> float:
        """Simple fallback similarity metric."""
        if not term1 or not term2:
            return 0.0
        
        # Jaccard similarity on character bigrams
        bigrams1 = set(term1[i:i+2] for i in range(len(term1)-1))
        bigrams2 = set(term2[i:i+2] for i in range(len(term2)-1))
        
        if not bigrams1 or not bigrams2:
            return 1.0 if term1 == term2 else 0.0
        
        intersection = len(bigrams1 & bigrams2)
        union = len(bigrams1 | bigrams2)
        
        return intersection / union if union > 0 else 0.0
    
    def _agglomerative_cluster(
        self,
        normalized_terms: List[tuple[str, str]],
        similarity_matrix: np.ndarray,
        threshold: float
    ) -> List[dict]:
        """
        Perform simple agglomerative clustering.
        
        Args:
            normalized_terms: List of (raw, normalized) term tuples
            similarity_matrix: Pairwise similarity matrix
            threshold: Similarity threshold for clustering
        
        Returns:
            List of cluster dictionaries
        """
        n = len(normalized_terms)
        assigned = [False] * n
        clusters = []
        
        for i in range(n):
            if assigned[i]:
                continue
            
            # Start a new cluster with this term as anchor
            cluster_indices = [i]
            assigned[i] = True
            
            # Find all similar terms
            for j in range(i + 1, n):
                if assigned[j]:
                    continue
                
                # Check similarity to anchor
                if similarity_matrix[i, j] >= threshold:
                    cluster_indices.append(j)
                    assigned[j] = True
            
            # Only create cluster if it has multiple members or represents unknown
            if len(cluster_indices) >= 1:
                anchor_raw, anchor_norm = normalized_terms[i]
                
                similar_variants = [
                    (normalized_terms[idx][0], float(similarity_matrix[i, idx]))
                    for idx in cluster_indices[1:]
                ]
                
                # Sort by similarity
                similar_variants.sort(key=lambda x: x[1], reverse=True)
                
                # Compute average similarity
                if similar_variants:
                    avg_sim = np.mean([score for _, score in similar_variants])
                else:
                    avg_sim = 1.0
                
                cluster = {
                    'anchor': anchor_raw,
                    'anchor_normalized': anchor_norm,
                    'similar_variants': similar_variants,
                    'cluster_size': len(cluster_indices),
                    'avg_similarity': float(avg_sim)
                }
                
                clusters.append(cluster)
        
        # Sort clusters by size (largest first)
        clusters.sort(key=lambda c: c['cluster_size'], reverse=True)
        
        return clusters
    
    def find_closest_analyte(
        self,
        term: str,
        db_session: Session,
        top_k: int = 3
    ) -> List[tuple[str, str, float]]:
        """
        Find the closest matching analytes for a term.
        
        Args:
            term: Term to match
            db_session: Database session
            top_k: Number of top matches to return
        
        Returns:
            List of (analyte_id, preferred_name, similarity_score) tuples
        """
        # Get all analytes
        stmt = select(Analyte)
        analytes = db_session.execute(stmt).scalars().all()
        
        if not analytes:
            return []
        
        # Normalize input term
        norm_term = self.normalizer.normalize(term)
        
        # Compute similarities
        similarities = []
        for analyte in analytes:
            norm_name = self.normalizer.normalize(analyte.preferred_name)
            sim = self._compute_similarity(norm_term, norm_name)
            similarities.append((analyte.analyte_id, analyte.preferred_name, sim))
        
        # Sort by similarity and return top_k
        similarities.sort(key=lambda x: x[2], reverse=True)
        
        return similarities[:top_k]
    
    def enrich_clusters_with_suggestions(
        self,
        clusters: List[dict],
        db_session: Session,
        top_k: int = 3
    ) -> List[dict]:
        """
        Enrich clusters with suggested analyte matches.
        
        Args:
            clusters: List of cluster dictionaries
            db_session: Database session
            top_k: Number of suggestions per cluster
        
        Returns:
            Enriched clusters with 'suggested_analytes' field
        """
        logger.info(f"Enriching {len(clusters)} clusters with analyte suggestions")
        
        for cluster in clusters:
            anchor = cluster['anchor']
            suggestions = self.find_closest_analyte(anchor, db_session, top_k)
            cluster['suggested_analytes'] = [
                {
                    'analyte_id': aid,
                    'preferred_name': name,
                    'similarity': float(score)
                }
                for aid, name, score in suggestions
            ]
        
        return clusters
    
    def get_clustering_statistics(self, clusters: List[dict]) -> dict:
        """
        Get statistics about the clustering results.
        
        Args:
            clusters: List of cluster dictionaries
        
        Returns:
            Statistics dictionary
        """
        if not clusters:
            return {
                'total_clusters': 0,
                'total_terms': 0,
                'avg_cluster_size': 0,
                'max_cluster_size': 0,
                'singleton_clusters': 0
            }
        
        cluster_sizes = [c['cluster_size'] for c in clusters]
        
        return {
            'total_clusters': len(clusters),
            'total_terms': sum(cluster_sizes),
            'avg_cluster_size': float(np.mean(cluster_sizes)),
            'max_cluster_size': max(cluster_sizes),
            'min_cluster_size': min(cluster_sizes),
            'singleton_clusters': sum(1 for size in cluster_sizes if size == 1),
            'avg_similarity': float(np.mean([c['avg_similarity'] for c in clusters]))
        }
