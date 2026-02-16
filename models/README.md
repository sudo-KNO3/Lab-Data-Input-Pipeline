# Models Directory

This directory stores trained model artifacts and configuration files.

## Contents

- **sentence-transformers/** - Downloaded SentenceTransformer models
  - `all-MiniLM-L6-v2/` - Default lightweight model (384d)
  - Model binaries are ignored by .gitignore due to size
  
- **fine-tuned/** (Future) - Custom fine-tuned models
  - Only needed if Layer 3 retraining is triggered (rare)

## Model Management

### Initial Download
Models are automatically downloaded on first use:
```python
from sentence_transformers import SentenceTransformer
model = SentenceTransformer('all-MiniLM-L6-v2')
```

### Storage
- Base models: ~90MB (MiniLM-L6-v2)
- Keep in version control: Model metadata and config only
- Actual binaries: Store in this directory but gitignored

## Model Versioning

Track model changes in `data_lineage.md` with:
- Model name and version
- Download date
- Embedding dimension
- Hash of generated embeddings

## Notes

This system rarely needs model retraining. Most learning happens through synonym accretion (Layer 1) and threshold calibration (Layer 2), not neural model updates (Layer 3).
