# Neo4j Database to Data Importer Export

Exports Neo4j databases to Neo4j Data Importer package format for backup, migration, or re-import.

## What It Does

Extracts a complete Neo4j database and packages it for Neo4j Data Importer:
- Node data → CSV files (`{Label}.csv`)
- Relationship data → CSV files (`{RELATIONSHIP_TYPE}.csv`)
- Schema mapping → `neo4j_importer_model.json`

The output can be directly imported into Neo4j using the Data Importer tool.

## Input

Neo4j database connection via environment variables:

```bash
NEO4J_URI=bolt://localhost
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
OUTPUT_DIR=paysim
FORMAT_VERSION=2.4.0  # "2.4.0" for new format with indexes/constraints (default), "0.1.0" for legacy
```

## Output

**Node CSV files** - One per label with all properties as columns:
```csv
id,name,email
1,John,john@example.com
```

**Relationship CSV files** - Source/target identifiers plus relationship properties:
```csv
source_label,target_label,source_id,target_id,amount
Customer,Order,C123,O456,150.00
```

**Importer Model JSON** (`neo4j_importer_model.json`) - Import package for Neo4j Data Importer:
- File schemas with field types and sample data
- Graph schemas with node/relationship definitions
- **Indexes and constraints** from database (format 2.4.0)
- Mappings connecting CSV files to graph structure
- Can be opened directly in Neo4j Data Importer UI

**Format Versions:**
- **2.4.0** (default): New format with full index/constraint support
- **0.1.0**: Legacy format for older Data Importer versions

## Usage

```bash
# Install dependencies
uv sync

# Run export
uv run python main.py

# Run tests
uv run pytest
```

Output appears in `paysim/` directory (or `OUTPUT_DIR` if specified).
