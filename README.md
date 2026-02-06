# Neo4j Database to Data Importer Export

Exports Neo4j databases to Neo4j Data Importer v3.0 package format for backup, migration, or re-import.

## What It Does

Extracts a complete Neo4j database and packages it for Neo4j Data Importer:
- Node data → CSV files (`{Label}.csv`)
- Relationship data → CSV files (`{SourceLabel}_{REL_TYPE}_{TargetLabel}.csv`)
- Schema mapping → `neo4j_importer_model.json`
- Intelligent identifier detection (unique constraints → 'id' properties → unique values)
- Complete relationship pattern export (handles multi-target relationships)

The output can be directly imported into Neo4j using the Data Importer tool.

## Installation

```bash
# Clone or download the repository
git clone <repository-url>
cd neo4j-data-importer-export

# Install dependencies with uv
uv sync
```

## Configuration

The tool supports multiple configuration methods (in priority order):

### 1. Command Line Arguments (Highest Priority)

```bash
uv run python neo4j_export.py \
  --uri bolt://localhost:7687 \
  --user neo4j \
  --password mypassword \
  --database neo4j \
  --output my_export \
  --format 3.0
```

### 2. Environment Variables

```bash
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=mypassword
export NEO4J_DATABASE=neo4j
export OUTPUT_DIR=my_export
export FORMAT_VERSION=3.0

uv run python neo4j_export.py
```

### 3. .env File (Lowest Priority)

Create a `.env` file:
```bash
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=mypassword
NEO4J_DATABASE=neo4j
OUTPUT_DIR=my_export
FORMAT_VERSION=3.0
```

Then run:
```bash
uv run python neo4j_export.py

# Or specify a custom env file:
uv run python neo4j_export.py --env-file .env.production
```

## CLI Options

```
usage: neo4j_export.py [-h] [-u URI] [-U USER] [-P PASSWORD] [-d DATABASE]
                       [-o OUTPUT] [-f {3.0,2.4.0,0.1.0}] [-z ZIP] [--no-zip]
                       [--env-file ENV_FILE]

Connection Options:
  -u, --uri URI          Neo4j connection URI (default: bolt://localhost)
  -U, --user USER        Neo4j username (default: neo4j)
  -P, --password PASS    Neo4j password (default: password)
  -d, --database DB      Neo4j database name (optional)

Output Options:
  -o, --output DIR       Output directory/basename (default: paysim)
  -f, --format VERSION   Format version: 3.0, 2.4.0, or 0.1.0 (default: 3.0)
  -z, --zip FILE         Custom zip file name (default: auto-generated with timestamp)
  --no-zip               Skip creating zip file (only export CSVs and JSON)

Other:
  --env-file FILE        Path to .env file (default: .env if exists)
  -h, --help            Show this help message
```

## Output Format

### Zip Package
By default, the tool creates a ready-to-import zip file:
- **Auto-generated name**: `{output_dir}-export-{timestamp}.zip`
- **Custom name**: Use `-z myexport.zip` option
- **Skip zip**: Use `--no-zip` to only create CSVs and JSON

The zip contains all CSV files and the model JSON, ready to upload directly to Neo4j Data Importer.

### Node CSV Files
One per label with identifier first, then all properties:
```csv
id,firstName,lastName,email
4193892236705694,John,Doe,john@example.com
```

### Relationship CSV Files
Label-prefixed identifiers plus relationship properties:
```csv
Customer_id,Order_id,amount,timestamp
4193892236705694,12345,150.00,2024-01-15
```

**Filename format:** `{SourceLabel}_{REL_TYPE}_{TargetLabel}.csv`
- Example: `Customer_PLACED_Order.csv`
- Handles multi-target relationships (e.g., `Transaction_BENEFITS_TO_Bank.csv`, `Transaction_BENEFITS_TO_Merchant.csv`)

### Importer Model JSON
`neo4j_importer_model.json` - Complete import package:
- Table schemas with field types and samples
- Node/relationship type definitions
- Constraints and indexes from database
- Property mappings (CSV columns → graph properties)
- Compatible with Neo4j Data Importer v3.0

**Format Versions:**
- **3.0** (default): Latest format with full v3.0 structure
- **2.4.0**: Beta format with indexes/constraints
- **0.1.0**: Legacy format for older Data Importer versions

## Examples

```bash
# Quick export with defaults (creates auto-named zip)
uv run python neo4j_export.py
# Creates: paysim-export-2026-02-06-HHMMSS.zip

# Export to custom directory with custom zip name
uv run python neo4j_export.py -o fraud_detection -z fraud-export.zip

# Export from remote Neo4j instance
uv run python neo4j_export.py \
  -u bolt://production.neo4j.com:7687 \
  -U admin \
  -P secret123 \
  -d frauddb \
  -o fraud_export \
  -z fraud-production.zip

# Use .env.production file
uv run python neo4j_export.py --env-file .env.production -o prod_export

# Legacy format for older Data Importer
uv run python neo4j_export.py --format 0.1.0 -o legacy_export

# Skip zip creation (only CSVs and JSON)
uv run python neo4j_export.py -o my_export --no-zip
```

## Features

- ✅ **Intelligent Identifier Detection**: Automatically finds the best identifier for each node (unique constraints → 'id' properties → unique-valued properties)
- ✅ **Complete Pattern Export**: Handles relationship types with multiple source/target combinations (e.g., BENEFITS_TO going to Bank, Merchant, and Account)
- ✅ **NULL Filtering**: Automatically excludes nodes/relationships with NULL identifiers
- ✅ **Format Versions**: Supports v3.0 (latest), v2.4.0 (beta), and v0.1.0 (legacy)
- ✅ **Flexible Configuration**: CLI args, environment variables, or .env files
- ✅ **Direct Import**: Generated packages work directly with Neo4j Data Importer UI
