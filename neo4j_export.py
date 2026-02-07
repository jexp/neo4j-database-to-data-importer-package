#!/usr/bin/env python3
"""
Neo4j Graph Export Script
Exports nodes and relationships from a Neo4j database to CSV files
and generates a neo4j_importer_model.json mapping file.
"""

import os
import sys
import json
import csv
import argparse
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
from neo4j import GraphDatabase
from dotenv import load_dotenv
import uuid


class Neo4jExporter:
    def __init__(self, uri: str, user: str, password: str, output_dir: str = "paysim",
                 format_version: str = "3.0", database: Optional[str] = None):
        """Initialize Neo4j connection and output directory.

        Args:
            uri: Neo4j connection URI
            user: Neo4j username
            password: Neo4j password
            output_dir: Directory to write CSV files and model JSON
            format_version: "3.0" for latest format (default), "2.4.0" for beta format, "0.1.0" for legacy
            database: Neo4j database name (optional, defaults to server default)
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.output_dir = output_dir
        self.format_version = format_version
        self.database = database
        self.metadata = {}
        self.unique_constraints = {}
        self.indexes = []
        self.constraints = []
        self.label_identifiers = {}  # Map label -> identifier property name

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    def close(self):
        """Close the Neo4j driver connection."""
        self.driver.close()

    def _session(self):
        """Create a session with optional database parameter."""
        if self.database:
            return self.driver.session(database=self.database)
        return self.driver.session()

    def get_graph_metadata(self):
        """Retrieve graph metadata using apoc.meta.data."""
        with self._session() as session:
            # Get metadata from apoc.meta.data
            result = session.run("CALL apoc.meta.data()")

            for record in result:
                label_or_type = record["label"]
                property_name = record["property"]
                prop_type = record["type"]

                # Store metadata
                if label_or_type not in self.metadata:
                    self.metadata[label_or_type] = {
                        "properties": {},
                        "type": record["elementType"]  # 'node' or 'relationship'
                    }

                self.metadata[label_or_type]["properties"][property_name] = {
                    "type": prop_type,
                    "elementType": record["elementType"]
                }

            print(f"Retrieved metadata for {len(self.metadata)} labels/types")

    def get_unique_constraints(self):
        """Get unique constraints to identify business identifiers."""
        with self._session() as session:
            # Get constraints (works for Neo4j 4.x and 5.x)
            try:
                result = session.run("SHOW CONSTRAINTS")

                for record in result:
                    constraint_type = record.get("type", "")
                    if "UNIQUENESS" in constraint_type or "UNIQUE" in constraint_type:
                        # Extract label/type and properties
                        labelsOrTypes = record.get("labelsOrTypes", [])
                        properties = record.get("properties", [])

                        if labelsOrTypes and properties:
                            label = labelsOrTypes[0]
                            if label not in self.unique_constraints:
                                self.unique_constraints[label] = []
                            self.unique_constraints[label].extend(properties)

            except Exception as e:
                print(f"Warning: Could not retrieve constraints: {e}")
                print("Attempting fallback method...")

                # Fallback for older Neo4j versions
                try:
                    result = session.run("CALL db.constraints()")
                    for record in result:
                        description = record.get("description", "")
                        if "UNIQUE" in description:
                            # Parse description to extract label and property
                            # Format typically: "CONSTRAINT ON ( label:Label ) ASSERT (label.property) IS UNIQUE"
                            parts = description.split(":")
                            if len(parts) >= 2:
                                label = parts[1].split(")")[0].strip()
                                prop_parts = description.split(".")
                                if len(prop_parts) >= 2:
                                    prop = prop_parts[1].split(")")[0].strip()
                                    if label not in self.unique_constraints:
                                        self.unique_constraints[label] = []
                                    self.unique_constraints[label].append(prop)
                except Exception as e2:
                    print(f"Warning: Fallback method also failed: {e2}")
                    print("Proceeding without constraint information")

        print(f"Found unique constraints for {len(self.unique_constraints)} labels")
        for label, props in self.unique_constraints.items():
            print(f"  {label}: {', '.join(props)}")

    def _determine_identifier(self, label: str, properties: List[str], nodes: List[Dict]) -> str:
        """Determine the best identifier property for a node label.

        Priority:
        1. Unique constraint property
        2. Property named 'id' (case-insensitive)
        3. Property ending with 'id' or 'ID'
        4. Property with all unique values
        5. First property (fallback)
        """
        # 1. Check for unique constraint
        if label in self.unique_constraints and self.unique_constraints[label]:
            identifier = self.unique_constraints[label][0]
            print(f"    Using unique constraint property: {identifier}")
            return identifier

        # 2. Check for property named 'id' (case-insensitive)
        for prop in properties:
            if prop.lower() == 'id':
                print(f"    Using 'id' property: {prop}")
                return prop

        # 3. Check for property ending with 'id' or 'ID'
        id_candidates = [p for p in properties if p.lower().endswith('id')]
        if id_candidates:
            # Prefer exact 'id' match, then shortest name
            identifier = sorted(id_candidates, key=lambda x: (x.lower() != 'id', len(x)))[0]
            print(f"    Using ID-like property: {identifier}")
            return identifier

        # 4. Check for properties with all unique values
        for prop in properties:
            values = [node.get(prop) for node in nodes if node.get(prop) not in [None, "", "NULL"]]
            if values and len(values) == len(set(values)):
                print(f"    Using unique-valued property: {prop}")
                return prop

        # 5. Fallback to first property
        if properties:
            identifier = properties[0]
            print(f"    Using fallback (first property): {identifier}")
            return identifier

        raise ValueError(f"No properties found for label {label}")

    def get_indexes(self):
        """Get all indexes from the database."""
        self.indexes = []

        with self._session() as session:
            try:
                # Try modern SHOW INDEXES command (Neo4j 4.0+)
                result = session.run("SHOW INDEXES")

                for record in result:
                    index_name = record.get("name", "")
                    index_type = record.get("type", "RANGE")
                    labels = record.get("labelsOrTypes", [])
                    properties = record.get("properties", [])
                    entity_type = record.get("entityType", "NODE")

                    if labels and properties:
                        self.indexes.append({
                            "name": index_name,
                            "type": index_type,
                            "entityType": entity_type.lower(),
                            "labels": labels,
                            "properties": properties
                        })

            except Exception as e:
                print(f"Warning: Could not retrieve indexes: {e}")
                print("Attempting fallback method...")

                try:
                    # Fallback for older Neo4j versions
                    result = session.run("CALL db.indexes()")
                    for record in result:
                        description = record.get("description", "")
                        index_name = record.get("indexName", "")
                        label = record.get("tokenNames", [None])[0]
                        properties = record.get("properties", [])

                        if label and properties:
                            self.indexes.append({
                                "name": index_name,
                                "type": "default",
                                "entityType": "node",
                                "labels": [label],
                                "properties": properties
                            })
                except Exception as e2:
                    print(f"Warning: Fallback method also failed: {e2}")
                    print("Proceeding without index information")

        print(f"Found {len(self.indexes)} indexes")

    def get_constraints_detailed(self):
        """Get detailed constraint information."""
        self.constraints = []

        with self._session() as session:
            try:
                result = session.run("SHOW CONSTRAINTS")

                for record in result:
                    constraint_name = record.get("name", "")
                    constraint_type = record.get("type", "")
                    labels = record.get("labelsOrTypes", [])
                    properties = record.get("properties", [])
                    entity_type = record.get("entityType", "NODE")

                    # Map Neo4j constraint types to importer format
                    if "UNIQUENESS" in constraint_type or "UNIQUE" in constraint_type:
                        c_type = "uniqueness"
                    elif "EXISTENCE" in constraint_type or "EXISTS" in constraint_type:
                        c_type = "existence"
                    elif "NODE_KEY" in constraint_type or "KEY" in constraint_type:
                        c_type = "key"
                    else:
                        c_type = "uniqueness"  # default

                    if labels and properties:
                        self.constraints.append({
                            "name": constraint_name,
                            "type": c_type,
                            "entityType": entity_type.lower(),
                            "labels": labels,
                            "properties": properties
                        })

            except Exception as e:
                print(f"Warning: Could not retrieve detailed constraints: {e}")
                # Will use the basic unique_constraints already collected

        print(f"Found {len(self.constraints)} constraints")

    def export_nodes(self) -> Dict[str, List[str]]:
        """Export all nodes to CSV files, one per label."""
        node_labels = [label for label, meta in self.metadata.items()
                       if meta["type"] == "node"]

        exported_files = {}

        with self._session() as session:
            for label in node_labels:
                print(f"Exporting nodes with label: {label}")

                # Get all nodes with this label
                query = f"MATCH (n:`{label}`) RETURN n"
                result = session.run(query)

                nodes = []
                all_properties = set()

                # Collect all nodes and properties
                for record in result:
                    node = record["n"]
                    node_props = dict(node)
                    nodes.append(node_props)
                    all_properties.update(node_props.keys())

                if not nodes:
                    print(f"  No nodes found for label {label}")
                    continue

                # Determine identifier property for this label
                properties_list = sorted(all_properties)
                identifier = self._determine_identifier(label, properties_list, nodes)
                self.label_identifiers[label] = identifier

                # Filter out nodes with NULL identifier values
                nodes_with_id = []
                for node in nodes:
                    id_value = node.get(identifier)
                    if id_value not in [None, "", "NULL"]:
                        nodes_with_id.append(node)
                    else:
                        print(f"  Warning: Skipping node with NULL identifier: {node}")

                if len(nodes_with_id) < len(nodes):
                    print(f"  Filtered {len(nodes) - len(nodes_with_id)} nodes with NULL identifiers")

                # Sort properties to have identifier first
                sorted_properties = [identifier]
                sorted_properties.extend(sorted(p for p in all_properties if p != identifier))

                # Write to CSV
                filename = f"{label}.csv"
                filepath = os.path.join(self.output_dir, filename)

                with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=sorted_properties,
                                           extrasaction='ignore', restval='NULL')
                    writer.writeheader()
                    writer.writerows(nodes_with_id)

                exported_files[label] = sorted_properties
                print(f"  Exported {len(nodes_with_id)} nodes to {filepath}")

        return exported_files

    def export_relationships(self) -> Dict[str, Dict]:
        """Export all relationships to CSV files, one per (source, type, target) pattern."""
        rel_types = [label for label, meta in self.metadata.items()
                     if meta["type"] == "relationship"]

        exported_files = {}

        with self._session() as session:
            # First, discover all unique (source_label, rel_type, target_label) patterns
            print("Discovering relationship patterns...")
            patterns = []
            for rel_type in rel_types:
                pattern_query = f"""
                MATCH (s)-[r:`{rel_type}`]->(t)
                RETURN DISTINCT labels(s)[0] as source_label, type(r) as rel_type, labels(t)[0] as target_label
                """
                pattern_result = session.run(pattern_query)
                for record in pattern_result:
                    patterns.append((record["source_label"], record["rel_type"], record["target_label"]))

            print(f"Found {len(patterns)} unique relationship patterns")

            # Now export each pattern separately
            for source_label, rel_type, target_label in patterns:
                pattern_key = f"{source_label}_{rel_type}_{target_label}"
                print(f"Exporting relationship pattern: {source_label} -{rel_type}-> {target_label}")

                # Get all relationships matching this pattern
                query = f"""
                MATCH (source:`{source_label}`)-[r:`{rel_type}`]->(target:`{target_label}`)
                RETURN
                    source,
                    r,
                    target
                """
                result = session.run(query)

                relationships = []
                rel_properties = set()

                # Get identifier properties for this pattern
                source_id_prop = self.label_identifiers.get(source_label)
                target_id_prop = self.label_identifiers.get(target_label)

                if not source_id_prop or not target_id_prop:
                    print(f"  Warning: Missing identifier for {source_label} or {target_label}, skipping")
                    continue

                # Determine column names - check for self-relationships
                if source_label == target_label and source_id_prop == target_id_prop:
                    # Self-relationship: add _source/_target suffix to avoid duplicate columns
                    source_col_name = f"{source_label}_{source_id_prop}_source"
                    target_col_name = f"{target_label}_{target_id_prop}_target"
                else:
                    # Normal case: use label_property format
                    source_col_name = f"{source_label}_{source_id_prop}"
                    target_col_name = f"{target_label}_{target_id_prop}"

                # Collect all relationships and properties
                for record in result:
                    source = dict(record["source"])
                    target = dict(record["target"])
                    rel = dict(record["r"])

                    source_id_value = source.get(source_id_prop)
                    target_id_value = target.get(target_id_prop)

                    # Skip relationships with NULL identifiers
                    if source_id_value in [None, "", "NULL"] or target_id_value in [None, "", "NULL"]:
                        continue

                    rel_data = {
                        source_col_name: source_id_value,
                        target_col_name: target_id_value,
                    }

                    # Add relationship properties
                    rel_data.update(rel)
                    rel_properties.update(rel.keys())

                    relationships.append(rel_data)

                if not relationships:
                    print(f"  No relationships found for pattern")
                    continue

                # Define column order: source column, target column, then relationship properties
                sorted_properties = [source_col_name, target_col_name] + sorted(rel_properties)

                # Write to CSV with source and target labels in filename
                filename = f"{source_label}_{rel_type}_{target_label}.csv"
                filepath = os.path.join(self.output_dir, filename)

                with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=sorted_properties,
                                           extrasaction='ignore', restval='NULL')
                    writer.writeheader()
                    writer.writerows(relationships)

                # Store metadata for importer generation (keyed by pattern)
                exported_files[pattern_key] = {
                    "rel_type": rel_type,
                    "all_properties": sorted_properties,  # All CSV columns
                    "rel_properties": sorted(rel_properties),  # Only relationship properties
                    "source_label": source_label,
                    "target_label": target_label,
                    "source_id_prop": source_id_prop,
                    "target_id_prop": target_id_prop,
                    "source_col_name": source_col_name,  # CSV column name for source
                    "target_col_name": target_col_name,  # CSV column name for target
                }

                print(f"  Exported {len(relationships)} relationships to {filepath}")

        return exported_files

    def generate_importer_model(self, node_files: Dict[str, List[str]],
                                rel_files: Dict[str, Dict]) -> Dict:
        """Generate neo4j_importer_model.json in the specified format version."""
        if self.format_version == "3.0":
            return self._generate_model_v3_0(node_files, rel_files)
        elif self.format_version == "2.4.0":
            return self._generate_model_v2_4(node_files, rel_files)
        else:
            return self._generate_model_v0_1(node_files, rel_files)

    def _generate_model_v3_0(self, node_files: Dict[str, List[str]],
                             rel_files: Dict[str, Dict]) -> Dict:
        """Generate neo4j_importer_model.json in format 3.0.0."""

        model = {
            "version": "3.0.0",
            "visualisation": {
                "nodes": []
            },
            "dataModel": {
                "version": "3.0",
                "graphSchemaRepresentation": {
                    "version": "1.0.0",
                    "graphSchema": {
                        "nodeLabels": [],
                        "relationshipTypes": [],
                        "nodeObjectTypes": [],
                        "relationshipObjectTypes": [],
                        "constraints": [],
                        "indexes": []
                    }
                },
                "graphSchemaExtensionsRepresentation": {
                    "nodeKeyProperties": []
                },
                "graphMappingRepresentation": {
                    "dataSourceSchema": {
                        "type": "local",
                        "tableSchemas": []
                    },
                    "nodeMappings": [],
                    "relationshipMappings": []
                },
                "configurations": {
                    "idsToIgnore": []
                }
            }
        }

        # Generate node labels and mappings with v3.0 property ID format
        node_id_map = {}  # Map label name to n:X id
        node_label_map = {}  # Map label name to nl:X id
        property_id_map = {}  # Map (label, property) to p:X_Y id

        for node_idx, (label, properties) in enumerate(node_files.items()):
            node_viz_id = f"n:{node_idx}"
            node_label_id = f"nl:{node_idx}"
            node_id_map[label] = node_viz_id
            node_label_map[label] = node_label_id

            # Visualization position
            x = 300 * (node_idx % 3) - 300
            y = 300 * (node_idx // 3)

            model["visualisation"]["nodes"].append({
                "id": node_viz_id,
                "position": {"x": x, "y": y}
            })

            # Get sample data
            csv_path = os.path.join(self.output_dir, f"{label}.csv")
            sample_data = {}
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    first_row = next(reader, {})
                    sample_data = first_row
            except:
                pass

            # Generate property definitions with v3.0 format (p:nodeIdx_propIdx)
            node_properties = []
            for prop_idx, prop in enumerate(properties):
                prop_id = f"p:{node_idx}_{prop_idx}"
                property_id_map[(label, prop)] = prop_id

                node_properties.append({
                    "$id": prop_id,
                    "token": prop,
                    "type": {"type": self._infer_type(sample_data.get(prop, ""))},
                    "nullable": sample_data.get(prop) in ["", "NULL", None]
                })

            # Node label schema
            model["dataModel"]["graphSchemaRepresentation"]["graphSchema"]["nodeLabels"].append({
                "$id": node_label_id,
                "token": label,
                "properties": node_properties
            })

            # Node object type (new in v3.0)
            model["dataModel"]["graphSchemaRepresentation"]["graphSchema"]["nodeObjectTypes"].append({
                "$id": node_viz_id,
                "labels": [{"$ref": f"#{node_label_id}"}]
            })

            # Determine key property
            key_prop_id = None
            if label in self.unique_constraints and self.unique_constraints[label]:
                key_prop_name = self.unique_constraints[label][0]
                key_prop_id = property_id_map.get((label, key_prop_name))

            if not key_prop_id and properties:
                # Use first property as key
                key_prop_id = property_id_map.get((label, properties[0]))

            # Node key property
            if key_prop_id:
                model["dataModel"]["graphSchemaExtensionsRepresentation"]["nodeKeyProperties"].append({
                    "node": {"$ref": f"#{node_viz_id}"},
                    "keyProperties": [{"$ref": f"#{key_prop_id}"}]
                })

                # Add constraint for key property (v3.0 includes constraints)
                model["dataModel"]["graphSchemaRepresentation"]["graphSchema"]["constraints"].append({
                    "$id": f"c:{node_idx}",
                    "name": f"{properties[0]}_{label}_uniq",
                    "constraintType": "uniqueness",
                    "entityType": "node",
                    "nodeLabel": {"$ref": f"#{node_label_id}"},
                    "relationshipType": None,
                    "properties": [{"$ref": f"#{key_prop_id}"}]
                })

            # Table schema
            filename = f"{label}.csv"
            model["dataModel"]["graphMappingRepresentation"]["dataSourceSchema"]["tableSchemas"].append({
                "name": filename,
                "expanded": False,
                "fields": [
                    {
                        "name": prop,
                        "sample": str(sample_data.get(prop, ""))[:50],
                        "recommendedType": {"type": self._infer_type(sample_data.get(prop, ""))}
                    }
                    for prop in properties
                ]
            })

            # Node mapping
            model["dataModel"]["graphMappingRepresentation"]["nodeMappings"].append({
                "node": {"$ref": f"#{node_viz_id}"},
                "propertyMappings": [
                    {
                        "property": {"$ref": f"#{property_id_map[(label, prop)]}"},
                        "fieldName": prop
                    }
                    for prop in properties
                ],
                "tableName": filename
            })

        # Generate relationship types and mappings (v3.0 separates type from instances)
        rel_type_id_counter = 0
        rel_obj_id_counter = 0
        rel_type_tokens = {}  # Track which relationship type tokens we've seen

        for pattern_key, rel_info in rel_files.items():
            rel_type = rel_info["rel_type"]
            source_label = rel_info["source_label"]
            target_label = rel_info["target_label"]

            source_label_id = node_label_map.get(source_label)
            target_label_id = node_label_map.get(target_label)
            source_node_id = node_id_map.get(source_label)
            target_node_id = node_id_map.get(target_label)

            if not source_label_id or not target_label_id:
                print(f"Warning: Could not find label IDs for relationship {rel_type}")
                continue

            # Create relationship type if not already created
            if rel_type not in rel_type_tokens:
                rel_type_id = f"rt:{rel_type_id_counter}"
                rel_type_tokens[rel_type] = rel_type_id
                rel_type_id_counter += 1

                # Get relationship properties (only create type schema once per rel type)
                rel_properties = rel_info.get("rel_properties", [])

                # Get sample data
                csv_path = os.path.join(self.output_dir, f"{source_label}_{rel_type}_{target_label}.csv")
                sample_data = {}
                try:
                    with open(csv_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        first_row = next(reader, {})
                        sample_data = first_row
                except:
                    pass

                # Relationship properties (use rel_type_id_counter for property IDs)
                rel_props = []
                for prop_idx, prop in enumerate(rel_properties):
                    prop_id = f"p:r{rel_type_id_counter}_{prop_idx}"
                    rel_props.append({
                        "$id": prop_id,
                        "token": prop,
                        "type": {"type": self._infer_type(sample_data.get(prop, ""))},
                        "nullable": sample_data.get(prop) in ["", "NULL", None]
                    })

                # Relationship type schema (v3.0: no from/to here)
                model["dataModel"]["graphSchemaRepresentation"]["graphSchema"]["relationshipTypes"].append({
                    "$id": rel_type_id,
                    "token": rel_type,
                    "properties": rel_props
                })
            else:
                rel_type_id = rel_type_tokens[rel_type]

            # Relationship object type (v3.0: from/to goes here)
            rel_obj_id = f"r:{rel_obj_id_counter}"
            rel_obj_id_counter += 1

            model["dataModel"]["graphSchemaRepresentation"]["graphSchema"]["relationshipObjectTypes"].append({
                "$id": rel_obj_id,
                "type": {"$ref": f"#{rel_type_id}"},
                "from": {"$ref": f"#{source_node_id}"},
                "to": {"$ref": f"#{target_node_id}"}
            })

            # Get source and target identifier properties and column names
            source_id_prop = rel_info.get("source_id_prop")
            target_id_prop = rel_info.get("target_id_prop")
            source_col_name = rel_info.get("source_col_name")
            target_col_name = rel_info.get("target_col_name")

            # Build mappings for source and target identifiers (v3.0: use # prefix in keys)
            from_mappings = {}
            to_mappings = {}

            if source_id_prop and source_col_name:
                source_prop_id = property_id_map.get((source_label, source_id_prop))
                if source_prop_id:
                    from_mappings[f"#{source_prop_id}"] = source_col_name  # Note: # prefix

            if target_id_prop and target_col_name:
                target_prop_id = property_id_map.get((target_label, target_id_prop))
                if target_prop_id:
                    to_mappings[f"#{target_prop_id}"] = target_col_name  # Note: # prefix

            # Relationship mapping
            filename = f"{source_label}_{rel_type}_{target_label}.csv"
            rel_mapping = {
                "relationship": {"$ref": f"#{rel_obj_id}"},
                "tableName": filename,
                "fromMappings": from_mappings,
                "toMappings": to_mappings,
                "propertyMappings": []
            }

            model["dataModel"]["graphMappingRepresentation"]["relationshipMappings"].append(rel_mapping)

            # Add tableSchema for the relationship CSV
            all_rel_csv_fields = rel_info.get("all_properties", [])
            model["dataModel"]["graphMappingRepresentation"]["dataSourceSchema"]["tableSchemas"].append({
                "name": filename,
                "expanded": True,  # v3.0 uses expanded=true for relationships
                "fields": [
                    {
                        "name": field,
                        "sample": str(sample_data.get(field, ""))[:50],
                        "recommendedType": {"type": self._infer_type(sample_data.get(field, ""))}
                    }
                    for field in all_rel_csv_fields
                ]
            })

        return model

    def _generate_model_v2_4(self, node_files: Dict[str, List[str]],
                             rel_files: Dict[str, Dict]) -> Dict:
        """Generate neo4j_importer_model.json in format 2.4.0 with indexes and constraints."""

        model = {
            "version": "2.4.0-beta.0",
            "visualisation": {
                "nodes": []
            },
            "dataModel": {
                "version": "2.4.0-beta.0",
                "graphSchemaRepresentation": {
                    "version": "1.0.0",
                    "graphSchema": {
                        "nodeLabels": [],
                        "relationshipTypes": [],
                        "indexes": [],
                        "constraints": []
                    }
                },
                "graphSchemaExtensionsRepresentation": {
                    "nodeKeyProperties": []
                },
                "graphMappingRepresentation": {
                    "dataSourceSchema": {
                        "type": "local",
                        "tableSchemas": []
                    },
                    "nodeMappings": [],
                    "relationshipMappings": []
                },
                "configurations": {
                    "idsToIgnore": []
                }
            }
        }

        # Generate node labels and mappings
        node_id_map = {}
        node_label_map = {}  # Map label name to nl:nX id
        property_id_map = {}  # Map (label, property) to p:X id
        property_counter = 1

        for idx, (label, properties) in enumerate(node_files.items()):
            node_viz_id = f"n:n{idx}"
            node_label_id = f"nl:n{idx}"
            node_id_map[label] = node_viz_id
            node_label_map[label] = node_label_id

            # Visualization position
            x = 300 * (idx % 3) - 300
            y = 300 * (idx // 3)

            model["visualisation"]["nodes"].append({
                "id": node_viz_id,
                "position": {"x": x, "y": y}
            })

            # Get sample data
            csv_path = os.path.join(self.output_dir, f"{label}.csv")
            sample_data = {}
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    first_row = next(reader, {})
                    sample_data = first_row
            except:
                pass

            # Generate property definitions
            node_properties = []
            for prop in properties:
                prop_id = f"p:{property_counter}"
                property_id_map[(label, prop)] = prop_id
                property_counter += 1

                node_properties.append({
                    "$id": prop_id,
                    "token": prop,
                    "type": {"type": self._infer_type(sample_data.get(prop, ""))},
                    "nullable": sample_data.get(prop) in ["", "NULL", None]
                })

            # Node label schema
            model["dataModel"]["graphSchemaRepresentation"]["graphSchema"]["nodeLabels"].append({
                "$id": node_label_id,
                "token": label,
                "properties": node_properties
            })

            # Determine key property
            key_prop_id = None
            if label in self.unique_constraints and self.unique_constraints[label]:
                key_prop_name = self.unique_constraints[label][0]
                key_prop_id = property_id_map.get((label, key_prop_name))

            if not key_prop_id and properties:
                # Use first property as key
                key_prop_id = property_id_map.get((label, properties[0]))

            # Node key property
            if key_prop_id:
                model["dataModel"]["graphSchemaExtensionsRepresentation"]["nodeKeyProperties"].append({
                    "node": {"$ref": f"#{node_viz_id}"},
                    "keyProperties": [{"$ref": f"#{key_prop_id}"}]  # Array of refs
                })

            # Table schema
            filename = f"{label}.csv"
            model["dataModel"]["graphMappingRepresentation"]["dataSourceSchema"]["tableSchemas"].append({
                "name": filename,
                "expanded": False,
                "fields": [
                    {
                        "name": prop,
                        "sample": str(sample_data.get(prop, ""))[:50],
                        "recommendedType": {"type": self._infer_type(sample_data.get(prop, ""))}
                    }
                    for prop in properties
                ],
                "primaryKeys": [],
                "foreignKeys": []
            })

            # Node mapping
            model["dataModel"]["graphMappingRepresentation"]["nodeMappings"].append({
                "node": {"$ref": f"#{node_viz_id}"},
                "propertyMappings": [
                    {
                        "property": {"$ref": f"#{property_id_map[(label, prop)]}"},
                        "fieldName": prop
                    }
                    for prop in properties
                ],
                "tableName": filename
            })

        # Generate relationship types and mappings
        rel_id_counter = 0
        rel_type_ids = {}  # Track relationship type IDs by rel_type name

        for pattern_key, rel_info in rel_files.items():
            rel_type = rel_info["rel_type"]
            source_label = rel_info["source_label"]
            target_label = rel_info["target_label"]

            # Create unique key for this relationship type + source/target combination
            rel_schema_key = f"{source_label}_{rel_type}_{target_label}"

            # Create a new relationship type ID for this specific pattern
            rel_type_id = f"r:n{rel_id_counter}"
            rel_id_counter += 1

            source_label_id = node_label_map.get(source_label)
            target_label_id = node_label_map.get(target_label)
            source_node_id = node_id_map.get(source_label)
            target_node_id = node_id_map.get(target_label)

            if not source_label_id or not target_label_id:
                print(f"Warning: Could not find label IDs for relationship {rel_type}")
                continue

            # Get relationship properties (only the relationship's own properties)
            rel_properties = rel_info.get("rel_properties", [])

            # Get sample data
            csv_path = os.path.join(self.output_dir, f"{rel_type}.csv")
            sample_data = {}
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    first_row = next(reader, {})
                    sample_data = first_row
            except:
                pass

            # Relationship properties
            rel_props = []
            for prop in rel_properties:
                prop_id = f"p:{property_counter}"
                property_counter += 1

                rel_props.append({
                    "$id": prop_id,
                    "token": prop,
                    "type": {"type": self._infer_type(sample_data.get(prop, ""))},
                    "nullable": sample_data.get(prop) in ["", "NULL", None]
                })

            # Relationship type schema
            model["dataModel"]["graphSchemaRepresentation"]["graphSchema"]["relationshipTypes"].append({
                "$id": rel_type_id,
                "token": rel_type,
                "from": {"$ref": f"#{source_label_id}"},
                "to": {"$ref": f"#{target_label_id}"},
                "properties": rel_props
            })

            # Get source and target identifier properties and column names
            source_id_prop = rel_info.get("source_id_prop")
            target_id_prop = rel_info.get("target_id_prop")
            source_col_name = rel_info.get("source_col_name")
            target_col_name = rel_info.get("target_col_name")

            # Build mappings for source and target identifiers
            from_mappings = {}
            to_mappings = {}

            if source_id_prop and source_col_name:
                source_prop_id = property_id_map.get((source_label, source_id_prop))
                if source_prop_id:
                    from_mappings[source_prop_id] = source_col_name  # CSV column name

            if target_id_prop and target_col_name:
                target_prop_id = property_id_map.get((target_label, target_id_prop))
                if target_prop_id:
                    to_mappings[target_prop_id] = target_col_name  # CSV column name

            # Relationship mapping
            filename = f"{source_label}_{rel_type}_{target_label}.csv"
            rel_mapping = {
                "relationship": {"$ref": f"#{rel_type_id}"},
                "propertyMappings": [],
                "tableName": filename
            }

            # Add fromMappings/toMappings only if they exist
            if from_mappings:
                rel_mapping["fromMappings"] = from_mappings
            if to_mappings:
                rel_mapping["toMappings"] = to_mappings

            # Add property mappings if rel has properties
            for prop in rel_props:
                rel_mapping["propertyMappings"].append({
                    "property": {"$ref": f"#{prop['$id']}"},
                    "fieldName": prop["token"]
                })

            model["dataModel"]["graphMappingRepresentation"]["relationshipMappings"].append(rel_mapping)

            # Add tableSchema for the relationship CSV
            # Use actual CSV columns from rel_info
            all_rel_csv_fields = rel_info.get("all_properties", [])
            model["dataModel"]["graphMappingRepresentation"]["dataSourceSchema"]["tableSchemas"].append({
                "name": filename,
                "expanded": False,
                "fields": [
                    {
                        "name": field,
                        "sample": str(sample_data.get(field, ""))[:50],
                        "recommendedType": {"type": self._infer_type(sample_data.get(field, ""))}
                    }
                    for field in all_rel_csv_fields
                ],
                "primaryKeys": [],
                "foreignKeys": []
            })

        # Add indexes
        index_counter = 1
        for index_info in self.indexes:
            if index_info["entityType"] == "node":
                label = index_info["labels"][0]
                label_id = node_label_map.get(label)
                if label_id:
                    index_obj = {
                        "$id": f"i:{index_counter}",
                        "name": index_info["name"],
                        "indexType": index_info["type"] if index_info["type"] != "RANGE" else "default",
                        "entityType": "node",
                        "nodeLabel": {"$ref": f"#{label_id}"},
                        "properties": []
                    }

                    for prop_name in index_info["properties"]:
                        prop_id = property_id_map.get((label, prop_name))
                        if prop_id:
                            index_obj["properties"].append({"$ref": f"#{prop_id}"})

                    if index_obj["properties"]:
                        model["dataModel"]["graphSchemaRepresentation"]["graphSchema"]["indexes"].append(index_obj)
                        index_counter += 1

        # Add constraints
        constraint_counter = 0
        for constraint_info in self.constraints:
            if constraint_info["entityType"] == "node":
                label = constraint_info["labels"][0]
                label_id = node_label_map.get(label)
                if label_id:
                    constraint_obj = {
                        "$id": f"c:{constraint_counter}",
                        "name": constraint_info["name"],
                        "constraintType": constraint_info["type"],
                        "entityType": "node",
                        "nodeLabel": {"$ref": f"#{label_id}"},
                        "properties": []
                    }

                    for prop_name in constraint_info["properties"]:
                        prop_id = property_id_map.get((label, prop_name))
                        if prop_id:
                            constraint_obj["properties"].append({"$ref": f"#{prop_id}"})

                    if constraint_obj["properties"]:
                        model["dataModel"]["graphSchemaRepresentation"]["graphSchema"]["constraints"].append(constraint_obj)
                        constraint_counter += 1

        return model

    def _generate_model_v0_1(self, node_files: Dict[str, List[str]],
                             rel_files: Dict[str, Dict]) -> Dict:
        """Generate neo4j_importer_model.json in format 0.1.0 (for zip compatibility)."""

        model = {
            "version": "0.1.0-beta.0",
            "graph": {
                "nodes": [],
                "relationships": []
            },
            "dataModel": {
                "fileModel": {
                    "fileSchemas": {}
                },
                "graphModel": {
                    "nodeSchemas": {},
                    "relationshipSchemas": {}
                },
                "mappingModel": {
                    "nodeMappings": {},
                    "relationshipMappings": {}
                }
            }
        }

        # Generate node schemas and mappings
        node_id_map = {}
        property_id_counter = {}  # Track property IDs per node

        for idx, (label, properties) in enumerate(node_files.items()):
            node_id = f"n{idx}"
            node_id_map[label] = node_id

            # Graph visualization position
            x = 300 * (idx % 3) - 300
            y = 300 * (idx // 3)

            model["graph"]["nodes"].append({
                "id": node_id,
                "position": {"x": x, "y": y},
                "caption": label
            })

            # Get sample data
            csv_path = os.path.join(self.output_dir, f"{label}.csv")
            sample_data = {}
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    first_row = next(reader, {})
                    sample_data = first_row
            except:
                pass

            # File schema
            filename = f"{label}.csv"
            model["dataModel"]["fileModel"]["fileSchemas"][filename] = {
                "expanded": False,
                "fields": [
                    {
                        "name": prop,
                        "type": self._infer_type(sample_data.get(prop, "")),
                        "sample": str(sample_data.get(prop, ""))[:50],
                        "include": True
                    }
                    for prop in properties
                ]
            }

            # Node schema with properties
            node_properties = []
            for prop in properties:
                prop_identifier = str(uuid.uuid4())
                node_properties.append({
                    "property": prop,
                    "type": self._infer_type(sample_data.get(prop, "")),
                    "identifier": prop_identifier
                })

            # Determine key property (unique constraint)
            key_properties = []
            if label in self.unique_constraints and self.unique_constraints[label]:
                key_prop_name = self.unique_constraints[label][0]
                key_prop = next((p for p in node_properties if p["property"] == key_prop_name), None)
                if key_prop:
                    key_properties.append(key_prop["identifier"])

            if not key_properties and node_properties:
                # Use first property as key
                key_properties.append(node_properties[0]["identifier"])

            model["dataModel"]["graphModel"]["nodeSchemas"][node_id] = {
                "label": label,
                "additionLabels": [],
                "labelProperties": [],
                "properties": node_properties,
                "key": {
                    "properties": key_properties,
                    "name": ""
                }
            }

            # Node mapping
            model["dataModel"]["mappingModel"]["nodeMappings"][node_id] = {
                "nodeSchema": node_id,
                "fileSchema": filename,
                "mappings": [{"field": prop} for prop in properties]
            }

        # Generate relationship schemas and mappings
        rel_id_counter = 0
        for pattern_key, rel_info in rel_files.items():
            rel_type = rel_info["rel_type"]
            source_label = rel_info["source_label"]
            target_label = rel_info["target_label"]

            rel_id = f"n{rel_id_counter}"
            rel_id_counter += 1

            source_node_id = node_id_map.get(source_label)
            target_node_id = node_id_map.get(target_label)

            if not source_node_id or not target_node_id:
                print(f"Warning: Could not find node IDs for relationship {rel_type}")
                continue

            # Graph relationship
            model["graph"]["relationships"].append({
                "id": rel_id,
                "type": rel_type,
                "fromId": source_node_id,
                "toId": target_node_id
            })

            # Get relationship properties (only the relationship's own properties)
            rel_properties = rel_info.get("rel_properties", [])

            # Get sample data
            csv_path = os.path.join(self.output_dir, f"{rel_type}.csv")
            sample_data = {}
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    first_row = next(reader, {})
                    sample_data = first_row
            except:
                pass

            # Relationship schema
            rel_props = []
            for prop in rel_properties:
                rel_props.append({
                    "property": prop,
                    "type": self._infer_type(sample_data.get(prop, "")),
                    "identifier": str(uuid.uuid4())
                })

            model["dataModel"]["graphModel"]["relationshipSchemas"][rel_id] = {
                "type": rel_type,
                "sourceNodeSchema": source_node_id,
                "targetNodeSchema": target_node_id,
                "properties": rel_props
            }

            # Relationship mapping - CSV has label_property format for columns
            source_col_name = rel_info.get("source_col_name")
            target_col_name = rel_info.get("target_col_name")

            filename = f"{source_label}_{rel_type}_{target_label}.csv"
            model["dataModel"]["mappingModel"]["relationshipMappings"][rel_id] = {
                "relationshipSchema": rel_id,
                "mappings": [{"field": prop} for prop in rel_properties],
                "sourceMappings": [{"field": source_col_name}] if source_col_name else [],
                "targetMappings": [{"field": target_col_name}] if target_col_name else [],
                "fileSchema": filename
            }

        return model

    def _infer_type(self, value: str) -> str:
        """Infer property type from sample value."""
        if not value or value == "NULL":
            return "string"

        # Try integer
        try:
            int(value)
            return "integer"
        except ValueError:
            pass

        # Try float
        try:
            float(value)
            return "float"
        except ValueError:
            pass

        # Try boolean
        if value.lower() in ["true", "false"]:
            return "boolean"

        return "string"

    def create_zip(self, zip_path: Optional[str] = None) -> str:
        """Create a zip file containing all exported files.

        Args:
            zip_path: Optional custom path for zip file. If not provided,
                     generates name based on output_dir and timestamp.

        Returns:
            Path to created zip file
        """
        if not zip_path:
            # Generate timestamp-based zip filename
            timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            basename = Path(self.output_dir).name
            zip_path = f"{basename}-export-{timestamp}.zip"

        print(f"\nCreating zip file: {zip_path}")

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add all CSV files and the model JSON
            for file in Path(self.output_dir).glob('*'):
                if file.is_file() and (file.suffix == '.csv' or file.name == 'neo4j_importer_model.json'):
                    # Store with relative path (just the filename)
                    zipf.write(file, arcname=file.name)
                    print(f"  Added: {file.name}")

        file_size = Path(zip_path).stat().st_size
        size_mb = file_size / (1024 * 1024)
        print(f"Zip file created: {zip_path} ({size_mb:.1f} MB)")

        return zip_path

    def export_all(self, create_zip: bool = True) -> Optional[str]:
        """Run the complete export process.

        Args:
            create_zip: Whether to create a zip file after export

        Returns:
            Path to zip file if created, None otherwise
        """
        print("=== Starting Neo4j Export ===\n")

        print("Step 1: Retrieving graph metadata...")
        self.get_graph_metadata()
        print()

        print("Step 2: Retrieving constraints...")
        self.get_unique_constraints()
        if self.format_version in ["2.4.0", "3.0"]:
            self.get_constraints_detailed()
            print()
            print("Step 3: Retrieving indexes...")
            self.get_indexes()
        print()

        step = 3 if self.format_version in ["2.4.0", "3.0"] else 2

        print(f"Step {step + 1}: Exporting nodes to CSV...")
        node_files = self.export_nodes()
        print()

        print(f"Step {step + 2}: Exporting relationships to CSV...")
        rel_files = self.export_relationships()
        print()

        print(f"Step {step + 3}: Generating importer model...")
        importer_model = self.generate_importer_model(node_files, rel_files)

        # Write importer model to JSON file
        output_file = os.path.join(self.output_dir, "neo4j_importer_model.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(importer_model, f, indent=2)

        print(f"Generated importer model: {output_file}")
        print()

        print("=== Export Complete ===")
        print(f"Node labels exported: {len(node_files)}")
        print(f"Relationship types exported: {len(rel_files)}")
        print(f"Output directory: {self.output_dir}")

        # Create zip file if requested
        zip_path = None
        if create_zip:
            zip_path = self.create_zip()

        return zip_path


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Export Neo4j database to Neo4j Data Importer format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Configuration Priority (highest to lowest):
  1. Command line arguments
  2. Environment variables
  3. .env file
  4. Default values

Examples:
  # Using CLI arguments
  %(prog)s -u bolt://localhost -U neo4j -P password -o myexport

  # Using .env file
  %(prog)s --env-file .env.production -o myexport

  # Using environment variables
  export NEO4J_URI=bolt://localhost
  export NEO4J_USER=neo4j
  export NEO4J_PASSWORD=password
  %(prog)s -o myexport
        """
    )

    # Connection arguments
    conn_group = parser.add_argument_group('connection options')
    conn_group.add_argument(
        "-u", "--uri",
        help="Neo4j connection URI (default: bolt://localhost or NEO4J_URI env var)"
    )
    conn_group.add_argument(
        "-U", "--user",
        help="Neo4j username (default: neo4j or NEO4J_USER env var)"
    )
    conn_group.add_argument(
        "-P", "--password",
        help="Neo4j password (default: password or NEO4J_PASSWORD env var)"
    )
    conn_group.add_argument(
        "-d", "--database",
        help="Neo4j database name (default: neo4j or NEO4J_DATABASE env var)"
    )

    # Output arguments
    output_group = parser.add_argument_group('output options')
    output_group.add_argument(
        "-o", "--output",
        help="Output directory/basename for export (default: paysim or OUTPUT_DIR env var)"
    )
    output_group.add_argument(
        "-f", "--format",
        choices=["3.0", "2.4.0", "0.1.0"],
        help="Data Importer format version (default: 3.0 or FORMAT_VERSION env var)"
    )
    output_group.add_argument(
        "-z", "--zip",
        help="Custom zip file name (default: auto-generated with timestamp)"
    )
    output_group.add_argument(
        "--no-zip",
        action="store_true",
        help="Skip creating zip file (only export CSVs and JSON)"
    )

    # Config file
    parser.add_argument(
        "--env-file",
        help="Path to .env file (default: .env in current directory if exists)"
    )

    return parser.parse_args()


def load_config(args):
    """Load configuration from multiple sources with priority."""
    # Load .env file if specified or if default exists
    env_file = args.env_file if args.env_file else ".env"
    if os.path.exists(env_file):
        print(f"Loading configuration from {env_file}")
        load_dotenv(env_file)
    elif args.env_file:
        print(f"Warning: Specified env file {args.env_file} not found", file=sys.stderr)

    # Priority: CLI args > env vars > defaults
    config = {
        "uri": args.uri or os.getenv("NEO4J_URI", "bolt://localhost"),
        "user": args.user or os.getenv("NEO4J_USER", "neo4j"),
        "password": args.password or os.getenv("NEO4J_PASSWORD", "password"),
        "database": args.database or os.getenv("NEO4J_DATABASE"),
        "output_dir": args.output or os.getenv("OUTPUT_DIR", "paysim"),
        "format_version": args.format or os.getenv("FORMAT_VERSION", "3.0")
    }

    return config


def main():
    """Main entry point."""
    args = parse_args()
    config = load_config(args)

    print(f"Connecting to Neo4j at {config['uri']}")
    if config['database']:
        print(f"Database: {config['database']}")
    print(f"Output directory: {config['output_dir']}")
    print(f"Format version: {config['format_version']}")
    if args.no_zip:
        print("Zip creation: Disabled")
    elif args.zip:
        print(f"Zip file: {args.zip}")
    print()

    exporter = Neo4jExporter(
        config['uri'],
        config['user'],
        config['password'],
        config['output_dir'],
        config['format_version'],
        config['database']
    )

    try:
        # Run export
        create_zip = not args.no_zip
        zip_path = exporter.export_all(create_zip=create_zip)

        # Create custom-named zip if specified
        if create_zip and args.zip and zip_path:
            # Rename the auto-generated zip to custom name
            import shutil
            shutil.move(zip_path, args.zip)
            print(f"\nRenamed zip to: {args.zip}")

    finally:
        exporter.close()


if __name__ == "__main__":
    main()
