#!/usr/bin/env bash
# Initialize PostgreSQL schema locally
set -e

DB_NAME="ecommerce"
DB_USER="postgres"

echo "Creating PostgreSQL database $DB_NAME if not exists..."
createdb -U "$DB_USER" "$DB_NAME" || true

echo "Applying initial SQL DDL script..."
psql -U "$DB_USER" -d "$DB_NAME" -f scripts/init-postgres.sql

echo "Database initialization complete."
