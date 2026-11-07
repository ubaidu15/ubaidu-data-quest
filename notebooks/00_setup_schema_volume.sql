-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup: schema and Volume
-- MAGIC Creates the project's home in Unity Catalog. Run once; safe to re-run (`IF NOT EXISTS`).
-- MAGIC Run in: SQL Editor, or this notebook attached to any cluster or serverless.

-- COMMAND ----------

-- One schema holds everything for the quest: the raw Volume plus the bronze_/silver_/gold_ tables.
CREATE SCHEMA IF NOT EXISTS workspace.data_quest
  COMMENT 'Rearc Data Quest: raw Volume plus Bronze/Silver/Gold tables';

-- COMMAND ----------

-- Managed Volume = file storage handled by Databricks. Raw source files land here untouched.
-- Path: /Volumes/workspace/data_quest/raw/
CREATE VOLUME IF NOT EXISTS workspace.data_quest.raw
  COMMENT 'Raw landing zone: BLS pr files and DataUSA population JSON';

-- COMMAND ----------

-- Verify: should show type MANAGED and our user as owner.
DESCRIBE VOLUME workspace.data_quest.raw;

-- COMMAND ----------

-- Manifest = ingestion's memory. One row per source file with its version info (ETag, date, hash).
-- Re-runs compare against this to skip unchanged files.
CREATE TABLE IF NOT EXISTS workspace.data_quest.ingest_manifest (
  source            STRING    NOT NULL COMMENT 'bls_pr or datausa_population',
  file_name         STRING    NOT NULL COMMENT 'Source file name (key with source)',
  source_url        STRING    COMMENT 'Full URL it was fetched from',
  etag              STRING    COMMENT 'Server ETag at last check (null if not provided)',
  last_modified     TIMESTAMP COMMENT 'Server Last-Modified at last check',
  size_bytes        BIGINT    COMMENT 'Size of landed content',
  sha256            STRING    COMMENT 'SHA-256 of landed content',
  volume_path       STRING    COMMENT 'Where the current copy lives in the Volume',
  status            STRING    COMMENT 'active or removed',
  first_ingested_at TIMESTAMP,
  last_ingested_at  TIMESTAMP COMMENT 'Last time content was actually written',
  last_checked_at   TIMESTAMP COMMENT 'Last time ingestion looked at this file'
) COMMENT 'Ingestion state: one row per source file. Drives idempotent re-runs.';

-- COMMAND ----------

-- Append-only audit log: one row per file per run, saying what happened (new/changed/unchanged/removed).
CREATE TABLE IF NOT EXISTS workspace.data_quest.ingest_log (
  run_id     STRING    COMMENT 'UTC timestamp id of the ingestion run',
  source     STRING,
  file_name  STRING,
  action     STRING    COMMENT 'new | changed | unchanged | metadata_only | removed',
  sha256     STRING,
  logged_at  TIMESTAMP
) COMMENT 'Append-only audit log: what each ingestion run did to each file.';
