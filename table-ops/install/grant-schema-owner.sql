-- =====================================================
-- Oracle Partition Management Packages - Privilege Grants
-- Grants minimal required privileges for non-DBA operation
-- Author: Principal Oracle Database Application Engineer
-- Version: 1.0
-- =====================================================

-- This script grants the minimal set of privileges required for the
-- partition management packages to operate without DBA privileges.
-- The packages are designed to work with schema-owned objects only.

-- Prerequisites:
-- 1. Run this script as a DBA or user with GRANT ANY PRIVILEGE
-- 2. Replace 'SCHEMA_OWNER' with the actual schema owner name
-- 3. Replace 'APP_USER' with the actual application user name
-- 4. The schema owner should already exist and have CREATE privileges

-- Variables to customize (replace with actual values)
DEFINE schema_owner = 'PARTITION_MGMT'
DEFINE app_user = 'APP_USER'

-- =====================================================
-- 1. BASIC OBJECT PRIVILEGES FOR SCHEMA OWNER
-- =====================================================

-- Grant comprehensive object creation privileges to schema owner
GRANT CREATE TABLE TO &schema_owner;
GRANT CREATE SEQUENCE TO &schema_owner;
GRANT CREATE PACKAGE TO &schema_owner;
GRANT CREATE PROCEDURE TO &schema_owner;
GRANT CREATE FUNCTION TO &schema_owner;
GRANT CREATE VIEW TO &schema_owner;
GRANT CREATE SYNONYM TO &schema_owner;
GRANT CREATE TRIGGER TO &schema_owner;
