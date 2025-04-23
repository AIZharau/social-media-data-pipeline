-- Create schema for optimized orders data
SET client_encoding = 'UTF8';

-- 1. Create tables with appropriate indexes and constraints

-- Subjects lookup table
CREATE TABLE IF NOT EXISTS subjects (
    subject_id SERIAL PRIMARY KEY,
    subject_name VARCHAR(100) NOT NULL UNIQUE
);

-- Courses lookup table
CREATE TABLE IF NOT EXISTS courses (
    course_id SERIAL PRIMARY KEY,
    course_name VARCHAR(255) NOT NULL,
    subject_id INTEGER NOT NULL REFERENCES subjects(subject_id),
    UNIQUE(course_name, subject_id)
);

-- Packages lookup table
CREATE TABLE IF NOT EXISTS packages (
    package_id SERIAL PRIMARY KEY,
    package_name VARCHAR(255) NOT NULL UNIQUE
);

-- Junction table for package-course relationships
CREATE TABLE IF NOT EXISTS package_courses (
    package_id INTEGER NOT NULL REFERENCES packages(package_id),
    course_id INTEGER NOT NULL REFERENCES courses(course_id),
    PRIMARY KEY (package_id, course_id)
);

-- Create partitioned orders table by order_date (range partitioning)
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    course_id INTEGER REFERENCES courses(course_id),
    package_id INTEGER REFERENCES packages(package_id),
    order_date DATE NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    payment_status VARCHAR(20) NOT NULL
) PARTITION BY RANGE (order_date);

-- Create partitions by month
-- We'll create partitions for 2023 as an example
CREATE TABLE orders_2023_01 PARTITION OF orders
    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');
    
CREATE TABLE orders_2023_02 PARTITION OF orders
    FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');
    
CREATE TABLE orders_2023_03 PARTITION OF orders
    FOR VALUES FROM ('2023-03-01') TO ('2023-04-01');
    
-- Add more partitions as needed...

CREATE TABLE orders_default PARTITION OF orders
    DEFAULT;

-- 2. Create indexes for common query patterns

-- Index on the partitioning key (order_date) 
CREATE INDEX idx_orders_date ON orders(order_date);

-- Index for user queries
CREATE INDEX idx_orders_user_id ON orders(user_id);

-- Indexes for course/package lookups
CREATE INDEX idx_orders_course_id ON orders(course_id);
CREATE INDEX idx_orders_package_id ON orders(package_id);

-- Compound index for course sales analysis
CREATE INDEX idx_course_date ON orders(course_id, order_date);

-- Compound index for package sales analysis
CREATE INDEX idx_package_date ON orders(package_id, order_date);

-- For the status-based queries
CREATE INDEX idx_payment_status ON orders(payment_status);

-- 3. Set up database parameters for bulk loading (these would normally be in postgresql.conf)
-- These are just examples; actual values would depend on available system resources
/*
work_mem = 64MB                 -- Increase sort memory
maintenance_work_mem = 512MB    -- Increase memory for bulk operations
checkpoint_timeout = 30min      -- Less frequent checkpoints during bulk load
max_wal_size = 4GB              -- Larger WAL size for bulk operations
synchronous_commit = off        -- Faster inserts (but less durability during load)
wal_buffers = 16MB              -- Larger WAL buffers
*/

-- 4. Configure storage parameters for performance
ALTER TABLE orders SET (
    autovacuum_vacuum_scale_factor = 0.1,
    autovacuum_analyze_scale_factor = 0.05
);

-- Create temporary tables for bulk loading
CREATE TEMPORARY TABLE temp_orders (
    order_id INTEGER,
    user_id INTEGER NOT NULL,
    course_name VARCHAR(255),
    subject_name VARCHAR(100),
    package_name VARCHAR(255),
    order_date DATE NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    payment_status VARCHAR(20) NOT NULL
);

COMMENT ON TABLE orders IS 'Partitioned table containing order information';
COMMENT ON TABLE courses IS 'Lookup table for course information';
COMMENT ON TABLE subjects IS 'Lookup table for subject information';
COMMENT ON TABLE packages IS 'Lookup table for package information'; 