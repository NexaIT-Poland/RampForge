-- DCDock PostgreSQL Initialization Script
-- This script runs when the PostgreSQL container is first created

-- Create extensions if needed
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON DATABASE dcdock TO dcdock_user;
GRANT ALL ON SCHEMA public TO dcdock_user;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO dcdock_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO dcdock_user;

-- Connection info
SELECT 'DCDock database initialized successfully!' as message;
