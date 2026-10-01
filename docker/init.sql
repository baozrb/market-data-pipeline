-- Separate database for the test suite, so tests never touch pipeline data.
CREATE DATABASE IF NOT EXISTS market_test;
GRANT ALL PRIVILEGES ON market_test.* TO 'pipeline'@'%';
