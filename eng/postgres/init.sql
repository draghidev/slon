SET password_encryption = 'scram-sha-256';
CREATE ROLE slon_scram LOGIN PASSWORD 'scram-password';
CREATE ROLE slon_password LOGIN PASSWORD 'cleartext-password';

SET password_encryption = 'md5';
CREATE ROLE slon_md5 LOGIN PASSWORD 'md5-password';
