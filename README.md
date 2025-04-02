# RDS Encryptor

## Overview
RDS Encryptor is a CLI tool that automates the encryption of Amazon RDS instances for **SOC2** and **CMMC** compliance. The tool performs the following steps:

1. Creates a snapshot of the existing RDS instance.
2. Encrypts the snapshot using a specified **AWS KMS key**.
3. Restores a new RDS instance from the encrypted snapshot.
4. Configures **DMS (AWS Database Migration Service)** to migrate data.
5. Sets up and executes replication tasks to transfer data from the source to the encrypted instance.

## Features
- **Automated RDS Encryption**: Encrypts an RDS instance with a KMS key.
- **DMS Integration**: Uses AWS DMS for seamless data migration.
- **Replication Support**: Maintains database consistency during migration.
- **Parameter Group Management**: Ensures correct settings for logical replication.

## Installation
```sh
pip install rds-encryptor
```

## Requirements
- Created dms replication instance and kms key before running the tool. [How to choose correct replication instance class?](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_BestPractices.SizingReplicationInstance.html)

## Usage
Run the tool using the CLI:
```sh
rds-encryptor \
    --rds-instance-name my-rds-instance \
    --master-password mypassword \
    --kms-key-arn my-kms-key \
    --dms-replication-instance-arn my-dms-replication \
    --databases db1 db2 \
    --new-instance-identifier new-encrypted-instance
```

### CLI Arguments
| Argument | Short | Description |
|----------|-------|-------------|
| `--rds-instance-name` | `-r` | Source RDS instance ID |
| `--master-password` | `-p` | Master password for authentication |
| `--kms-key-arn` | `-k` | KMS key ARN for encryption |
| `--dms-replication-instance-arn` | `-i` | DMS replication instance ARN |
| `--databases` | `-d` | List of databases to encrypt and migrate |
| `--new-instance-identifier` | `-n` | Identifier for the new encrypted instance |

## Workflow
### 1. Validate Database Connections
Ensures the tool can connect to the source RDS instance before starting encryption.

### 2. Create Encrypted RDS Instance
- Takes a snapshot of the existing instance.
- Encrypts it using the specified KMS key.
- Creates a new RDS instance from the encrypted snapshot.

### 3. Configure Parameter Groups
- Adjusts `wal_sender_timeout`.
- Enables `pglogical` in `shared_preload_libraries`.
- Ensures `rds.logical_replication` is enabled.

### 4. Setup Database Migration
- Configures DMS endpoints.
- Creates replication tasks for each database.
- Truncates the target database before migration.

### 5. Execute Migration
- Runs the DMS replication tasks.
- Ensures sequences and IDs are correctly migrated.

## Logging
Logs are generated throughout the process, helping track the migration progress and any potential issues.

## License
This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
