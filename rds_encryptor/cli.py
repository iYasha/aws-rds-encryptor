import argparse

from rds_encryptor.encryption_pipeline import EncryptionPipeline


def main():
    parser = argparse.ArgumentParser(description="Encrypt RDS instance")
    parser.add_argument("--rds-instance-name", "-r", type=str, required=True, help="RDS instance ID")
    parser.add_argument(
        "--master-password",
        "-p",
        type=str,
        required=True,
        help="Master password for the database",
    )
    parser.add_argument(
        "--kms-key-arn",
        "-k",
        type=str,
        required=True,
        help="AWS KMS key ARN for encryption",
    )
    parser.add_argument(
        "--dms-replication-instance-arn",
        "-i",
        type=str,
        required=True,
        help="AWS DMS replication instance ARN",
    )
    parser.add_argument(
        "--databases",
        "-d",
        type=str,
        nargs="*",
        required=True,
        help="List of databases to encrypt",
    )
    parser.add_argument(
        "--new-instance-identifier",
        "-n",
        type=str,
        required=False,
        help="Identifier for the new encrypted RDS instance",
    )
    args = parser.parse_args()
    pipeline = EncryptionPipeline(
        instance_id=args.rds_instance_name,
        master_password=args.master_password,
        kms_key_arn=args.kms_key_arn,
        dms_replication_instance_arn=args.dms_replication_instance_arn,
        databases=args.databases,
        new_instance_identifier=args.new_instance_identifier,
    )
    pipeline.run_pipeline()


if __name__ == "__main__":
    main()
