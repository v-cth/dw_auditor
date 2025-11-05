import argparse
from dw_auditor.core.auditor import SecureTableAuditor
from dw_auditor.core.config import AuditConfig

def main():
    parser = argparse.ArgumentParser(description="Run Data Warehouse Audit")
    parser.add_argument("--config", default="audit_config.yaml", help="Path to config file")
    parser.add_argument("--full", action="store_true", help="Run full audit")
    parser.add_argument("--tables", nargs="*", help="Run specific tables only")
# need to integrate other flags

    args = parser.parse_args()

    # Load configuration
    config = AuditConfig.from_yaml(args.config)
    auditor = SecureTableAuditor()

    if args.full:
        print("Running full audit...")
        auditor.audit_all(config)
    elif args.tables:
        print(f"Running audit for specific tables: {args.tables}")
        for table in args.tables:
            auditor.audit_table(config, table)
    else:
        print("No mode specified. Use --full or --tables <name>")

if __name__ == "__main__":
    main()
