---
name: accessing-cloud-storage
description: "Cloud storage authentication and access patterns: AWS S3, Google Cloud Storage, Azure Blob. Covers IAM roles, service principals, credential management, and secure access for data pipelines."
---

# Accessing Cloud Storage

Secure authentication and access patterns for cloud storage services (S3, GCS, Azure Blob) in data pipelines. Covers IAM roles, managed identities, service principals, secret management, and best practices for credential handling.

## When to use this skill

Use this skill when:
- Configuring authentication for AWS S3, Google Cloud Storage, or Azure Blob Storage
- Setting up IAM roles, service principals, or managed identities
- Managing credentials securely in data pipelines
- Implementing cross-account or cross-cloud access patterns
- Setting up CI/CD authentication for cloud storage

For the actual storage operations (reading/writing data), see the data access library skills:
- `data-engineering-storage-remote-access-libraries-fsspec`
- `data-engineering-storage-remote-access-libraries-pyarrow-fs`
- `data-engineering-storage-remote-access-libraries-obstore`

For lakehouse table formats (Delta Lake, Iceberg), see:
- `data-engineering-storage-lakehouse`

## Quick Reference

| Provider | Recommended Auth | Alternative |
|----------|----------------|-------------|
| **AWS** | IAM roles (EC2/ECS/Lambda) | Environment variables, Secrets Manager |
| **GCP** | Workload Identity / ADC | Service account keys (discouraged) |
| **Azure** | Managed Identity | Service principal with certificate |
| **Local Dev** | `.env` files + local credentials | Static keys (temporary only) |

## Core Principles

1. **Least Privilege**: Grant only necessary permissions (read-only, specific bucket)
2. **Short-lived credentials**: Use STS tokens, OIDC, not long-term keys
3. **Automatic rotation**: Prefer managed identities that rotate automatically
4. **Secret management**: Never commit credentials; use secret managers
5. **Audit everything**: Enable CloudTrail/Azure Audit Logs/GCP Audit Logs
6. **Separate environments**: Different credentials for dev/staging/prod

## When to Use What?

- **Production on cloud VMs**: Use IAM roles/Managed Identities (no credentials in code)
- **CI/CD pipelines**: Use workload identity federation (OIDC) or short-lived tokens
- **Local development**: `.env` files with user credentials from `aws configure`, `gcloud auth`, `az login`
- **Third-party integrations**: Service principals with scoped permissions
- **Cross-account access**: Role assumption (AWS), workload identity (GCP), service principal (Azure)

---

## Detailed Guides

### AWS Authentication
See: `references/aws.md`

- IAM roles (EC2 instance profiles, ECS task roles, Lambda execution roles)
- IAM users with access keys (discouraged for production)
- STS temporary credentials (AssumeRole, GetSessionToken)
- S3 presigned URLs for temporary file access
- Cross-account access patterns
- AWS Secrets Manager integration
- Environment variable resolution (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)

### Google Cloud Platform
See: `references/gcp.md`

- Service accounts (JSON keys)
- Workload Identity Federation (no keys needed!)
- Application Default Credentials (ADC)
- Cloud Storage signed URLs
- Secret Manager integration
- Environment variables (`GOOGLE_APPLICATION_CREDENTIALS`)
- GCP workload identity for GKE, Cloud Run, Compute Engine

### Azure
See: `references/azure.md`

- Managed Identities (system-assigned, user-assigned)
- Service Principals (client secret, certificate)
- SAS tokens for Blob Storage
- Azure Key Vault integration
- Environment variables (`AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`)
- Azure AD workload identity for AKS, App Service, VMs

### Patterns & Best Practices
See: `references/patterns.md`

- Secret rotation automation
- Multi-environment credential management
- Local development setup without production keys
- CI/CD pipeline authentication (GitHub Actions, GitLab CI, Jenkins)
- Testing with mock credentials (Moto, google-cloud-testutils)
- Credential leakage prevention (.gitignore, pre-commit hooks)

### Testing Strategies
See: `references/testing.md`

- Mocking cloud services for unit tests
- Using local emulators (MinIO, Azurite, LocalStack)
- Test credential patterns with placeholders
- Integration test setup with temporary credentials

---

## Quick Examples

### AWS IAM Role (Production)
```python
# No credentials in code - automatically from EC2/ECS/Lambda
import boto3
s3 = boto3.client('s3')  # Uses instance metadata
```

### GCP Workload Identity (Production)
```bash
# Enable workload identity on GKE/Cloud Run
# Then in Python:
import google.auth
credentials, project = google.auth.default()
# No env vars needed!
```

### Azure Managed Identity (Production)
```python
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

credential = DefaultAzureCredential()  # Auto-detects managed identity
client = BlobServiceClient(account_url="...", credential=credential)
```

### Local Development
```bash
# AWS
aws configure  # Enter keys from IAM user (dev only)

# GCP
gcloud auth application-default login

# Azure
az login
```

---

## Common Pitfalls

❌ **Hardcoding credentials** - Committing to git → rotate immediately  
❌ **Using root/admin accounts** - Create scoped users/service principals  
❌ **Long-lived keys** - Rotate every 90 days or less  
❌ **Over-permissive roles** - Grant `s3:GetObject` not `s3:*`  
❌ **Missing environment separation** - Dev credentials in prod  
❌ **Disabling TLS verification** - Except for local MinIO testing only  

---

## Related Skills

- `data-engineering-storage-remote-access-libraries-fsspec` - fsspec/s3fs/gcsfs/adlfs integration
- `data-engineering-storage-remote-access-libraries-pyarrow-fs` - PyArrow filesystem integration
- `data-engineering-storage-remote-access-libraries-obstore` - obstore Rust-based storage
- `data-engineering-storage-lakehouse` - Delta Lake, Iceberg table formats
- `data-engineering-orchestration` - Prefect, Dagster, dbt cloud connectors
- `data-engineering-core` - Polars, DuckDB, PyArrow data processing

---

## References

- [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [GCP Workload Identity](https://cloud.google.com/iam/docs/workload-identity-federation)
- [Azure Managed Identities](https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview)
- [HashiCorp Vault](https://developer.hashicorp.com/vault/docs)
