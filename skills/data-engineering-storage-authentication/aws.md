# AWS Authentication

Comprehensive guide to AWS authentication patterns for S3 and other services. Covers IAM users, roles, STS, workload identity, and credential resolution.

---

## Credential Resolution Priority

All AWS SDKs (boto3, s3fs, pyarrow.fs) follow this chain:

1. **Explicit credentials** passed to constructor
2. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
3. **Configuration files**: `~/.aws/credentials`, `~/.aws/config`
4. **IAM roles / instance profiles**: EC2, ECS, Lambda, Batch
5. **SSO / Identity Center** sessions

You can override any step by providing credentials explicitly.

---

## IAM Users (Access Keys)

For local development or CI/CD (not recommended for production):

```python
import s3fs

# Explicit credentials (from IAM user)
fs = s3fs.S3FileSystem(
    key='AKIA...',
    secret='...',
    token='...'  # optional session token for temporary credentials
)

# From AWS profile (~/.aws/credentials)
fs = s3fs.S3FileSystem(profile='my-profile')
```

**Security notes:**
- Never hardcode keys; use environment variables or config files
- Rotate access keys every 90 days
- Use IAM policy with least privilege

---

## IAM Roles (Production)

IAM roles provide temporary credentials automatically rotated by AWS. Attach roles to compute resources:

- **EC2 instance profile**: Role attached to VM
- **ECS task role**: Role per task definition
- **Lambda execution role**: Role per function
- **Batch compute environment**: Role for jobs

```python
import boto3

ec2 = boto3.client('ec2')  # Automatically uses instance profile
s3 = boto3.client('s3')    # No credentials in code!
```

**Setup:**
1. Create IAM role with trust policy allowing EC2/ECS/Lambda
2. Attach IAM policies (e.g., `AmazonS3ReadOnlyAccess`)
3. Assign role to resource in console/CLI/Terraform

---

## STS Temporary Credentials

AssumeRole to cross accounts or elevate privileges temporarily:

```python
import boto3

sts = boto3.client('sts')
assumed = sts.assume_role(
    RoleArn='arn:aws:iam::PROD_ACCOUNT:role/read-only',
    RoleSessionName='etl-session',
    DurationSeconds=3600  # 1 hour
)

creds = assumed['Credentials']
s3 = boto3.client(
    's3',
    aws_access_key_id=creds['AccessKeyId'],
    aws_secret_access_key=creds['SecretAccessKey'],
    aws_session_token=creds['SessionToken']
)
```

**Use cases:**
- Cross-account access
- Short-lived elevated permissions (principle of least privilege)
- Federation from external identity providers (OIDC/SAML)

---

## S3 Presigned URLs

Generate time-limited URLs for temporary object access:

```python
import boto3
from datetime import datetime, timedelta

s3 = boto3.client('s3')
url = s3.generate_presigned_url(
    ClientMethod='get_object',
    Params={'Bucket': 'my-bucket', 'Key': 'data.csv'},
    ExpiresIn=3600  # seconds
)
# Share URL - recipient does not need AWS credentials
```

---

## Workload Identity Federation (External Identity)

Allow GitHub Actions, Kubernetes, or on-prem systems to assume IAM roles without long-term keys using OIDC:

```yaml
# GitHub Actions
- uses: aws-actions/configure-aws-credentials@v4
  with:
    role-to-assume: arn:aws:iam::ACCOUNT:role/github-actions-role
    aws-region: us-east-1
```

Terraform:
```hcl
resource "aws_iam_role" "github" {
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRoleWithWebIdentity"
      Effect = "Allow"
      Principal = {
        Federated = "arn:aws:iam::ACCOUNT:oidc-provider/token.actions.githubusercontent.com"
      }
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "repo:org/repo:ref:refs/heads/main"
        }
      }
    }]
  })
}
```

**Benefit**: No static keys to store or rotate.

---

## Self-Hosted S3 Compatible (MinIO, Ceph, SeaweedFS)

For local development or private cloud, configure endpoint and certificate validation:

```python
import s3fs

fs = s3fs.S3FileSystem(
    client_kwargs={
        'endpoint_url': 'https://minio.local:9000',
        'region_name': 'us-east-1',
        'verify': '/etc/ssl/certs/minio-ca.pem'  # CA bundle for self-signed
    },
    config_kwargs={
        'max_pool_connections': 50,
        'retries': {'max_attempts': 5}
    }
)
```

**⚠️ Never use `verify=False` in production.** Use a proper CA bundle.

---

## Environment Variables Reference

| Variable | Purpose |
|-----------|---------|
| `AWS_ACCESS_KEY_ID` | Access key ID |
| `AWS_SECRET_ACCESS_KEY` | Secret access key |
| `AWS_SESSION_TOKEN` | Session token for temporary credentials |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | Default region |
| `AWS_PROFILE` | Named profile from config file |
| `AWS_CA_BUNDLE` | Path to CA certificate bundle |
| `AWS_EC2_METADATA_DISABLED` | Set `true` to disable IMDS (security) |

---

## Best Practices

1. ✅ **Prefer IAM roles** over access keys for production workloads
2. ✅ **Use workload identity federation** for CI/CD (GitHub Actions, GitLab)
3. ✅ **Least privilege**: Grant only specific S3 actions (GetObject, PutObject) on required buckets
4. ✅ **Rotate keys** every 90 days if using IAM users
5. ✅ **Enable CloudTrail** to audit all API activity
6. ✅ **Use separate credentials** per environment (dev, staging, prod)
7. ❌ **Don't** use root account credentials
8. ❌ **Don't** embed keys in code or Docker images
9. ❌ **Don't** disable TLS verification in production

---

## Troubleshooting

**"Unable to locate credentials"**
- Run `aws configure` for IAM user
- Ensure EC2/ECS has IAM role attached
- Check environment variables set correctly

**"403 Forbidden"**
- IAM role lacks required permissions
- Bucket policy denies access
- Using wrong region for bucket

---

## References

- [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [AssumeRole Documentation](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html)
- [Workload Identity Federation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_idp_oidc.html)
- `@data-engineering-storage-remote-access/libraries/fsspec` - fsspec S3 configuration
- `@data-engineering-storage-remote-access/libraries/pyarrow-fs` - PyArrow S3 integration
- `@data-engineering-storage-remote-access/libraries/obstore` - obstore S3 usage

```python
import s3fs

# Explicit credentials
fs = s3fs.S3FileSystem(
    key='AKIA...',
    secret='...',
    token='...'  # Optional session token for temporary credentials
)

# From AWS profile
fs = s3fs.S3FileSystem(profile='my-profile')

# From environment or default credential chain (recommended)
fs = s3fs.S3FileSystem()  # Uses AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, or IAM role

# S3-compatible endpoint (MinIO, Ceph, etc.)
fs = s3fs.S3FileSystem(
    client_kwargs={
        'endpoint_url': 'https://minio.local:9000',
        'region_name': 'us-east-1',
    },
    config_kwargs={
        'max_pool_connections': 50,
        'retries': {'max_attempts': 5}
    }
)
```

### PyArrow
```python
import pyarrow.fs as fs

# Explicit credentials
s3_fs = fs.S3FileSystem(
    access_key='AKIA...',
    secret_key='...',
    session_token='...'
)

# From URI - uses environment/config/iam automatically
s3_fs, path = fs.FileSystem.from_uri("s3://bucket/path/")

# S3-compatible endpoint
s3_fs = fs.S3FileSystem(
    endpoint_override="https://minio.local:9000",
    scheme="https",
    region="us-east-1"
)
```

### obstore
```python
from obstore.store import S3Store

# Explicit credentials
store = S3Store(
    bucket="my-bucket",
    region="us-east-1",
    access_key_id="AKIA...",
    secret_access_key="..."
)

# From environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
store = S3Store.from_env(bucket="my-bucket")

# S3-compatible endpoint
store = S3Store(
    bucket="my-bucket",
    region="us-east-1",
    endpoint="http://localhost:9000",
    access_key_id="minioadmin",
    secret_access_key="minioadmin",
    allow_http=True  # Use HTTP for local dev
)
```

---

## Google Cloud Storage

**Environment variable**: `GOOGLE_APPLICATION_CREDENTIALS` (path to service account JSON)

### fsspec (gcsfs)
```python
import fsspec

# From environment or gcloud CLI
gcs_fs = fsspec.filesystem('gcs', project='my-project')

# Explicit service account
gcs_fs = fsspec.filesystem(
    'gcs',
    project='my-project',
    token='/path/to/service-account.json'
)
```

### PyArrow
```python
import pyarrow.fs as fs

# From environment/application default
gcs_fs, path = fs.FileSystem.from_uri("gs://bucket/path/")

# No explicit credential parameter - relies on Google SDK
```

### obstore
```python
from obstore.store import GCSStore

# From environment
gcs = GCSStore.from_env(bucket="my-bucket")

# Explicit service account
gcs = GCSStore(
    bucket="my-bucket",
    service_account_path="/path/to/key.json"
)
```

---

## Azure Blob Storage

### fsspec (adlfs)
```python
import adlfs

# Account key
fs = adlfs.AzureBlobFileSystem(
    account_name="myaccount",
    account_key="..."
)

# Service principal
fs = adlfs.AzureBlobFileSystem(
    account_name="myaccount",
    tenant_id="...",
    client_id="...",
    client_secret="..."
)

# DefaultAzureCredential (env vars, managed identity, Azure CLI)
fs = adlfs.AzureBlobFileSystem(account_name="myaccount")
```

### PyArrow (via fsspec bridge)
```python
import adlfs
import pyarrow.fs as fs

azure_fs = adlfs.AzureBlobFileSystem(
    account_name="myaccount",
    account_key="..."
)

pa_fs = fs.PyFileSystem(fs.FSSpecHandler(azure_fs))
```

### obstore
```python
from obstore.store import AzureStore

# Account key
azure = AzureStore(
    container="my-container",
    account_name="myaccount",
    account_key="..."
)

# Service principal / DefaultAzureCredential
azure = AzureStore.from_env(container="my-container")
```

---

## Self-Hosted S3 with TLS/SSL

For MinIO/Ceph/SeaweedFS with self-signed certificates, **install a CA bundle** instead of disabling verification.

> ⚠️ Only set `verify=False` for short-lived local dev; it disables TLS verification.

### fsspec / s3fs (boto3)
```python
import s3fs

fs = s3fs.S3FileSystem(
    client_kwargs={
        "endpoint_url": "https://minio.local:9000",
        "region_name": "us-east-1",
        "verify": "/etc/ssl/certs/minio-ca.pem",  # CA bundle
    }
)
```

### PyArrow
```python
import pyarrow.fs as fs

s3 = fs.S3FileSystem(
    endpoint_override="https://minio.local:9000",
    scheme="https",
    region="us-east-1",
    tls_ca_file_path="/etc/ssl/certs/minio-ca.pem",
)
```

### DuckDB
```python
import os
os.environ['AWS_CA_BUNDLE'] = '/etc/ssl/certs/minio-ca.pem'

import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint = 'minio.local:9000';")
con.execute("SET s3_use_ssl = true;")
```

### Env Variable Fallback (SDKs)
```bash
export AWS_CA_BUNDLE=/etc/ssl/certs/minio-ca.pem
export SSL_CERT_FILE=/etc/ssl/certs/minio-ca.pem
```

---

## Environment Variable Reference

Standard cloud credentials:

| Provider | Variables |
|----------|-----------|
| **AWS** | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`, `AWS_CA_BUNDLE`, `AWS_DEFAULT_PROFILE` |
| **GCP** | `GOOGLE_APPLICATION_CREDENTIALS` (path to service account JSON) |
| **Azure** | `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` |

---

## Best Practices

1. ✅ **Never hardcode credentials** - Use environment variables or IAM roles
2. ✅ **Prefer IAM roles/managed identities** for production workloads on cloud VMs
3. ✅ **Use short-lived credentials** (STS, service principals) when possible
4. ✅ **Rotate keys regularly** and audit access
5. ✅ **Use separate credentials** for development vs production
6. ✅ **For local testing**, use MinIO/Azurite or explicitly configured local credentials
7. ❌ **Don't** commit credentials to version control
8. ❌ **Don't** use root account credentials - create scoped IAM users/sa keys
9. ❌ **Don't** disable TLS verification in production (use proper CA management)

---

## References

- `@data-engineering-storage-remote-access/libraries/fsspec` - fsspec auth
- `@data-engineering-storage-remote-access/libraries/pyarrow-fs` - PyArrow auth
- `@data-engineering-storage-remote-access/libraries/obstore` - obstore auth
