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
- `data-engineering-storage-remote-access-libraries-fsspec` - fsspec S3 configuration
- `data-engineering-storage-remote-access-libraries-pyarrow-fs` - PyArrow S3 integration
- `data-engineering-storage-remote-access-libraries-obstore` - obstore S3 usage
