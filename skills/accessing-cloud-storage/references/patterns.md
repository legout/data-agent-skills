# Authentication Patterns

Common patterns for managing credentials in data pipelines: secret rotation, multi-environment management, CI/CD integration, and secure local development.

## Table of Contents

1. [Secret Rotation](#secret-rotation)
2. [Multi-Environment Management](#multi-environment-management)
3. [CI/CD Pipeline Authentication](#cicd-pipeline-authentication)
4. [Local Development Without Production Keys](#local-development)
5. [Cross-Cloud / Cross-Account Access](#cross-cloud-access)
6. [Secret Leakage Prevention](#secret-leakage-prevention)

---

## Secret Rotation

Automatically rotate credentials to limit blast radius if leaked.

### AWS Automatic Rotation

Use IAM roles (auto-rotated by AWS) instead of long-term keys. For access keys:

```bash
# Rotate IAM user access key (manual, but can be automated)
aws iam create-access-key --user-name deploy-user
# Update apps with new key
aws iam delete-access-key --user-name deploy-user --access-key-id OLD_KEY
```

Automated rotation via AWS Secrets Manager:
```python
# Lambda function triggered by Secrets Manager rotation
def rotate_secret(event, context):
    secret_arn = event['SecretId']
    token = event['Token']
    step = event['Step']

    if step == "createSecret":
        # Generate new credential
        new_key = iam.create_access_key(UserName='app-user')
        secret_string = json.dumps({
            "AWS_ACCESS_KEY_ID": new_key['AccessKey']['AccessKeyId'],
            "AWS_SECRET_ACCESS_KEY": new_key['AccessKey']['SecretAccessKey']
        })
        secretsmanager.put_secret_value(SecretId=secret_arn, ClientRequestToken=token, SecretString=secret_string)

    elif step == "setSecret":
        # Test new credential works
        pass

    elif step == "testSecret":
        # Verify app can use it
        pass

    elif step == "finishSecret":
        # Mark rotation complete, old secret scheduled for deletion
        pass
```

### GCP Service Account Key Rotation

Prefer Workload Identity (no rotation needed). If using keys:

```bash
# Create new key
gcloud iam service-accounts keys create new-key.json --iam-account=my-sa@project.iam.gserviceaccount.com

# Update applications to use new key

# Delete old key
gcloud iam service-accounts keys delete OLD_KEY_ID --iam-account=my-sa@project.iam.gserviceaccount.com
```

Automate with Cloud Scheduler + Cloud Functions.

### Azure Key Rotation

For storage account keys:

```bash
# Regenerate key (key1/key2)
az storage account keys renew --account-name myaccount --key-name key1
```

Use SAS tokens with short expiry instead of account keys for client apps.

---

## Multi-Environment Management

Separate credentials for dev/staging/prod.

### Principle

- **Never share credentials** across environments
- Use separate IAM users/service principals per environment
- Environmental variable naming: `AWS_ACCESS_KEY_ID_DEV`, `AWS_ACCESS_KEY_ID_PROD`

### Implementation

```python
import os

ENV = os.getenv("APP_ENV", "dev")  # dev, staging, prod

if ENV == "dev":
    os.environ["AWS_PROFILE"] = "myapp-dev"
    s3_bucket = "myapp-dev-bucket"
elif ENV == "prod":
    # Use IAM role (no credentials in env)
    s3_bucket = "myapp-prod-bucket"
```

**Terraform example:**
```hcl
# Separate state per environment
terraform {
  backend "s3" {
    bucket = "myapp-terraform-state"
    key    = "${terraform.workspace}/state.tfstate"
  }
}

provider "aws" {
  region = "us-east-1"
  # Assume role based on workspace
  assume_role {
    role_arn = "arn:aws:iam::${var.account_id}:role/myapp-${terraform.workspace}"
  }
}
```

---

## CI/CD Pipeline Authentication

### GitHub Actions

**AWS:**
```yaml
jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      id-token: write   # For OIDC
      contents: read
    steps:
      - name: Configure AWS Credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::123456789012:role/github-actions-deploy
          aws-region: us-east-1

      - name: Deploy to S3
        run: aws s3 sync dist/ s3://my-bucket/
```

**GCP:**
```yaml
- uses: google-github-actions/auth@v1
  with:
    workload_identity_provider: 'projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/my-pool/providers/my-provider'
    service_account: 'my-sa@project.iam.gserviceaccount.com'

- run: gsutil cp dist/* gs://my-bucket/
```

**Azure:**
```yaml
- name: Azure Login
  uses: azure/login@v2
  with:
    client-id: ${{ secrets.AZURE_CLIENT_ID }}
    tenant-id: ${{ secrets.AZURE_TENANT_ID }}
    client-secret: ${{ secrets.AZURE_CLIENT_SECRET }}

- run: az storage blob upload-batch -d mycontainer -s dist/
```

### GitLab CI

```yaml
variables:
  AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
  AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

---

## Local Development

### Without Production Credentials

1. **Use local emulators**:
   - MinIO for S3
   - Azurite for Azure Blob
   - LocalStack for full AWS stack

```bash
# Start MinIO
docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address :9001

# Configure AWS CLI
aws configure set endpoint-url http://localhost:9000
aws configure set aws_access_key_id minioadmin
aws configure set aws_secret_access_key minioadmin
```

2. **Use separate dev IAM user** with read-only access to dev bucket
3. **Use `aws configure` with dev profile**, never prod
4. **Never commit `.env` files** - add to `.gitignore`

### Example `.env` (git-ignored)
```bash
# .env.development (never commit)
AWS_PROFILE=myapp-dev
GOOGLE_APPLICATION_CREDENTIALS=./secrets/gcp-dev-key.json
```

---

## Cross-Cloud / Cross-Account Access

### AWS Cross-Account Role

```python
# Assume role in another account
sts_client = boto3.client('sts')
assumed_role = sts_client.assume_role(
    RoleArn='arn:aws:iam::PROD_ACCOUNT:role/read-only',
    RoleSessionName='cross-account-session'
)

s3 = boto3.client(
    's3',
    aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
    aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
    aws_session_token=assumed_role['Credentials']['SessionToken']
)
```

### GCP Workload Identity Federation

See `gcp.md` - allows non-GCP identities (GitHub, K8s) to impersonate service account without keys.

---

## Secret Leakage Prevention

### Pre-commit Hooks

```bash
# .git/hooks/pre-commit or use pre-commit framework
#!/bin/sh
if git diff --cached | grep -q 'AWS_SECRET_ACCESS_KEY'; then
  echo "ERROR: Secret key staged!"
  exit 1
fi
```

**Use `detect-secrets`**:
```bash
pip install detect-secrets
detect-secrets scan > .secrets.baseline
detect-secrets-hook --baseline .secrets.baseline
```

### GitHub Secret Scanning

GitHub automatically scans for known secret patterns. Enable alerts:
- Settings → Code security and analysis → Secret scanning

### Rotate Immediately If Leaked

If a key is accidentally committed:
1. Revoke/rotate the key immediately
2. Invalidate all sessions using it
3. Check audit logs for unauthorized access
4. Commit a new key (if needed) via secure channel

---

## Testing with Mock Credentials

### Moto (AWS mocking)

```python
from moto import mock_s3
import boto3

@mock_s3
def test_s3_upload():
    # Creates fake S3 in memory
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test-bucket')
    # Test without real credentials
```

### Google Cloud Test Utils

```python
from google.cloud import storage
from unittest.mock import patch

@patch('google.cloud.storage.Client')
def test_storage(mock_client):
    # Mock GCS client
    pass
```

### LocalStack

Run local AWS services in Docker:
```bash
docker run -p 4566:4566 localstack/localstack
```

Configure SDKs to use `localhost:4566` endpoints.

---

## References

- [AWS Secret Rotation](https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotating-secrets.html)
- [GCP Workload Identity](https://cloud.google.com/iam/docs/workload-identity-federation)
- [Azure Managed Identities](https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview)
- [detect-secrets](https://github.com/Yelp/detect-secrets)
