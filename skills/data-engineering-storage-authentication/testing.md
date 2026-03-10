# Authentication Testing Strategies

Testing data pipelines that require cloud credentials: unit tests with mocks, integration tests with local emulators, and test credential management.

## Challenges

- **Security**: Never use production credentials in tests
- **Flakiness**: Network calls to cloud services can fail/timeout
- **Cost**: Cloud API calls may incur costs
- **Speed**: External service calls slow down test suite

## Solutions by Test Type

### 1. Unit Tests (Mock Everything)

Use libraries that mock cloud SDKs.

#### AWS with Moto

```python
from moto import mock_s3, mock_sts
import boto3
import pytest

@mock_s3
def test_s3_upload():
    # Creates in-memory fake S3
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test-bucket')

    s3.put_object(Bucket='test-bucket', Key='data.parquet', Body=b'fake data')

    obj = s3.get_object(Bucket='test-bucket', Key='data.parquet')
    assert obj['Body'].read() == b'fake data'

@mock_sts
def test_assume_role():
    sts = boto3.client('sts')
    resp = sts.assume_role(
        RoleArn='arn:aws:iam::123456789012:role/TestRole',
        RoleSessionName='test'
    )
    assert 'Credentials' in resp
```

#### Azure with unittest.mock

```python
from unittest.mock import patch, MagicMock
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

@patch('azure.identity.DefaultAzureCredential')
def test_blob_upload(mock_credential):
    mock_credential.return_value.get_token.return_value = MagicMock(token="fake-token")

    # Mock the BlobServiceClient
    with patch('azure.storage.blob.BlobServiceClient') as mock_client:
        mock_blob = MagicMock()
        mock_client.return_value.get_blob_client.return_value = mock_blob

        # Your code here
        upload_to_blob("my-container", "data.csv", b"csv data")

        mock_blob.upload_blob.assert_called_once()
```

#### GCP with google-cloud-testutils

```python
from google.cloud import storage
from unittest import mock

@mock.patch('google.cloud.storage.Client')
def test_gcs_download(mock_client_class):
    mock_client = mock.MagicMock()
    mock_client_class.return_value = mock_client

    mock_bucket = mock.MagicMock()
    mock_client.bucket.return_value = mock_bucket

    mock_blob = mock.MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_blob.download_as_bytes.return_value = b"data"

    result = download_from_gcs("my-bucket", "data.csv")
    assert result == b"data"
```

### 2. Integration Tests with Local Emulators

Use Docker containers that emulate cloud services.

#### MinIO (S3)

```bash
docker run -d -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address :9001
```

```python
import boto3
import os

def test_s3_integration():
    # Configure boto3 for MinIO
    os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
    os.environ['AWS_CA_BUNDLE'] = ''  # Disable cert verification for local dev

    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        region_name='us-east-1'
    )

    s3.create_bucket(Bucket='test-bucket')
    s3.put_object(Bucket='test-bucket', Key='test.parquet', Body=b'data')

    # Verify
    resp = s3.list_objects_v2(Bucket='test-bucket')
    assert len(resp['Contents']) == 1
```

#### Azurite (Azure Blob)

```bash
docker run -p 10000:10000 -p 10001:10001 mcr.microsoft.com/azure-storage/azurite
```

```python
from azure.storage.blob import BlobServiceClient

def test_azure_integration():
    conn_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFe...;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
    client = BlobServiceClient.from_connection_string(conn_str)

    container = client.create_container("test-container")
    container.upload_blob(name="test.txt", data=b"hello")
    blobs = container.list_blobs()
    assert len(list(blobs)) == 1
```

#### LocalStack (Full AWS)

```bash
docker run -p 4566:4566 -p 4572:4572 localstack/localstack
```

```python
import boto3

def test_localstack():
    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        region_name='us-east-1'
    )
    # Works identically to real AWS
```

#### GCP Emulators

Google provides Cloud Storage emulator via `gcloud`:

```bash
gcloud beta emulators storage start --host-port=localhost:9090
```

```python
from google.cloud import storage

def test_gcs_emulator():
    os.environ['STORAGE_EMULATOR_HOST'] = 'http://localhost:9090'
    client = storage.Client()  # Connects to emulator
    bucket = client.bucket('test-bucket')
    bucket.create()
    blob = bucket.blob('test.txt')
    blob.upload_from_string('hello')
```

### 3. Test Credential Management

#### Use Dedicated Test IAM Resources

Create separate IAM users/service principals for CI/CD with **least-privilege** permissions:

```yaml
# Terraform: minimal test IAM user
resource "aws_iam_user" "test_runner" {
  name = "ci-test-runner"
}

resource "aws_iam_user_policy" "test_s3_access" {
  name = "TestS3ReadWrite"
  user = aws_iam_user.test_runner.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject"
        ]
        Resource = "arn:aws:s3:::test-bucket/*"
      }
    ]
  })
}
```

Store credentials in CI/CD secrets vault (GitHub Secrets, GitLab CI variables, Jenkins credentials).

#### Use Environment-Specific Test Buckets

```python
import os

TEST_BUCKET = os.getenv('TEST_BUCKET', 'myapp-test-12345')

def test_upload():
    # Upload to test bucket (never prod)
    s3.upload_file('local.csv', TEST_BUCKET, 'test-data.csv')
```

#### Never Use Production Credentials in Tests

- **Enforce** via pre-commit hooks and CI scanners
- **Separate** IAM users for dev/test/prod
- **Rotate** test credentials regularly (90 days)

---

## Example Test Suite Structure

```
tests/
├── unit/
│   ├── test_s3_client.py      # Moto mocks
│   ├── test_gcs_client.py     # Mock patches
│   └── test_blob_client.py    # Mock patches
├── integration/
│   ├── test_s3_minio.py       # MinIO container
│   ├── test_azure_azurite.py  # Azurite container
│   └── conftest.py            # Pytest fixtures (start/stop containers)
├── fixtures/
│   ├── sample_data.parquet
│   └── test_config.json
└── helpers/
    └── mock_cloud.py          # Shared mock helpers
```

**conftest.py** (pytest fixtures):
```python
import pytest
import subprocess
import time
import boto3

@pytest.fixture(scope="session")
def minio_container():
    # Start MinIO Docker container for entire test session
    subprocess.run(["docker", "run", "-d", "-p", "9000:9000", "...minio/minio", "server", "/data"])
    time.sleep(5)  # Wait for startup
    yield
    subprocess.run(["docker", "stop", "minio-container"])
    subprocess.run(["docker", "rm", "minio-container"])

@pytest.fixture
def s3_client(minio_container):
    client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1'
    )
    client.create_bucket(Bucket='test-bucket')
    yield client
```

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      minio:
        image: minio/minio
        env:
          MINIO_ROOT_USER: minioadmin
          MINIO_ROOT_PASSWORD: minioadmin
        options: >-
          --health-cmd "curl -f http://localhost:9000/minio/health/live"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 9000:9000

    steps:
      - uses: actions/checkout@v3

      - name: Wait for MinIO
        run: |
          for i in {1..10}; do
            curl -s http://localhost:9000/minio/health/live && break
            sleep 2
          done

      - name: Unit tests (AWS mocks)
        run: |
          pytest tests/unit --cov=data_pipeline

      - name: Integration tests (MinIO)
        env:
          AWS_ACCESS_KEY_ID: minioadmin
          AWS_SECRET_ACCESS_KEY: minioadmin
          AWS_ENDPOINT_URL: http://localhost:9000
        run: |
          pytest tests/integration/test_s3_minio.py
```

---

## Best Practices

1. ✅ **Unit tests**: Mock all cloud SDKs (moto, patch)
2. ✅ **Integration tests**: Use local emulators (MinIO, Azurite) in Docker
3. ✅ **CI/CD**: Spin up emulators as services (GitHub Actions services, GitLab services)
4. ✅ **Separate test credentials** from dev/prod; minimal permissions
5. ✅ **Test credential rotation** in staging environment (auto-rotate keys)
6. ❌ **Don't** call real cloud services in unit tests
7. ❌ **Don't** use production credentials in CI, even read-only
8. ❌ **Don't** commit any credentials (even test keys) to version control

---

## References

- [Moto Documentation](https://docs.getmoto.org/)
- [MinIO Quickstart](https://min.io/docs/minio/container/index.html)
- [Azurite GitHub](https://github.com/Azure/Azurite)
- [LocalStack](https://localstack.cloud/)
- `@data-engineering-storage-authentication/patterns.md` - Secret rotation patterns
