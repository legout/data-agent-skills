# Google Cloud Platform Authentication

GCP authentication for Cloud Storage and other services. Covers service accounts, workload identity federation, and Application Default Credentials.

## Core Concepts

- **Service Account**: Identity for applications, not users. Has email and key pairs.
- **Workload Identity Federation**: Use external identity (GitHub, K8s) to get GCP tokens without keys.
- **Application Default Credentials (ADC)**: Automatic credential discovery chain.
- **OAuth 2.0**: Token-based authentication for user accounts.

## Credential Resolution Order

1. **GOOGLE_APPLICATION_CREDENTIALS** environment variable (path to JSON key)
2. **User credentials** from `gcloud auth application-default login`
3. **Service account key** in well-known location (Compute Engine, GKE, Cloud Run)
4. **Metadata server** (Compute Engine, GKE, Cloud Run, Cloud Functions)
5. **Workload Identity** (if configured)

## Methods

### 1. Service Account Key File (Simple but less secure)

```bash
# Create service account in IAM console
# Download JSON key
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

```python
# Any Google Cloud library auto-detects this
from google.cloud import storage

client = storage.Client()  # Credentials auto-loaded
bucket = client.bucket("my-bucket")
```

**Pros**: Simple, works everywhere
**Cons**: Key can be leaked; manual rotation required; not recommended for production

### 2. Workload Identity Federation (Production Best Practice)

Allow workloads running outside GCP (or in GKE/GCE) to access GCP services **without service account keys**.

```yaml
# Terraform example: create workload identity pool
resource "google_iam_workload_identity_pool" "pool" {
  workload_identity_pool_id = "my-pool"
}

resource "google_iam_workload_identity_pool_provider" "provider" {
  workload_identity_pool_id = google_iam_workload_identity_pool.pool.workload_identity_pool_id
  provider_id = "my-provider"
  attribute_mapping = {
    "google.subject" = "assertion.sub"
    "attribute.actor" = "assertion.actor"
  }
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_binding" "binding" {
  service_account_id = "my-sa@project.iam.gserviceaccount.com"
  role = "roles/iam.workloadIdentityUser"
  members = [
    "principalSet://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/my-pool/attribute.actor/REPO_OWNER/REPO_NAME"
  ]
}
```

Then in GitHub Actions:
```yaml
jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: actions/checkout@v3
      - uses: google-github-actions/auth@v1
        with:
          workload_identity_provider: 'projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/my-pool/providers/my-provider'
          service_account: 'my-sa@project.iam.gserviceaccount.com'
      - run: gsutil cp data.csv gs://my-bucket/
```

**Pros**: No keys to manage; short-lived tokens; audit trail; rotation automatic
**Cons**: Initial setup more complex

### 3. Google Compute Engine Default Service Account

VMs in GCE, GKE nodes, Cloud Run, Cloud Functions automatically have a service account attached.

```python
# No configuration needed - uses VM's service account
from google.cloud import storage

client = storage.Client()  # Picks up credentials from metadata server
```

**Configure:**
- Set default service account at project/instance level
- Grant least-privilege IAM roles to that SA

### 4. Application Default Credentials (ADC)

ADC tries sources in order:

```python
import google.auth

# Explicit load (examines environment, etc.)
credentials, project = google.auth.default()

# Use with any Google Cloud library
from google.cloud import storage
client = storage.Client(credentials=credentials, project=project)
```

**Sources checked:**
1. `GOOGLE_APPLICATION_CREDENTIALS` env var
2. `gcloud auth application-default login` (user account)
3. On GCE/GKE/Cloud Run → metadata server

### 5. Signed URLs (Temporary Access for Non-Google Identities)

Generate time-limited URLs to grant temporary access to objects:

```python
from google.cloud import storage
from datetime import timedelta

client = storage.Client()
bucket = client.bucket("my-bucket")
blob = bucket.blob("data/secret.csv")

# Generate signed URL valid for 1 hour
url = blob.generate_signed_url(
    expiration=timedelta(hours=1),
    method="GET",
    version="v4"
)
print(url)  # Share this URL - no credentials needed to download
```

---

## fsspec / gcsfs Integration

```python
import fsspec

# From environment (ADC or service account key)
fs = fsspec.filesystem('gcs', project='my-project')

# With explicit token
fs = fsspec.filesystem(
    'gcs',
    project='my-project',
    token='/path/to/service-account.json'
)

# Use with pandas/polars
df = pd.read_csv('gs://bucket/data.csv', filesystem=fs)
```

---

## PyArrow Integration

```python
import pyarrow.fs as fs

# Auto-detects GCP credentials via ADC
gcs_fs, path = fs.FileSystem.from_uri("gs://bucket/path/")

# Explicit token (if not using ADC)
gcs_fs = fs.GcsFileSystem(
    access_token='ya29....'  # From service account or OAuth flow
)
```

---

## obstore Integration

```python
from obstore.store import GCSStore

# From environment (GOOGLE_APPLICATION_CREDENTIALS or workload identity)
gcs = GCSStore.from_env(bucket="my-bucket")

# Explicit service account
gcs = GCSStore(
    bucket="my-bucket",
    service_account_path="/path/to/key.json"
)
```

---

## Best Practices

1. ✅ **Use Workload Identity** for production workloads on GCP/GKE/Cloud Run
2. ✅ **Use Workload Identity Federation** for GitHub Actions, external K8s, on-prem
3. ✅ **Never use service account keys** in production code or CI/CD unless absolutely necessary
4. ✅ **Least-privilege IAM roles**: Grant only `storage.objectViewer`, not `storage.admin`
5. ✅ **Rotate keys** every 90 days if you must use them
6. ✅ **Enable audit logs** on Cloud Storage
7. ❌ **Don't** use user credentials (`gcloud auth login`) for production services
8. ❌ **Don't** embed JSON keys in code or Docker images
9. ❌ **Don't** grant `storage.objectAdmin` broadly - scope to specific buckets

---

## Troubleshooting

**"Could not automatically determine credentials"**
- Run `gcloud auth application-default login` for local dev
- Ensure service account key file exists at path in `GOOGLE_APPLICATION_CREDENTIALS`
- On GCE/GKE: ensure scopes include `https://www.googleapis.com/auth/devstorage.read_write`

**"Invalid JSON key file"**
- Key file may be corrupted or wrong format
- Regenerate service account key

**"403 Forbidden"**
- Check IAM permissions on service account
- Ensure bucket exists and SA has `storage.buckets.get` and `storage.objects.*`

---

## References

- [GCP Authentication Overview](https://cloud.google.com/docs/authentication)
- [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation)
- [Application Default Credentials](https://cloud.google.com/docs/authentication/production)
- [Using ADC with gcsfs](https://gcsfs.readthedocs.io/en/latest/index.html?highlight=auth#credentials)
