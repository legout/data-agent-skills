# Azure Blob Storage Authentication

Azure authentication patterns for Blob Storage and other Azure services. Covers Managed Identities, Service Principals, Shared Access Signatures (SAS), and Azure AD integration.

## Core Concepts

- **Managed Identity**: Azure-managed identity for Azure resources (VMs, AKS, App Service). No secrets to manage.
- **Service Principal**: Application identity in Azure AD with client secret or certificate.
- **SAS Token**: Time-limited token with specific permissions (read, write, delete).
- **Azure AD**: Identity provider for users, groups, service principals.
- **DefaultAzureCredential**: Chain of credential providers that automatically tries multiple auth methods.

## Credential Resolution Order (DefaultAzureCredential)

`DefaultAzureCredential` tries these sources in order:

1. **EnvironmentCredential** - From env vars (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, etc.)
2. **ManagedIdentityCredential** - From Azure resource's managed identity
3. **SharedTokenCacheCredential** - From Azure CLI or Visual Studio
4. **VisualStudioCredential** - From Visual Studio
5. **AzureCliCredential** - From `az login`

## Methods

### 1. Managed Identity (Production Best Practice)

Attach a managed identity to your Azure resource (VM, App Service, AKS, Function).

```python
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

credential = DefaultAzureCredential()  # Auto-detects managed identity
client = BlobServiceClient(
    account_url="https://myaccount.blob.core.windows.net",
    credential=credential
)

container = client.get_container_client("my-container")
blobs = container.list_blobs()
```

**Setup:**
- Enable system-assigned or user-assigned managed identity on VM/App Service
- Grant IAM role on storage account: `Storage Blob Data Contributor` or finer-grained
- No credentials in code or configuration

**User-assigned identity:**
```python
credential = ManagedIdentityCredential(client_id="<user-assigned-client-id>")
```

### 2. Service Principal with Client Secret

For CI/CD, external apps, or when managed identity not available.

```python
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

credential = ClientSecretCredential(
    tenant_id="<tenant-id>",
    client_id="<app-registration-client-id>",
    client_secret="<client-secret>"
)

client = BlobServiceClient(
    account_url="https://myaccount.blob.core.windows.net",
    credential=credential
)
```

**Create service principal:**
```bash
# Azure CLI
az ad sp create-for-rbac --name "my-app" --skip-assignment
# Output includes: appId (client_id), password (client_secret), tenant
```

**Grant RBAC:**
```bash
az role assignment create \
  --assignee <client-id> \
  --role "Storage Blob Data Contributor" \
  --scope /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account>
```

### 3. Service Principal with Certificate (More Secure)

```python
from azure.identity import CertificateCredential

credential = CertificateCredential(
    tenant_id="<tenant-id>",
    client_id="<client-id>",
    certificate_path="/path/to/cert.pem"
)
```

### 4. Shared Access Signature (SAS) Token

Generate time-limited, permission-scoped tokens for temporary access.

```python
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta

# Generate SAS token
sas_token = generate_blob_sas(
    account_name="myaccount",
    container_name="my-container",
    blob_name="data.csv",
    account_key="<storage-account-key>",  # Keep this secure!
    permission=BlobSasPermissions(read=True),
    expiry=datetime.utcnow() + timedelta(hours=1)
)

# Use SAS token in URL
url = f"https://myaccount.blob.core.windows.net/my-container/data.csv?{sas_token}"
client = BlobServiceClient(account_url="https://myaccount.blob.core.windows.net", credential=sas_token)

# Or pass just the SAS token (for specific container)
client = BlobServiceClient(
    account_url="https://myaccount.blob.core.windows.net",
    credential={"account_name": "myaccount", "sas_token": sas_token}
)
```

### 5. Environment Variables (Development)

```bash
export AZURE_STORAGE_ACCOUNT="myaccount"
export AZURE_STORAGE_KEY="base64-encoded-account-key"
# Or for service principal:
export AZURE_TENANT_ID="<tenant-id>"
export AZURE_CLIENT_ID="<client-id>"
export AZURE_CLIENT_SECRET="<client-secret>"
```

```python
# fsspec / adlfs
import adlfs

fs = adlfs.AzureBlobFileSystem(
    account_name="myaccount",  # From AZURE_STORAGE_ACCOUNT
    # key auto-detected from AZURE_STORAGE_KEY
)

# Or explicit
fs = adlfs.AzureBlobFileSystem(
    account_name="myaccount",
    account_key="..."
)
```

### 6. Connection String

```python
from azure.storage.blob import BlobServiceClient

conn_str = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=...;EndpointSuffix=core.windows.net"
client = BlobServiceClient.from_connection_string(conn_str)
```

---

## fsspec / adlfs Integration

```python
import adlfs
import fsspec

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

# DefaultAzureCredential (managed identity, CLI, etc.)
fs = adlfs.AzureBlobFileSystem(account_name="myaccount")
# Credentials auto-detected via DefaultAzureCredential chain

# Use with pandas/polars
df = pd.read_csv("abfs://my-container@myaccount.dfs.core.windows.net/data.csv", filesystem=fs)
```

**Note**: ADLS Gen2 uses `abfs://` protocol: `abfs://container@account.dfs.core.windows.net/path`

---

## PyArrow Integration (via fsspec)

```python
import adlfs
import pyarrow.fs as fs

# Create adlfs filesystem
azure_fs = adlfs.AzureBlobFileSystem(
    account_name="myaccount",
    account_key="..."
)

# Wrap for PyArrow
pa_fs = fs.PyFileSystem(fs.FSSpecHandler(azure_fs))

# Use with Parquet
table = pq.read_table(
    "mycontainer/data.parquet",
    filesystem=pa_fs
)
```

---

## obstore Integration

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
# Uses AZURE_STORAGE_ACCOUNT, AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
```

---

## IAM Roles for Blob Storage

Grant appropriate RBAC roles at storage account, container, or blob level:

| Role | Permissions |
|------|-------------|
| **Storage Blob Data Owner** | Full control (read, write, delete, modify ACLs) |
| **Storage Blob Data Contributor** | Read/write/delete but cannot manage access |
| **Storage Blob Data Reader** | Read-only |
| **Storage Queue Data Contributor** | For queues (not blobs) |

**Principle of least privilege**: Grant `Storage Blob Data Reader` to analytics service, `Contributor` only to ETL writers.

---

## Best Practices

1. ✅ **Use Managed Identity** for Azure resources (VMs, App Service, AKS)
2. ✅ **Use Workload Identity** for GitHub Actions / external K8s (Azure AD workload identity)
3. ✅ **Rotate keys** every 90 days if using account keys
4. ✅ **Grant least-privilege RBAC**: `Storage Blob Data Reader` over `Contributor`
5. ✅ **Enable logging**: Azure Storage Analytics logs, Azure Monitor
6. ✅ **Use SAS tokens** for temporary external sharing (time-limited, scoped)
7. ❌ **Don't** use account keys in application code - treat them like passwords
8. ❌ **Don't** grant `Owner` or `Contributor` at subscription level to apps
9. ❌ **Don't** use storage account keys for user-facing apps (use SAS)

---

## Troubleshooting

**"AuthenticationFailed" / "403 Forbidden"**
- Check that service principal has RBAC role on storage account
- Ensure tenant ID matches the Azure AD directory
- Verify client secret hasn't expired

**"Local device not found" with DefaultAzureCredential**
- Means no managed identity and not logged in via `az login`
- Run `az login` for local dev or assign managed identity

**"The specified resource does not exist"**
- Storage account name incorrect or doesn't exist
- Container doesn't exist (create it first)
- Using wrong endpoint (check `account_url` format)

---

## References

- [Azure Identity Library](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme)
- [Azure Storage Blob SDK](https://learn.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme)
- [Workload Identity with GitHub Actions](https://learn.microsoft.com/en-us/azure/developer/github/connect-from-azure)
- [Managed Identities for Azure Resources](https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview)
- [adlfs Documentation](https://github.com/fsspec/adlfs)
