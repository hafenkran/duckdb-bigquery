# Authentication & Secrets

You must configure authentication before using the BigQuery extension. The
extension supports [Google Application Default Credentials
(ADC)](https://cloud.google.com/docs/authentication/application-default-credentials)
and project-scoped credentials managed by DuckDB. Common setup paths are:

- **[User Account ADC](#user-account-adc)**<br>
  Create local Application Default Credentials with
  `gcloud auth application-default login`.

- **[Service Account ADC](#service-account-adc)**<br>
  Use a credential file through `GOOGLE_APPLICATION_CREDENTIALS`, or use the
  service account attached to a Google-hosted runtime.

- **[DuckDB Secrets](#duckdb-secrets)**<br>
  Manage project-scoped credentials with per-connection isolation and easy
  rotation, particularly for multi-tenant or server use.

DuckDB secrets take priority when their scope matches the target project. If no
secret matches, the Google client library resolves ADC, including
`GOOGLE_APPLICATION_CREDENTIALS`, local gcloud ADC files, and service accounts
attached to Google-hosted runtimes.

Authentication identifies the caller. The selected identity must also have the
[permissions required](required-permissions.md) by each
operation.

## Option 1: User Account ADC { #user-account-adc }

To authenticate with your Google Account, first install the [Google Cloud
CLI](https://cloud.google.com/sdk/docs/install). Then create local Application
Default Credentials and follow the browser-based login flow:

```bash title="Command line"
gcloud auth application-default login
```

This differs from `gcloud auth login`: the extension uses application-default
credentials, not the Google Cloud CLI's user-session credentials. See the
[`gcloud` documentation](https://cloud.google.com/sdk/gcloud) for additional
CLI configuration.

User account ADC is convenient for local development. It is generally not the
right credential source for unattended production workloads.

## Option 2: Service Account ADC { #service-account-adc }

You can authenticate with a service account by creating the account in Google
Cloud, assigning the necessary roles, and downloading its JSON key file. Set
`GOOGLE_APPLICATION_CREDENTIALS` to the file path before starting DuckDB:

```bash title="Command line"
# Linux and macOS
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Windows Command Prompt
set "GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\service-account.json"
```

Service-account keys are long-lived credentials. Keep the file outside the
repository, restrict access to it, and rotate it according to your
organization's security policy.

### Attached Service Account

On Dataproc, Compute Engine, GKE, Cloud Run, and similar Google-hosted
environments, no service-account key file is required when the workload has an
attached service account. ADC obtains tokens from the metadata server. Make
sure the attached identity has the required IAM permissions and, where
applicable, access scopes.

Prefer an attached service account or Workload Identity Federation over
downloaded keys for deployed workloads.

## Option 3: DuckDB Secrets { #duckdb-secrets }

DuckDB secrets are particularly useful in multi-tenant scenarios where
different credentials must access different BigQuery projects from the same
DuckDB process. Create separate secrets with project-specific `SCOPE` values,
and the extension automatically selects the matching secret for each
operation. The
[DuckDB Secrets Manager](https://duckdb.org/docs/stable/configuration/secrets_manager)
documentation covers secret scopes, storage, inspection, and deletion in more
detail.

The following authentication parameters are currently supported:

- **`ACCESS_TOKEN`** — Temporary OAuth2 access token, obtainable with
  `gcloud auth print-access-token`.
- **`SERVICE_ACCOUNT_PATH`** — Path to a service-account key file.
- **`SERVICE_ACCOUNT_JSON`** — Inline JSON content of a service-account key.
- **`EXTERNAL_ACCOUNT_PATH`** — Path to an external-account credential file
  for Workload Identity Federation.
- **`EXTERNAL_ACCOUNT_JSON`** — Inline external-account JSON for Workload
  Identity Federation.
- **`REFRESH_TOKEN` + `CLIENT_ID` + `CLIENT_SECRET`** — OAuth authorized-user
  credentials. `TOKEN_URI` is optional and defaults to Google's OAuth token
  endpoint.

Use `bq://PROJECT_ID` or `bigquery://PROJECT_ID` as the scope. The following
examples show the supported credential forms:

=== "Access token"

    ```sql
    -- Create a process-local secret with a temporary access token.
    CREATE SECRET bigquery_token (
        TYPE bigquery,
        SCOPE 'bq://my-gcp-project',
        ACCESS_TOKEN 'temporary-access-token'
    );
    ┌─────────┐
    │ Success │
    │ boolean │
    ├─────────┤
    │ true    │
    └─────────┘
    ```

=== "Service-account JSON"

    ```sql
    -- Create a process-local secret with inline service-account JSON.
    CREATE SECRET bigquery_service_account_json (
        TYPE bigquery,
        SCOPE 'bq://my-gcp-project',
        SERVICE_ACCOUNT_JSON '{"type":"service_account", "...":"..."}'
    );
    ┌─────────┐
    │ Success │
    │ boolean │
    ├─────────┤
    │ true    │
    └─────────┘
    ```

=== "External-account file"

    ```sql
    -- Create a persistent secret backed by an external-account file.
    CREATE PERSISTENT SECRET bigquery_external_account (
        TYPE bigquery,
        SCOPE 'bq://my-gcp-project',
        EXTERNAL_ACCOUNT_PATH '/path/to/external-account.json'
    );
    ┌─────────┐
    │ Success │
    │ boolean │
    ├─────────┤
    │ true    │
    └─────────┘
    ```

=== "External-account JSON"

    ```sql
    -- Create a process-local secret with inline external-account JSON.
    CREATE SECRET bigquery_external_account_json (
        TYPE bigquery,
        SCOPE 'bq://my-gcp-project',
        EXTERNAL_ACCOUNT_JSON '{"type":"external_account", "...":"..."}'
    );
    ┌─────────┐
    │ Success │
    │ boolean │
    ├─────────┤
    │ true    │
    └─────────┘
    ```

=== "OAuth refresh token"

    ```sql
    -- Create a process-local secret with OAuth user credentials.
    CREATE SECRET bigquery_oauth (
        TYPE bigquery,
        SCOPE 'bq://my-gcp-project',
        REFRESH_TOKEN 'refresh-token',
        CLIENT_ID 'oauth-client-id',
        CLIENT_SECRET 'oauth-client-secret'
    );
    ┌─────────┐
    │ Success │
    │ boolean │
    ├─────────┤
    │ true    │
    └─────────┘
    ```

By default, `CREATE SECRET` keeps a credential in memory for the lifetime of
the DuckDB instance. Use `CREATE OR REPLACE SECRET` to update it when the
credential changes or expires.

Add `PERSISTENT` when DuckDB should load the credential again in later
sessions. Use `CREATE PERSISTENT SECRET` for a new persistent credential and
`CREATE OR REPLACE PERSISTENT SECRET` to update it. The external-account file
example above demonstrates the persistent form.

Persistent secrets are stored in unencrypted binary form under
`~/.duckdb/stored_secrets` by default. Use the `secret_directory` setting to
choose another location, and protect that directory like any other credential
store.

### Secret Validation

- Credential methods cannot be combined.
- `REFRESH_TOKEN`, `CLIENT_ID`, and `CLIENT_SECRET` must be supplied together.
- `TOKEN_URI` is optional only for the refresh-token method.
- Credential paths must exist and be readable.
- Inline JSON is parsed when the secret is created.
- Secret values are redacted from normal DuckDB secret inspection.

## Authentication Preflight

`ATTACH` immediately checks that the selected credentials can provide an
authentication token. It does not load catalog metadata or verify BigQuery
permissions for datasets, tables, jobs, or Storage APIs. Those checks occur
when the corresponding objects or operations are used.

When `ACCESS_TOKEN` is configured directly, BigQuery may only reject an invalid
or expired token on the first API request.
