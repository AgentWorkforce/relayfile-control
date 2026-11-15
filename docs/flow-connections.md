# Flow connection management

The dashboard's **Flows → Connections** view lists the workspace's provider
connections, their authentication method and status, and allows a confirmed
disconnect. Connections created by the CLI and just-in-time flow setup appear
in the same view. Credentials are never displayed.

Provider connection management uses relay-control's existing workspace APIs:

- `GET /cloud/api/v1/workspaces/{workspaceId}/integrations/providers` supplies
  non-secret connection details and `canManage`.
- `DELETE /cloud/api/v1/workspaces/{workspaceId}/integrations/{provider}`
  disconnects a provider with relay-control's existing owner/admin authorization.
- `POST /cloud/api/v1/workspaces/{workspaceId}/integrations/connect-session`
  starts native OAuth in self-hosted installs. API-key providers use the existing
  provider `POST` with the catalog's `credentialFields`; secrets are never
  returned to the dashboard.
- `PATCH /cloud/api/v1/workspaces/{workspaceId}/integrations/{provider}/settings`
  updates only the connection display name. It requires the selected
  `connectionId`, as does the dashboard's disconnect request, so an old screen
  cannot edit or disconnect a replacement account. Native records also expose
  an opaque `version` edit token: reconnects retain their row ID, so the UI sends
  both the ID and version for conditional writes under a row lock. Renaming does not change
  provider permissions, tokens or service-account scope identifiers.

Hosted Cloud dispatches setup to the selected Nango or Composio backend and
looks up/deletes existing connections through their persisted backend identity.
Authorization health is distinct from ingestion readiness: missing, inactive
and unknown (including provider outages) are separate outcomes. Provider-specific
permission editors are not a general metadata editor; unsupported scopes are
rejected rather than silently saved to a default account. The local design
preview remains an in-memory playground, separate from these real API paths.

These requests must reach relay-control. The `/api/v1/flows`, `/api/v1/workers`
and `/api/v1/workflows` families continue to reach relayfile-control. This UI
change does not require an ingress cutover or a new service credential.

`integration_bindings` in this service records webhook delivery routing. Its
`active` state means routing is enabled; it does not establish that OAuth is
valid or a provider token still works. Bindings use Relayfile `rw_…` workspace
IDs; Flows user introspection returns relay-control workspace IDs. Do not join
them by assuming those identifiers match, expose bindings as credential health,
or delete a delivery binding as a substitute for revoking a provider connection.

Existing runs may already hold issued credentials. Disconnect does not cancel
those runs. A subsequent run that needs the connection must connect again.

Cloud settings and disconnect requests identify the selected persisted row and
connection ID. Conditional local deletion preserves a replacement installed
while provider cleanup is in flight; the UI reloads the surviving connection.
