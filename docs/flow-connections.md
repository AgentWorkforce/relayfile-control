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
