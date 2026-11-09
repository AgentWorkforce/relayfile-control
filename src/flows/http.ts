/**
 * Joins relay-control's base URL (an origin, see RELAYCONTROL_BASE_URL) and
 * an absolute route path. Every relay-control client (introspection, the
 * credential broker) builds its URLs here.
 */
export function joinUrl(baseUrl: string, path: string): string {
  return `${baseUrl.replace(/\/+$/, "")}/${path.replace(/^\/+/, "")}`;
}
