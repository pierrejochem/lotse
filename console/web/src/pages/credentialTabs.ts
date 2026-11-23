/** The tabs of People and credentials, in the order the navigation lists them. The URL names the tab, so the sidebar can link to each. */
export const CREDENTIAL_TABS = [
  { id: 'users', label: 'Users', icon: 'users' },
  { id: 'bindings', label: 'Role bindings', icon: 'bindings' },
  { id: 'tokens', label: 'Tokens', icon: 'tokens' },
  { id: 'certificates', label: 'Certificates', icon: 'certificates' },
  { id: 'sessions', label: 'Sessions', icon: 'sessions' },
] as const;
export type CredentialTab = (typeof CREDENTIAL_TABS)[number]['id'];
export const credentialTab = (id: string | undefined) => CREDENTIAL_TABS.find((t) => t.id === (id ?? 'users'));

