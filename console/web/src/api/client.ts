import createClient from 'openapi-fetch';
import type { components, paths } from './schema';

/** Every schema of the console API, generated from lotse-console's OpenAPI document. */
export type S = components['schemas'];
export type Tone = S['Tone'];

/** Typed client; paths and bodies are checked against the OpenAPI document. */
export const api = createClient<paths>({ baseUrl: '' });

/** A failed request, with the console's error code and message. */
export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

type Result<T> = { data?: T; error?: unknown; response: Response };

/** Returns the data or throws an ApiError with the server's message. */
export async function unwrap<T>(call: Promise<Result<T>>): Promise<T> {
  let res: Result<T>;
  try {
    res = await call;
  } catch {
    throw new ApiError(0, 'NETWORK', 'The console cannot be reached. Check your connection and try again.');
  }
  if (res.data !== undefined && res.response.ok) return res.data;
  if (res.response.status === 401) signIn();
  const body = res.error as Partial<S['ErrorBody']> | undefined;
  throw new ApiError(
    res.response.status,
    body?.code ?? 'HTTP_' + res.response.status,
    body?.message ?? `The console answered ${res.response.status}.`,
  );
}

/** Sends the browser to sign in, returning here afterwards. */
export function signIn(): void {
  if (typeof window === 'undefined') return;
  const here = window.location.pathname + window.location.search;
  window.location.assign('/auth/login?return_to=' + encodeURIComponent(here));
}

/** Ends the session: a form POST, so the console can redirect to the identity provider. */
export function signOut(): void {
  const form = document.createElement('form');
  form.method = 'post';
  form.action = '/auth/logout';
  document.body.appendChild(form);
  form.submit();
}
