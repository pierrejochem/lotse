# 44. Sign-ins in progress in a sealed cookie

- Status: Accepted
- Date: 2026-10-29

## Context

Since ADR 0041 a session lives in the browser's cookie and survives a console restart, but a sign-in in progress (the `state` sent to the identity provider, the PKCE verifier and the `nonce`) still lived in the console's memory. A restart while the user was at the provider, or a callback that reached another console behind the same address, made that sign-in fail.

## Decision

**The sign-in travels in a cookie of its own** (maintainer's choice). `/auth/login` sets `lotse_sign_in`: the state, verifier, nonce, return path and start time, sealed with the session key (AES-256-GCM, bound to its purpose, so a session cookie does not open as a sign-in and the other way round). It is `HttpOnly`, `SameSite=Lax` (the callback is a top-level navigation from the provider, so the browser sends it), sent only to `/auth`, and kept for ten minutes. `/auth/callback` finishes the sign-in only if the cookie opens, its state is the one the provider sent back, and it started less than ten minutes ago; the cookie is cleared either way. A console also remembers the states it finished until they would have expired, so a copy of the cookie cannot finish the same sign-in twice there.

## Consequences

- Nothing about a user stays in a console's memory but caches: any console with the session key can start a sign-in and another can finish it.
- A sign-in started in one browser finishes only in that browser, since the cookie is there.
- A copied sign-in cookie could be replayed at another console within its ten minutes; the provider still accepts its code only once.
- The sign-in test now restarts the console between the provider and the callback, and checks a callback without the cookie, with another key's cookie, with a forged state and a second time with a copy; a unit test checks the ten minutes. Each rule (the state, the ten minutes, once per console, the purpose binding, clearing the cookie) was broken on purpose and a test failed.
