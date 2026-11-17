# 119. Users request certificates in their own name

- Status: Accepted
- Date: 2026-11-17

## Context

[ADR 0092](0092-requesting-and-signing-client-certificates.md) lets a cluster admin ask for a client certificate in the console: the browser makes the key and a signing request, the cluster keeps the request, and the CA holder signs it offline with `lotse certs --sign`. Only cluster admins may ask, so a user who wants a certificate for themselves has to find an admin. Nothing tells the CA holder that a request waits: they learn of it only by running `lotse certs --sign`. The README listed both as gaps.

Two facts shape the change:

- A certificate signs in with the user's name and no IdP groups (`client/session.rs`). A user whose roles all come from a group at the identity provider would get a certificate that grants nothing.
- Requests are capped at 256 for the whole cluster. If any user may ask, users could fill the cap and leave cluster admins no room.

## Decision

**Any signed-in user may ask for a client certificate in their own name, with one pending request at a time. The cluster and every node say how many requests wait for the CA holder** (maintainer's choices: own name plus a waiting signal; any signed-in user; one pending each with room kept for admins; a new request replaces the pending one).

- **Who asks.** `RequestCertificate` (key 55) is allowed for any user, not only for cluster admins.
  - A user who is not a cluster admin may name only themselves as the principal; any other name is `UNAUTHORIZED`.
  - Anonymous clients and nodes are treated as before.
  - A name the CA holder could not sign (ADR 0092's characters) is `INVALID_REQUEST`, with the reason. An OIDC subject such as `oidc|bob` is one.
  - The CA holder still decides: nothing is signed by itself, and the CA key stays offline.
- **One pending request each.** Such a request is written as a new metadata change, `RequestOwnCertificate` (tag 27), which has no separate "asked by".
  - When it applies, it drops the user's pending request in their own name, if there is one, and enters the new one.
  - It does not drop a signed request, whose certificate is still there to fetch, or one an admin asked for in the user's name.
  - Asking again, for example after losing the key file, therefore needs no withdraw call.
  - The rule is applied in every replica, so two requests sent together still leave one.
- **Room for admins.** Requests a user made in their own name (principal equal to who asked) stop at 192 of the 256 places, so cluster admins always find at least 64.
  - The user's request that the new one replaces does not count.
  - The node that leads the metadata group checks both caps; requests from cluster admins check only the 256.
- **Who sees what.** `DescribeCertificateRequests` (key 56) gives a cluster admin every request and anyone else only the requests they asked for. A user does not see a request an admin made in their name.
- **The waiting signal.** The operator's endpoint (ADR 0117) gains the gauge `lotse_certificate_requests_pending`: requests that are unsigned and have not lapsed, as this node's replica of the metadata group has applied them.
  - Every node reports it, so an alert takes the maximum. For example, `max(lotse_certificate_requests_pending) > 0` held for an hour.
  - The console's *Certificates* tab shows the same count to cluster admins, and the request list says how many wait.
- **Console.** A user who is not a cluster admin sees their own requests and can ask with the principal fixed to their own name.
  - The dialog says that a new request replaces the pending one, whose key file will not match.
  - When no role is bound to the user's name itself, the page says that the certificate would grant nothing until a cluster admin binds one, because a certificate signs in without IdP groups.
  - `CertificateRequestsView` gains `all`, `pending`, `principal` and `bound_by_name`.

The protocol's messages do not change, only who may send them and what they answer. The Rust client and the Java, C# and Node.js SDKs keep their calls; their documentation now says who may use them.

Rejected options:

- **Only users with a role bound to their name.** That avoids certificates that grant nothing, but users whose roles all come from IdP groups could never ask. The console warns instead, and the CA holder may decline.
- **A new role for requesting certificates.** More control, but more setup for admins and a seventh role.
- **Renewal over a certificate connection.** A user could renew with the certificate they hold, without signing in another way. It is left for later: an expiring certificate can be replaced by a new request today.
- **An explicit withdraw call.** One more request key and SDK call for what replacing on a new request already covers.
- **Refusing a second request until the first lapses.** A user who lost the key would wait up to seven days or need an admin.
- **One shared cap only.** Many users together could fill the 256 places and block admins until requests lapse.
- **Notifying the CA holder by mail or webhook.** The cluster would need settings for an outside service. A gauge fits the alerting operators already have (ADR 0117).

## Consequences

- A user gets a certificate for themselves without a cluster admin: they ask in the console, the CA holder signs, and they download it from their list.
- The CA holder learns of waiting requests from their alerts or from the console instead of running `lotse certs --sign` on the off chance.
- A certificate grants only the roles bound to the user's name. The console says so before the user asks.
- The CA holder now sees requests from many users. Requests carry who asked; `lotse certs --sign` shows it and signs nothing without confirmation.
- Tests:
  - A metadata test covers the new change: replacing only the asker's pending request in their own name, never a signed one or an admin's; a repeat that changes nothing; the snapshot and the record round trip; and what counts as pending.
  - A simulation in `tests/auth.rs` covers:
    - asking in one's own name and being refused another;
    - seeing only one's own requests, not an admin's in one's name;
    - replacing on asking again;
    - refusing a name the CA holder cannot sign;
    - the 192 cap, with a replacing request still allowed at it;
    - room left for an admin;
    - every node's count for the operator.
  - The ADR 0092 simulation now has a user refused when all 256 places are full.
  - The console's sign-in test against a real node covers a user who asks for themselves, sees only theirs and is told no role is bound to their name, and an admin who sees the waiting count.
  - The render test covers the gauge.
- Mutation-checked, each broken on purpose until a test failed:
  - not replacing the pending request;
  - replacing another user's request;
  - replacing a signed one;
  - no cap of 192;
  - counting the replaced request against the cap;
  - no cap of 256 for users;
  - listing every request to a user;
  - letting a user ask in another name;
  - sending a cluster admin's request through the user path;
  - counting signed or lapsed requests as pending.
