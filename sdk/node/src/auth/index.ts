export { authenticate, type Exchange, makeNonce } from "./authenticator.js";
export {
  type Credentials,
  mechanismOf,
  OAUTHBEARER_MECHANISM,
  type OAuthBearerCredentials,
  oauthBearerCredentials,
  oauthBearerInitialResponse,
  SCRAM_MECHANISM,
  type ScramCredentials,
  scramCredentials,
} from "./credentials.js";
export { AuthenticationError, ScramError, type ScramErrorKind } from "./errors.js";
export { MAX_ITERATIONS, MIN_ITERATIONS, ScramClient } from "./scram-client.js";
export { SALT_BYTES, ScramCredential } from "./scram-credential.js";
