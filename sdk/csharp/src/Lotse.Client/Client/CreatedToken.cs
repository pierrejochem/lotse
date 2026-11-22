namespace Lotse.Client;

/// <summary>An access token just created (ADR 0133), from <see cref="LotseClient.CreateTokenAsync"/>.</summary>
/// <param name="Id">Names the token; listed with <see cref="LotseClient.DescribeTokensAsync"/> and revoked with <see cref="LotseClient.RevokeTokenAsync"/>.</param>
/// <param name="Text">The token's text, <c>lotse_tok_...</c>: shown once, since the cluster keeps only the hash of its
/// secret; sign in with it through <see cref="Lotse.Auth.Credentials.Token"/>.</param>
/// <param name="ExpiresAtMs">When the token stops being valid, in milliseconds since the epoch.</param>
public sealed record CreatedToken(ulong Id, string Text, ulong ExpiresAtMs)
{
    /// <summary>Names the id and the end only: the text is the secret and stays out of logs.</summary>
    public override string ToString() => $"CreatedToken[id={Id}, expiresAtMs={ExpiresAtMs}]";
}
