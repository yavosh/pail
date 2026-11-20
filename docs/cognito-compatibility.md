# Cognito user pool compatibility

pail supports local user pools with password sign-in and OIDC federation. Both flows issue RS256-signed ID and access tokens.

## Configure a user pool

1. Route each pool's HTTPS hostname to pail's [HTTPS listener](../README.md#https) or a TLS proxy. Preserve the `Host` header at the proxy.
2. Use a certificate that your applications and browsers trust. pail reserves the auth hostname for Cognito requests.
3. Mount a JSON file with the pool configuration. Pass its path with `--cognito-config` or `PAIL_COGNITO_CONFIG`.
4. Set your SDK endpoint to the pool's HTTPS origin. Configure JWT validators with the full `issuer` URL.

This configuration gives the SDK a password client and the browser a public OAuth client.

```json
{
  "pools": [
    {
      "issuer": "https://auth.local.test/us-east-1_local",
      "clients": [
        { "id": "sdk-app" },
        {
          "id": "browser-app",
          "redirect_uris": ["https://app.local.test/callback"],
          "scopes": ["openid", "email"],
          "origins": ["https://app.local.test"]
        }
      ],
      "users": [
        {
          "username": "local-user",
          "password_file": "/run/secrets/local-user-password",
          "claims": { "email": "local-user@example.test" }
        }
      ],
      "provider": {
        "name": "LocalOIDC",
        "issuer": "https://idp.local.test",
        "client_id": "pail-local",
        "client_secret_file": "/run/secrets/idp-client-secret",
        "scopes": ["openid", "email"],
        "claim_map": { "email": "email" }
      }
    }
  ]
}
```

Mount synthetic passwords and client secrets as files. Relative file references resolve from the configuration file's directory.
An SDK client can set `secret_file` to require the Cognito `SECRET_HASH` parameter.
A client with redirect URIs needs `password_auth: true` to also allow password sign-in.
Set a user's optional `subject` to pin their `sub` claim. Otherwise, pail derives it from the issuer and username.
A pool can omit `provider` for password sign-in only.

## Password sign-in

Call `InitiateAuth` with `USER_PASSWORD_AUTH`, `ClientId`, `USERNAME`, and `PASSWORD`.
The AWS JSON API uses `POST /` with the target `AWSCognitoIdentityProviderService.InitiateAuth`.
It authenticates with the supplied user credentials and app client settings. [AWS InitiateAuth API](https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_InitiateAuth.html)

## Browser sign-in

Register `https://auth.local.test/oauth2/idpresponse` as pail's callback at the upstream provider.
Configure the provider to offer OIDC discovery, S256 PKCE, and RS256 ID tokens. Its TLS certificate must be trusted by pail.
The provider must issue ID tokens with pail's registered client ID as the sole audience.
The `claim_map` maps pail claim names to upstream ID token claims.

Use `GET /oauth2/authorize` and `POST /oauth2/token` on the auth hostname.
Register the SPA callback URI and origin in its app client. Request `response_type=code`, include the `openid` scope, and use S256 PKCE.
Use `identity_provider` to select the configured provider. If you omit it, pail uses that provider.
Authorization codes expire, and each code can be redeemed once.

## Token verification and persistence

The issuer serves metadata at `/.well-known/openid-configuration` and public keys at `/.well-known/jwks.json` under the configured issuer path.
Browsers can fetch both documents across origins.
Validate issuer, signature, expiration, `token_use`, and the expected app client. Use `aud` for ID tokens and `client_id` for access tokens.
Both token types expire after one hour. [Cognito token verification](https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-tokens-verifying-a-jwt.html)

Keep the same issuer URL and data directory across restarts to preserve signing keys. Restarting clears in-progress browser sign-ins.
User and app client configuration loads at startup. [Storage layout](storage.md#cognito-signing-keys)

## API coverage

Use a new sign-in to renew an expired session. Cognito refresh, logout, user-info, user management, and identity pool APIs are outside this compatibility surface.
Browser federation supports one OIDC provider per pool. SDK password auth uses startup-configured users.
