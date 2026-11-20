package test

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/yavosh/pail/internal/config"
)

func TestCognitoSDK(t *testing.T) {
	const credential = "synthetic-password"
	dir := t.TempDir()
	configPath := filepath.Join(dir, "pools.json")
	if err := os.WriteFile(filepath.Join(dir, "user-password"), []byte(credential), 0o600); err != nil {
		t.Fatal(err)
	}
	const issuer = "https://auth.example.test/us-east-1_local"
	const pools = `{"pools":[{"issuer":"` + issuer + `","clients":[{"id":"local-client"},` +
		`{"id":"browser-client","redirect_uris":["https://example.test/callback"],"origins":["https://example.test"]}],` +
		`"users":[{"username":"local-user","password_file":"user-password"}],` +
		`"provider":{"name":"local","issuer":"https://upstream.example.test","client_id":"upstream-client"}}]}`
	if err := os.WriteFile(configPath, []byte(pools), 0o600); err != nil {
		t.Fatal(err)
	}
	p := startConfiguredPail(t, config.Config{
		Addr: "127.0.0.1:0", DataDir: t.TempDir(), Region: testRegion,
		AccessKeyID: testKey, SecretAccessKey: testSecret, Domain: "localhost", CognitoConfig: configPath,
	})
	const authEndpoint = "http://auth.example.test"
	c := cognitoidentityprovider.New(cognitoidentityprovider.Options{
		Region: testRegion, BaseEndpoint: new(authEndpoint), HTTPClient: p.httpClient,
		Credentials: aws.AnonymousCredentials{}, RetryMaxAttempts: 1,
	})
	ctx := t.Context()
	for _, tt := range []struct{ name, clientID, password, errorCode string }{
		{"valid password", "local-client", credential, ""},
		{"wrong password", "local-client", "incorrect", "NotAuthorizedException"},
		{"unknown app client", "missing-client", credential, "ResourceNotFoundException"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			out, err := c.InitiateAuth(t.Context(), &cognitoidentityprovider.InitiateAuthInput{
				AuthFlow: types.AuthFlowTypeUserPasswordAuth, ClientId: new(tt.clientID),
				AuthParameters: map[string]string{"USERNAME": "local-user", "PASSWORD": tt.password},
			})
			if tt.errorCode != "" {
				api, ok := errors.AsType[smithy.APIError](err)
				if !ok || api.ErrorCode() != tt.errorCode {
					t.Fatalf("InitiateAuth error = %v, want %s", err, tt.errorCode)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			result := out.AuthenticationResult
			if result == nil || aws.ToString(result.IdToken) == "" || aws.ToString(result.AccessToken) == "" || result.ExpiresIn <= 0 {
				t.Fatal("InitiateAuth returned incomplete authentication tokens")
			}
		})
	}

	for _, resource := range []string{"openid-configuration", "jwks.json"} {
		t.Run("cross-origin "+resource, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, authEndpoint+"/us-east-1_local/.well-known/"+resource, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Origin", "https://example.test")
			resp, err := p.httpClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK || resp.Header.Get("Access-Control-Allow-Origin") != "*" {
				t.Fatalf("GET %s = %d, origin %q, want 200 with public CORS", resource, resp.StatusCode, resp.Header.Get("Access-Control-Allow-Origin"))
			}
			if resource == "openid-configuration" {
				var metadata struct {
					Issuer  string `json:"issuer"`
					JWKSURI string `json:"jwks_uri"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
					t.Fatal(err)
				}
				if metadata.Issuer != issuer || metadata.JWKSURI != issuer+"/.well-known/jwks.json" {
					t.Errorf("discovery = %+v, want issuer and JWKS URI under %s", metadata, issuer)
				}
			}
		})
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, authEndpoint+"/oauth2/authorize", nil)
	if err != nil {
		t.Fatal(err)
	}
	invalid, err := p.httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer invalid.Body.Close()
	if invalid.StatusCode != http.StatusBadRequest || invalid.Header.Get("Content-Type") != "application/json" {
		t.Errorf("authorize without a client = %d %q, want 400 application/json", invalid.StatusCode, invalid.Header.Get("Content-Type"))
	}

	// The path-style S3 bucket and object names match the browser token route.
	s3Client := p.client(styles[0])
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: new("oauth2")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s3Client.PutObject(ctx, &s3.PutObjectInput{Bucket: new("oauth2"), Key: new("token"), Body: strings.NewReader("data")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: new("oauth2"), Key: new("token")}); err != nil {
		t.Fatalf("HeadObject on the S3 endpoint: %v", err)
	}

	options := s3Client.Options()
	options.BaseEndpoint = new(authEndpoint)
	presigned, err := s3.NewPresignClient(s3.New(options)).PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: new("oauth2"), Key: new("token"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{authEndpoint + "/oauth2/token/object", presigned.URL} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := p.httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
			t.Errorf("GET %s on auth hostname = %d %q, want plain 404", req.URL.Path, resp.StatusCode, resp.Header.Get("Content-Type"))
		}
	}
}
