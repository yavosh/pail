package test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/smithy-go"
)

func (p *pail) kmsClient() *kms.Client {
	return kms.New(kms.Options{
		BaseEndpoint: aws.String(p.endpoint), Region: testRegion, HTTPClient: p.httpClient,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}, nil
		}),
		RetryMaxAttempts: 1,
	})
}

func mustKMSKey(t *testing.T, c *kms.Client) *types.KeyMetadata {
	t.Helper()
	out, err := c.CreateKey(t.Context(), &kms.CreateKeyInput{})
	if err != nil {
		t.Fatal(err)
	}
	if out.KeyMetadata == nil || out.KeyMetadata.KeyId == nil || out.KeyMetadata.Arn == nil {
		t.Fatal("CreateKey returned incomplete metadata")
	}
	return out.KeyMetadata
}

func wantKMSError(t *testing.T, err error, want string) {
	t.Helper()
	api, ok := errors.AsType[smithy.APIError](err)
	if !ok || api.ErrorCode() != want {
		t.Fatalf("KMS error = %v, want %s", err, want)
	}
}

func TestKMSKeyManagement(t *testing.T) {
	c := startPail(t).kmsClient()
	ctx := t.Context()
	k := mustKMSKey(t, c)
	if !k.Enabled || k.KeyState != types.KeyStateEnabled || k.KeySpec != types.KeySpecSymmetricDefault || k.KeyUsage != types.KeyUsageTypeEncryptDecrypt {
		t.Fatalf("CreateKey metadata = %v, want enabled symmetric key", k)
	}
	if _, err := c.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: aws.String("alias/sdk"), TargetKeyId: k.KeyId}); err != nil {
		t.Fatal(err)
	}
	aliasARN := "arn:aws:kms:" + testRegion + ":000000000000:alias/sdk"
	for _, id := range []*string{k.KeyId, k.Arn, aws.String("alias/sdk"), &aliasARN} {
		out, err := c.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: id})
		if err != nil || aws.ToString(out.KeyMetadata.KeyId) != aws.ToString(k.KeyId) {
			t.Fatalf("DescribeKey(%s) error = %v", aws.ToString(id), err)
		}
	}
	if _, err := c.UpdateKeyDescription(ctx, &kms.UpdateKeyDescriptionInput{KeyId: k.KeyId, Description: aws.String("updated")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DisableKey(ctx, &kms.DisableKeyInput{KeyId: k.KeyId}); err != nil {
		t.Fatal(err)
	}
	_, err := c.Encrypt(ctx, &kms.EncryptInput{KeyId: k.KeyId, Plaintext: []byte("x")})
	wantKMSError(t, err, "DisabledException")
	if _, err := c.EnableKey(ctx, &kms.EnableKeyInput{KeyId: k.KeyId}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.TagResource(ctx, &kms.TagResourceInput{KeyId: k.KeyId, Tags: []types.Tag{
		{TagKey: aws.String("env"), TagValue: aws.String("test")},
		{TagKey: aws.String("owner"), TagValue: aws.String("sdk")},
	}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: k.KeyId, Limit: aws.Int32(1)})
	if err != nil || len(tags.Tags) != 1 || !tags.Truncated || tags.NextMarker == nil {
		t.Fatalf("ListResourceTags first page = %v, %v", tags, err)
	}
	lastTags, err := c.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: k.KeyId, Limit: aws.Int32(1), Marker: tags.NextMarker})
	if err != nil || len(lastTags.Tags) != 1 || lastTags.Truncated || aws.ToString(lastTags.Tags[0].TagKey) == aws.ToString(tags.Tags[0].TagKey) {
		t.Fatalf("ListResourceTags second page = %v, %v", lastTags, err)
	}
	if _, err := c.UntagResource(ctx, &kms.UntagResourceInput{KeyId: k.KeyId, TagKeys: []string{"owner"}}); err != nil {
		t.Fatal(err)
	}
	policy, err := c.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{KeyId: k.KeyId})
	if err != nil || !strings.Contains(aws.ToString(policy.Policy), "000000000000:root") {
		t.Fatalf("GetKeyPolicy default policy error = %v", err)
	}
	custom := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"kms:*","Resource":"*"}]}`
	if _, err := c.PutKeyPolicy(ctx, &kms.PutKeyPolicyInput{KeyId: k.KeyId, Policy: &custom}); err != nil {
		t.Fatal(err)
	}
	policy, err = c.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{KeyId: k.KeyId})
	if err != nil || aws.ToString(policy.Policy) != custom || aws.ToString(policy.PolicyName) != "default" {
		t.Fatalf("GetKeyPolicy round trip differs, error = %v", err)
	}
	policies, err := c.ListKeyPolicies(ctx, &kms.ListKeyPoliciesInput{KeyId: k.KeyId})
	if err != nil || len(policies.PolicyNames) != 1 || policies.PolicyNames[0] != "default" || policies.Truncated {
		t.Fatalf("ListKeyPolicies = %v, %v", policies, err)
	}
	second := mustKMSKey(t, c)
	if _, err := c.UpdateAlias(ctx, &kms.UpdateAliasInput{AliasName: aws.String("alias/sdk"), TargetKeyId: second.KeyId}); err != nil {
		t.Fatal(err)
	}
	aliases, err := c.ListAliases(ctx, &kms.ListAliasesInput{KeyId: second.KeyId})
	if err != nil || len(aliases.Aliases) != 1 || aws.ToString(aliases.Aliases[0].TargetKeyId) != aws.ToString(second.KeyId) {
		t.Fatalf("ListAliases = %v, %v", aliases, err)
	}
	keys, err := c.ListKeys(ctx, &kms.ListKeysInput{Limit: aws.Int32(1)})
	if err != nil || len(keys.Keys) != 1 || !keys.Truncated {
		t.Fatalf("ListKeys first page = %v, %v", keys, err)
	}
	lastKeys, err := c.ListKeys(ctx, &kms.ListKeysInput{Limit: aws.Int32(1), Marker: keys.NextMarker})
	if err != nil || len(lastKeys.Keys) != 1 || lastKeys.Truncated || aws.ToString(lastKeys.Keys[0].KeyId) == aws.ToString(keys.Keys[0].KeyId) {
		t.Fatalf("ListKeys second page = %v, %v", lastKeys, err)
	}
	if _, err := c.DeleteAlias(ctx, &kms.DeleteAliasInput{AliasName: aws.String("alias/sdk")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String("alias/sdk")})
	wantKMSError(t, err, "NotFoundException")
	scheduled, err := c.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{KeyId: k.KeyId, PendingWindowInDays: aws.Int32(7)})
	if err != nil || scheduled.DeletionDate == nil || scheduled.KeyState != types.KeyStatePendingDeletion || aws.ToString(scheduled.KeyId) != aws.ToString(k.Arn) {
		t.Fatalf("ScheduleKeyDeletion = %v, %v", scheduled, err)
	}
	_, err = c.Encrypt(ctx, &kms.EncryptInput{KeyId: k.KeyId, Plaintext: []byte("x")})
	wantKMSError(t, err, "KMSInvalidStateException")
	if _, err := c.CancelKeyDeletion(ctx, &kms.CancelKeyDeletionInput{KeyId: k.KeyId}); err != nil {
		t.Fatal(err)
	}
	described, err := c.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: k.KeyId})
	if err != nil || described.KeyMetadata.KeyState != types.KeyStateDisabled || described.KeyMetadata.DeletionDate != nil || aws.ToString(described.KeyMetadata.Description) != "updated" {
		t.Fatalf("DescribeKey after cancellation = %v, %v", described, err)
	}
}

func TestKMSCryptography(t *testing.T) {
	c := startPail(t).kmsClient()
	ctx := t.Context()
	k, other := mustKMSKey(t, c), mustKMSKey(t, c)
	context := map[string]string{"tenant": "one", "purpose": "sdk"}
	data := bytes.Repeat([]byte{0, 1, 128, 255}, 1024)
	enc, err := c.Encrypt(ctx, &kms.EncryptInput{KeyId: k.KeyId, Plaintext: data, EncryptionContext: context})
	if err != nil || enc.EncryptionAlgorithm != types.EncryptionAlgorithmSpecSymmetricDefault {
		t.Fatalf("Encrypt error = %v", err)
	}
	dec, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob, EncryptionContext: context})
	if err != nil || !bytes.Equal(dec.Plaintext, data) || aws.ToString(dec.KeyId) != aws.ToString(k.Arn) || aws.ToString(dec.KeyMaterialId) != aws.ToString(k.CurrentKeyMaterialId) {
		t.Fatalf("Decrypt payload differs, error = %v", err)
	}
	_, err = c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob})
	wantKMSError(t, err, "InvalidCiphertextException")
	_, err = c.Decrypt(ctx, &kms.DecryptInput{KeyId: other.KeyId, CiphertextBlob: enc.CiphertextBlob, EncryptionContext: context})
	wantKMSError(t, err, "IncorrectKeyException")
	newContext := map[string]string{"purpose": "changed"}
	reenc, err := c.ReEncrypt(ctx, &kms.ReEncryptInput{
		CiphertextBlob: enc.CiphertextBlob, SourceEncryptionContext: context,
		DestinationKeyId: other.KeyId, DestinationEncryptionContext: newContext,
	})
	if err != nil || aws.ToString(reenc.SourceKeyId) != aws.ToString(k.Arn) || aws.ToString(reenc.KeyId) != aws.ToString(other.Arn) || aws.ToString(reenc.DestinationKeyMaterialId) != aws.ToString(other.CurrentKeyMaterialId) {
		t.Fatalf("ReEncrypt identifiers differ, error = %v", err)
	}
	dec, err = c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: reenc.CiphertextBlob, EncryptionContext: newContext})
	if err != nil || !bytes.Equal(dec.Plaintext, data) {
		t.Fatalf("ReEncrypt payload differs, error = %v", err)
	}
	for _, spec := range []types.DataKeySpec{types.DataKeySpecAes128, types.DataKeySpecAes256} {
		generated, err := c.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{KeyId: k.KeyId, KeySpec: spec, EncryptionContext: context})
		if err != nil {
			t.Fatal(err)
		}
		dec, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: generated.CiphertextBlob, EncryptionContext: context})
		if err != nil || !bytes.Equal(dec.Plaintext, generated.Plaintext) {
			t.Fatalf("GenerateDataKey(%s) decrypt differs, error = %v", spec, err)
		}
	}
	wrapped, err := c.GenerateDataKeyWithoutPlaintext(ctx, &kms.GenerateDataKeyWithoutPlaintextInput{KeyId: k.KeyId, NumberOfBytes: aws.Int32(37)})
	if err != nil {
		t.Fatal(err)
	}
	dec, err = c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: wrapped.CiphertextBlob})
	if err != nil || len(dec.Plaintext) != 37 {
		t.Fatalf("GenerateDataKeyWithoutPlaintext decrypt length differs, error = %v", err)
	}
	random, err := c.GenerateRandom(ctx, &kms.GenerateRandomInput{NumberOfBytes: aws.Int32(64)})
	if err != nil || len(random.Plaintext) != 64 {
		t.Fatalf("GenerateRandom length differs, error = %v", err)
	}
	_, err = c.Encrypt(ctx, &kms.EncryptInput{KeyId: k.KeyId, Plaintext: []byte("dry"), DryRun: aws.Bool(true)})
	wantKMSError(t, err, "DryRunOperationException")
}

func TestKMSValidation(t *testing.T) {
	c := startPail(t).kmsClient()
	k := mustKMSKey(t, c)
	tests := []struct {
		name, code string
		call       func() error
	}{
		{"asymmetric key", "UnsupportedOperationException", func() error {
			_, err := c.CreateKey(t.Context(), &kms.CreateKeyInput{KeySpec: types.KeySpecRsa2048})
			return err
		}},
		{"multi-region key", "UnsupportedOperationException", func() error {
			_, err := c.CreateKey(t.Context(), &kms.CreateKeyInput{MultiRegion: aws.Bool(true)})
			return err
		}},
		{"reserved alias", "InvalidAliasNameException", func() error {
			_, err := c.CreateAlias(t.Context(), &kms.CreateAliasInput{AliasName: aws.String("alias/aws/test"), TargetKeyId: k.KeyId})
			return err
		}},
		{"empty plaintext", "ValidationException", func() error {
			_, err := c.Encrypt(t.Context(), &kms.EncryptInput{KeyId: k.KeyId, Plaintext: []byte{}})
			return err
		}},
		{"large plaintext", "ValidationException", func() error {
			_, err := c.Encrypt(t.Context(), &kms.EncryptInput{KeyId: k.KeyId, Plaintext: make([]byte, 4097)})
			return err
		}},
		{"conflicting data key lengths", "ValidationException", func() error {
			_, err := c.GenerateDataKey(t.Context(), &kms.GenerateDataKeyInput{KeyId: k.KeyId, KeySpec: types.DataKeySpecAes256, NumberOfBytes: aws.Int32(32)})
			return err
		}},
		{"invalid marker", "InvalidMarkerException", func() error {
			_, err := c.ListKeys(t.Context(), &kms.ListKeysInput{Marker: aws.String("invalid")})
			return err
		}},
		{"zero page limit", "ValidationException", func() error {
			_, err := c.ListKeys(t.Context(), &kms.ListKeysInput{Limit: aws.Int32(0)})
			return err
		}},
		{"short deletion window", "ValidationException", func() error {
			_, err := c.ScheduleKeyDeletion(t.Context(), &kms.ScheduleKeyDeletionInput{KeyId: k.KeyId, PendingWindowInDays: aws.Int32(1)})
			return err
		}},
		{"cancel active key", "KMSInvalidStateException", func() error {
			_, err := c.CancelKeyDeletion(t.Context(), &kms.CancelKeyDeletionInput{KeyId: k.KeyId})
			return err
		}},
		{"malformed policy", "MalformedPolicyDocumentException", func() error {
			_, err := c.PutKeyPolicy(t.Context(), &kms.PutKeyPolicyInput{KeyId: k.KeyId, Policy: aws.String("{}")})
			return err
		}},
		{"unsupported operation", "UnsupportedOperationException", func() error {
			_, err := c.EnableKeyRotation(t.Context(), &kms.EnableKeyRotationInput{KeyId: k.KeyId})
			return err
		}},
		{"unsupported describe grant", "UnsupportedOperationException", func() error {
			_, err := c.DescribeKey(t.Context(), &kms.DescribeKeyInput{KeyId: k.KeyId, GrantTokens: []string{"test-grant-token"}})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { wantKMSError(t, tt.call(), tt.code) })
	}
}

func TestKMSRawRequests(t *testing.T) {
	p := startPail(t)
	tests := []struct {
		name, target, body, mode, code string
		status                         int
	}{
		{"unsigned", "TrentService.CreateKey", "{}", "unsigned", "MissingAuthenticationTokenException", 400},
		{"wrong key", "TrentService.CreateKey", "{}", "unknown", "UnrecognizedClientException", 400},
		{"bad signature", "TrentService.CreateKey", "{}", "bad", "InvalidSignatureException", 400},
		{"tampered body", "TrentService.CreateKey", "{}", "tampered", "InvalidSignatureException", 400},
		{"bad json", "TrentService.CreateKey", "{", "", "SerializationException", 400},
		{"null body", "TrentService.CreateKey", "null", "", "SerializationException", 400},
		{"bad binary", "TrentService.Encrypt", `{"KeyId":"test","Plaintext":"!"}`, "", "SerializationException", 400},
		{"unknown operation", "TrentService.Unknown", "{}", "", "UnknownOperationException", 400},
		{"large body", "TrentService.CreateKey", strings.Repeat(" ", 129<<10), "", "RequestEntityTooLargeException", 413},
		{"whitespace json", "TrentService.CreateKey", " \n{}\n", "", "", 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := http.NewRequestWithContext(t.Context(), http.MethodPost, p.endpoint+"/", strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Content-Type", "application/x-amz-json-1.1")
			r.Header.Set("X-Amz-Target", tt.target)
			creds := aws.Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
			if tt.mode == "unknown" {
				creds.AccessKeyID = "AKIAPAILUNKNOWN00000"
			}
			if tt.mode == "bad" {
				creds.SecretAccessKey = "incorrect-test-secret"
			}
			if tt.mode != "unsigned" {
				sum := sha256.Sum256([]byte(tt.body))
				if err := v4.NewSigner().SignHTTP(t.Context(), creds, r, hex.EncodeToString(sum[:]), "kms", testRegion, time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			if tt.mode == "tampered" {
				r.Body = io.NopCloser(strings.NewReader("[]"))
			}
			resp, err := p.httpClient.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			var body struct {
				Type string `json:"__type"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tt.status || body.Type != tt.code {
				t.Errorf("raw KMS status/code = %d/%q, want %d/%q", resp.StatusCode, body.Type, tt.status, tt.code)
			}
			if resp.Header.Get("X-Amzn-RequestId") == "" || resp.Header.Get("Content-Type") != "application/x-amz-json-1.1" {
				t.Error("KMS response lacks request ID or protocol content type")
			}
		})
	}
}
