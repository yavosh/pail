package diff

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func kmsStep(name, operation, body string) step {
	return step{name: name, service: "kms", method: http.MethodPost, target: "TrentService." + operation, body: body}
}

func kmsSymmetricScenario() scenario {
	return scenario{name: "kms-symmetric", check: checkKMSSymmetric, steps: []step{
		kmsStep("create-key", "CreateKey", `{"Description":"pail differential key","Tags":[{"TagKey":"purpose","TagValue":"test"}]}`),
		kmsStep("create-alias", "CreateAlias", `{"AliasName":"alias/{name}","TargetKeyId":"{keyId}"}`),
		kmsStep("describe-alias", "DescribeKey", `{"KeyId":"alias/{name}"}`),
		kmsStep("list-aliases", "ListAliases", `{"KeyId":"{keyId}"}`),
		kmsStep("list-tags", "ListResourceTags", `{"KeyId":"{keyId}"}`),
		kmsStep("encrypt", "Encrypt", `{"KeyId":"alias/{name}","Plaintext":"cGFpbCBwcm9iZQ==","EncryptionContext":{"purpose":"test"}}`),
		kmsStep("decrypt", "Decrypt", `{"CiphertextBlob":"{kmsCiphertext}","EncryptionContext":{"purpose":"test"}}`),
		kmsStep("decrypt-wrong-context", "Decrypt", `{"CiphertextBlob":"{kmsCiphertext}","EncryptionContext":{"purpose":"other"}}`),
		kmsStep("disable", "DisableKey", `{"KeyId":"{keyId}"}`),
		kmsStep("encrypt-disabled", "Encrypt", `{"KeyId":"{keyId}","Plaintext":"eA=="}`),
		kmsStep("enable", "EnableKey", `{"KeyId":"{keyId}"}`),
		kmsStep("create-destination", "CreateKey", `{}`),
		kmsStep("reencrypt", "ReEncrypt", `{"CiphertextBlob":"{kmsCiphertext}","SourceKeyId":"{previousKeyId}","SourceEncryptionContext":{"purpose":"test"},"DestinationKeyId":"{keyId}"}`),
		kmsStep("decrypt-reencrypted", "Decrypt", `{"CiphertextBlob":"{kmsCiphertext}","KeyId":"{keyId}"}`),
		kmsStep("data-key", "GenerateDataKey", `{"KeyId":"{keyId}","KeySpec":"AES_256"}`),
		kmsStep("decrypt-data-key", "Decrypt", `{"CiphertextBlob":"{kmsCiphertext}"}`),
		kmsStep("wrapped-data-key", "GenerateDataKeyWithoutPlaintext", `{"KeyId":"{keyId}","KeySpec":"AES_128"}`),
		kmsStep("decrypt-wrapped-data-key", "Decrypt", `{"CiphertextBlob":"{kmsCiphertext}"}`),
		kmsStep("schedule-deletion", "ScheduleKeyDeletion", `{"KeyId":"{keyId}","PendingWindowInDays":7}`),
		kmsStep("encrypt-pending", "Encrypt", `{"KeyId":"{keyId}","Plaintext":"eA=="}`),
		kmsStep("cancel-deletion", "CancelKeyDeletion", `{"KeyId":"{keyId}"}`),
		kmsStep("encrypt-after-cancel", "Encrypt", `{"KeyId":"{keyId}","Plaintext":"eA=="}`),
		kmsStep("delete-alias", "DeleteAlias", `{"AliasName":"alias/{name}"}`),
		kmsStep("schedule-source", "ScheduleKeyDeletion", `{"KeyId":"{previousKeyId}","PendingWindowInDays":7}`),
		kmsStep("schedule-destination", "ScheduleKeyDeletion", `{"KeyId":"{keyId}","PendingWindowInDays":7}`),
	}}
}

// maskKMSResponse prevents ciphertext and generated data keys from entering fixtures.
func maskKMSResponse(body []byte) []byte {
	var object map[string]any
	if json.Unmarshal(body, &object) != nil {
		return body
	}
	var mask func(map[string]any)
	mask = func(object map[string]any) {
		for key, value := range object {
			switch key {
			case "CiphertextBlob", "Plaintext", "CreationDate", "LastUpdatedDate", "DeletionDate", "KeyMaterialId", "CurrentKeyMaterialId", "SourceKeyMaterialId", "DestinationKeyMaterialId":
				object[key] = "<volatile>"
			default:
				if nested, ok := value.(map[string]any); ok {
					mask(nested)
				}
				if list, ok := value.([]any); ok {
					for _, entry := range list {
						if nested, ok := entry.(map[string]any); ok {
							mask(nested)
						}
					}
				}
			}
		}
	}
	mask(object)
	result, _ := json.Marshal(object)
	return result
}

func checkKMSSymmetric(steps []step, responses []response) error {
	results := map[string]map[string]json.RawMessage{}
	for i, st := range steps {
		var body map[string]json.RawMessage
		if json.Unmarshal(responses[i].body, &body) == nil {
			results[st.name] = body
		}
	}
	decode := func(name, field string) []byte {
		var text string
		_ = json.Unmarshal(results[name][field], &text)
		value, _ := base64.StdEncoding.DecodeString(text)
		return value
	}
	for _, name := range []string{"decrypt", "decrypt-reencrypted"} {
		if !bytes.Equal(decode(name, "Plaintext"), []byte("pail probe")) {
			return fmt.Errorf("%s did not return the input payload", name)
		}
	}
	generated := decode("data-key", "Plaintext")
	if len(generated) != 32 || !bytes.Equal(generated, decode("decrypt-data-key", "Plaintext")) {
		return fmt.Errorf("generated data key does not match its decrypted ciphertext")
	}
	if _, exists := results["wrapped-data-key"]["Plaintext"]; exists || len(decode("decrypt-wrapped-data-key", "Plaintext")) != 16 {
		return fmt.Errorf("wrapped data key leaked plaintext or has the wrong length")
	}
	for _, pair := range [][2]string{{"encrypt", "decrypt"}, {"reencrypt", "decrypt-reencrypted"}, {"data-key", "decrypt-data-key"}} {
		if len(results[pair[0]]["KeyId"]) == 0 || !bytes.Equal(results[pair[0]]["KeyId"], results[pair[1]]["KeyId"]) {
			return fmt.Errorf("%s and %s identify different keys", pair[0], pair[1])
		}
	}
	if !bytes.Equal(results["reencrypt"]["SourceKeyId"], results["encrypt"]["KeyId"]) {
		return fmt.Errorf("reencrypt identifies a different source key")
	}
	return nil
}

func TestKMSNormalization(t *testing.T) {
	body := []byte(`{"KeyMetadata":{"CurrentKeyMaterialId":"test-material","CreationDate":123},"CiphertextBlob":"dGVzdC1jaXBoZXJ0ZXh0","Plaintext":"dGVzdC1wbGFpbnRleHQ=","Aliases":[{"LastUpdatedDate":456}],"KeyId":"test-key"}`)
	masked := string(maskKMSResponse(body))
	for _, value := range []string{"test-material", "dGVzdC1jaXBoZXJ0ZXh0", "dGVzdC1wbGFpbnRleHQ=", "123", "456"} {
		if strings.Contains(masked, value) {
			t.Error("KMS normalization retained a volatile or cryptographic value")
		}
	}
	if !strings.Contains(masked, "test-key") {
		t.Error("KMS normalization removed a key identifier")
	}
}

func cleanupKMSKeys(t *testing.T, tg *target, vars map[string]string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
	defer cancel()
	for _, id := range []string{vars["keyId"], vars["previousKeyId"]} {
		if id == "" {
			continue
		}
		body, _ := json.Marshal(map[string]any{"KeyId": id, "PendingWindowInDays": 7})
		resp, err := tg.do(ctx, kmsStep("cleanup", "ScheduleKeyDeletion", string(body)), "")
		if err != nil {
			t.Errorf("schedule kms cleanup: %v", err)
			continue
		}
		if resp.status != http.StatusOK && !strings.Contains(string(resp.body), "KMSInvalidStateException") {
			t.Errorf("schedule kms cleanup status = %d, want 200 or already pending deletion", resp.status)
		}
	}
}
