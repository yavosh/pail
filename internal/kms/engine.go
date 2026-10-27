// Package kms implements local symmetric keys. See docs/storage.md for persistence.
package kms

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/yavosh/pail/internal/account"
	"github.com/yavosh/pail/internal/vfs"
)

const stateFile = "kms/state.json"

var keyIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// FS holds KMS state and can commit files with owner-only permissions.
type FS interface {
	vfs.FS
	CreatePrivateTemp() (vfs.TempFile, error)
}

// Engine serializes cryptographic operations and persistent key changes.
type Engine struct {
	mu     sync.Mutex
	fs     FS
	region string
	state  state
}

type state struct {
	Region  string           `json:"region"`
	Keys    map[string]key   `json:"keys"`
	Aliases map[string]Alias `json:"aliases"`
}

type key struct {
	ID           string            `json:"id"`
	Material     []byte            `json:"material"`
	Description  string            `json:"description"`
	Created      float64           `json:"created"`
	State        string            `json:"state"`
	DeletionDate float64           `json:"deletionDate,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	Policy       string            `json:"policy"`
}

// Metadata is the public description of a symmetric KMS key.
type Metadata struct {
	AWSAccountID          string   `json:"AWSAccountId"`
	ARN                   string   `json:"Arn"`
	KeyID                 string   `json:"KeyId"`
	CreationDate          float64  `json:"CreationDate"`
	Enabled               bool     `json:"Enabled"`
	Description           string   `json:"Description"`
	KeyUsage              string   `json:"KeyUsage"`
	KeyState              string   `json:"KeyState"`
	Origin                string   `json:"Origin"`
	KeyManager            string   `json:"KeyManager"`
	CustomerMasterKeySpec string   `json:"CustomerMasterKeySpec"`
	KeySpec               string   `json:"KeySpec"`
	EncryptionAlgorithms  []string `json:"EncryptionAlgorithms"`
	MultiRegion           bool     `json:"MultiRegion"`
	CurrentKeyMaterialID  string   `json:"CurrentKeyMaterialId"`
	DeletionDate          float64  `json:"DeletionDate,omitempty"`
}

// Open loads persisted keys and aliases. Missing state starts an empty engine.
func Open(ctx context.Context, fsys FS, region string) (*Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := fsys.MkdirAll("kms"); err != nil {
		return nil, fmt.Errorf("create kms directory: %w", err)
	}
	e := &Engine{fs: fsys, region: region}
	if err := vfs.ReadJSON(fsys, stateFile, &e.state); errors.Is(err, fs.ErrNotExist) {
		e.state = state{Region: region, Keys: map[string]key{}, Aliases: map[string]Alias{}}
	} else if err != nil {
		return nil, fmt.Errorf("read kms state: %w", err)
	}
	if e.state.Region != region || e.state.Keys == nil || e.state.Aliases == nil {
		return nil, errors.New("kms state has an invalid region or missing collections")
	}
	for id, k := range e.state.Keys {
		if id != k.ID || !keyIDPattern.MatchString(id) || len(k.Material) != 32 ||
			(k.State != "Enabled" && k.State != "Disabled" && k.State != "PendingDeletion") ||
			(k.State == "PendingDeletion") != (k.DeletionDate > 0) {
			return nil, errors.New("kms state contains an invalid key")
		}
	}
	for name, a := range e.state.Aliases {
		if validateAlias(name) != nil || a.AliasName != name || a.AliasARN != e.arn(name) {
			return nil, errors.New("kms state contains an invalid alias")
		}
		if _, ok := e.state.Keys[a.TargetKeyID]; !ok {
			return nil, errors.New("kms state contains an alias without a key")
		}
	}
	return e, nil
}

func (e *Engine) arn(resource string) string {
	return "arn:aws:kms:" + e.region + ":" + account.ID + ":" + resource
}

func (e *Engine) metadata(k key) Metadata {
	return Metadata{
		AWSAccountID: account.ID, ARN: e.arn("key/" + k.ID), KeyID: k.ID,
		CreationDate: k.Created, Enabled: k.State == "Enabled", Description: k.Description,
		KeyUsage: "ENCRYPT_DECRYPT", KeyState: k.State, Origin: "AWS_KMS", KeyManager: "CUSTOMER",
		CustomerMasterKeySpec: "SYMMETRIC_DEFAULT", KeySpec: "SYMMETRIC_DEFAULT",
		EncryptionAlgorithms: []string{"SYMMETRIC_DEFAULT"}, DeletionDate: k.DeletionDate,
		CurrentKeyMaterialID: materialID(k),
	}
}

func materialID(k key) string {
	sum := sha256.Sum256(k.Material)
	return hex.EncodeToString(sum[:])
}

func timestamp() float64 { return float64(time.Now().UnixMilli()) / 1000 }

// lock sweeps expired keys before exposing state. Its caller must unlock on success.
func (e *Engine) lock(ctx context.Context) error {
	e.mu.Lock()
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		return err
	}
	now := timestamp()
	next := e.snapshot()
	for id, k := range next.Keys {
		if k.State == "PendingDeletion" && now >= k.DeletionDate {
			delete(next.Keys, id)
		}
	}
	if len(next.Keys) != len(e.state.Keys) {
		for name, a := range next.Aliases {
			if _, ok := next.Keys[a.TargetKeyID]; !ok {
				delete(next.Aliases, name)
			}
		}
		if err := e.commit(next); err != nil {
			e.mu.Unlock()
			return err
		}
	}
	return nil
}

func (e *Engine) snapshot() state {
	return state{Region: e.region, Keys: maps.Clone(e.state.Keys), Aliases: maps.Clone(e.state.Aliases)}
}

func (e *Engine) commit(next state) error {
	b, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("encode kms state: %w", err)
	}
	f, err := e.fs.CreatePrivateTemp()
	if err != nil {
		return fmt.Errorf("create kms state: %w", err)
	}
	defer func() { _ = f.Abort() }()
	if _, err := f.Write(b); err != nil {
		return fmt.Errorf("write kms state: %w", err)
	}
	if err := f.Commit(stateFile); err != nil {
		return fmt.Errorf("commit kms state: %w", err)
	}
	e.state = next
	return nil
}

func (e *Engine) resolve(id string, aliases bool) (key, error) {
	if id == "" || len(id) > 2048 {
		return key{}, invalid("key identifier must contain 1 to 2048 characters")
	}
	if strings.HasPrefix(id, "arn:") {
		prefix := e.arn("")
		if !strings.HasPrefix(id, prefix) {
			return key{}, fail("NotFoundException", "key not found")
		}
		id = strings.TrimPrefix(id, prefix)
		if after, ok := strings.CutPrefix(id, "key/"); ok {
			if !keyIDPattern.MatchString(after) {
				return key{}, fail("InvalidArnException", "invalid key arn")
			}
			id = after
		} else if !strings.HasPrefix(id, "alias/") {
			return key{}, fail("InvalidArnException", "invalid key arn")
		}
	}
	if strings.HasPrefix(id, "alias/") {
		if !aliases {
			return key{}, invalid("this operation requires a key id or key arn")
		}
		id = e.state.Aliases[id].TargetKeyID
	}
	k, ok := e.state.Keys[id]
	if !ok {
		return key{}, fail("NotFoundException", "key not found")
	}
	return k, nil
}

func active(k key) error {
	if k.State == "Disabled" {
		return fail("DisabledException", "the key is disabled")
	}
	return mutable(k)
}

func mutable(k key) error {
	if k.State == "PendingDeletion" {
		return fail("KMSInvalidStateException", "the key is pending deletion")
	}
	return nil
}

// CreateInput describes a new symmetric key.
type CreateInput struct {
	Description, KeySpec, CustomerMasterKeySpec, KeyUsage, Origin string
	Policy, CustomKeyStoreID, XksKeyID                            string
	MultiRegion, BypassPolicyLockoutSafetyCheck                   bool
	Tags                                                          []Tag
}

// Create creates and persists a key before returning its metadata.
func (e *Engine) Create(ctx context.Context, in CreateInput) (Metadata, error) {
	if in.KeySpec != "" && in.CustomerMasterKeySpec != "" && in.KeySpec != in.CustomerMasterKeySpec {
		return Metadata{}, invalid("key spec parameters must agree")
	}
	for _, spec := range []string{in.KeySpec, in.CustomerMasterKeySpec} {
		if spec != "" && spec != "SYMMETRIC_DEFAULT" {
			return Metadata{}, unsupported()
		}
	}
	if (in.KeyUsage != "" && in.KeyUsage != "ENCRYPT_DECRYPT") || (in.Origin != "" && in.Origin != "AWS_KMS") ||
		in.MultiRegion || in.CustomKeyStoreID != "" || in.XksKeyID != "" {
		return Metadata{}, unsupported()
	}
	if !utf8.ValidString(in.Description) || utf8.RuneCountInString(in.Description) > 8192 {
		return Metadata{}, invalid("description exceeds 8192 characters")
	}
	tags, err := mergeTags(nil, in.Tags)
	if err != nil {
		return Metadata{}, err
	}
	policy := in.Policy
	if policy == "" {
		policy = `{"Version":"2012-10-17","Statement":[{"Sid":"Enable IAM User Permissions","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::` + account.ID + `:root"},"Action":"kms:*","Resource":"*"}]}`
	}
	if err := validatePolicy(policy); err != nil {
		return Metadata{}, err
	}
	if err := e.lock(ctx); err != nil {
		return Metadata{}, err
	}
	defer e.mu.Unlock()
	id := newID()
	material := make([]byte, 32)
	_, _ = rand.Read(material)
	k := key{ID: id, Material: material, Description: in.Description, Created: timestamp(), State: "Enabled", Tags: tags, Policy: policy}
	next := e.snapshot()
	next.Keys[id] = k
	if err := e.commit(next); err != nil {
		return Metadata{}, err
	}
	return e.metadata(k), nil
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// Describe resolves a key ID, key ARN, alias name, or alias ARN.
func (e *Engine) Describe(ctx context.Context, id string) (Metadata, error) {
	if err := e.lock(ctx); err != nil {
		return Metadata{}, err
	}
	defer e.mu.Unlock()
	k, err := e.resolve(id, true)
	if err != nil {
		return Metadata{}, err
	}
	return e.metadata(k), nil
}

// Change updates a description, key state, or deletion schedule atomically.
func (e *Engine) Change(ctx context.Context, id, operation, description string, days int) (Metadata, error) {
	if err := e.lock(ctx); err != nil {
		return Metadata{}, err
	}
	defer e.mu.Unlock()
	k, err := e.resolve(id, false)
	if err != nil {
		return Metadata{}, err
	}
	if operation != "CancelKeyDeletion" {
		if err := mutable(k); err != nil {
			return Metadata{}, err
		}
	}
	switch operation {
	case "EnableKey":
		k.State = "Enabled"
	case "DisableKey":
		k.State = "Disabled"
	case "UpdateKeyDescription":
		if !utf8.ValidString(description) || utf8.RuneCountInString(description) > 8192 {
			return Metadata{}, invalid("description exceeds 8192 characters")
		}
		k.Description = description
	case "ScheduleKeyDeletion":
		if days < 7 || days > 30 {
			return Metadata{}, invalid("pending window must be between 7 and 30 days")
		}
		k.State = "PendingDeletion"
		k.DeletionDate = timestamp() + float64(days)*86400
	case "CancelKeyDeletion":
		if k.State != "PendingDeletion" {
			return Metadata{}, fail("KMSInvalidStateException", "the key is not pending deletion")
		}
		k.State, k.DeletionDate = "Disabled", 0
	default:
		return Metadata{}, unsupported()
	}
	next := e.snapshot()
	next.Keys[k.ID] = k
	if err := e.commit(next); err != nil {
		return Metadata{}, err
	}
	return e.metadata(k), nil
}
