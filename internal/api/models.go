// Package api holds the /api/v1 data types, the strict
// request parsers and the route registration contract for the Phase 0.5
// subset. It depends only on the standard library and the foundation
// packages, so the database, auth and lifecycle packages can use it without
// cycles.
//
// Handlers live in sibling files owned by their feature package. The types
// here serialize to the same field names, enums and nullability as the pinned
// API document in api/bbraft-v1.yaml; internal/compat checks that against the
// real spec in tests.
package api

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"time"
)

// Success envelope discriminators. Each response type sets Type through its
// constructor so a handler cannot accidentally emit the wrong envelope.
const (
	TypeUserInfo             = "user.info"
	TypeSandboxList          = "sandbox.list"
	TypeSandboxInfo          = "sandbox.info"
	TypeSandboxCreated       = "sandbox.created"
	TypeSandboxDeleting      = "sandbox.deleting"
	TypeDeletionOperation    = "deletion.operation"
	TypeCommandFinished      = "command.finished"
	TypeCommandStarted       = "command.started"
	TypeCommandStatus        = "command.status"
	TypeSavedSnapshotList    = "snapshot.saved.list"
	TypeSavedSnapshotInfo    = "snapshot.saved.info"
	TypeSavedSnapshotSaving  = "snapshot.saved.saving"
	TypeSavedSnapshotDeleted = "snapshot.saved.deleted"
	TypeFileUploadURL        = "file.upload_url"
)

// SuccessBase is embedded in every success envelope. Type is the stable
// discriminator; constructors set it.
type SuccessBase struct {
	OK   bool   `json:"ok"`
	Type string `json:"type"`
}

func success(typ string) SuccessBase {
	return SuccessBase{OK: true, Type: typ}
}

// Optional distinguishes a JSON member that is absent from one explicitly set
// to null. Present reports whether the member was written, and Value is nil
// for an explicit null. The zero value is absent and is omitted from structs
// that tag the field with omitzero.
type Optional[T any] struct {
	Present bool
	Value   *T
}

// Some returns an Optional holding v.
func Some[T any](v T) Optional[T] {
	return Optional[T]{Present: true, Value: &v}
}

// Null returns a present Optional holding an explicit JSON null.
func Null[T any]() Optional[T] {
	return Optional[T]{Present: true}
}

// MarshalJSONTo writes null for an absent or null Optional and the value
// otherwise.
func (o Optional[T]) MarshalJSONTo(enc *jsontext.Encoder) error {
	if o.Value == nil {
		return enc.WriteToken(jsontext.Null)
	}
	return json.MarshalEncode(enc, *o.Value)
}

// UnmarshalJSONFrom records the member as present and decodes either null or
// one value.
func (o *Optional[T]) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	o.Present = true
	if dec.PeekKind() == jsontext.KindNull {
		o.Value = nil
		return dec.SkipValue()
	}
	var v T
	if err := json.UnmarshalDecode(dec, &v); err != nil {
		return err
	}
	o.Value = &v
	return nil
}

// PageInfo is the cursor page metadata of list responses. NextCursor is always
// serialized, as an explicit null when the page is the last one.
type PageInfo struct {
	NextCursor *string `json:"nextCursor"`
	HasMore    bool    `json:"hasMore"`
	Limit      int     `json:"limit"`
}

// NewPageInfo builds page metadata. A nil next cursor means the last page, so
// HasMore follows from it.
func NewPageInfo(nextCursor *string, limit int) PageInfo {
	return PageInfo{NextCursor: nextCursor, HasMore: nextCursor != nil, Limit: limit}
}

// SandboxState is one value of the API sandbox state machine.
type SandboxState string

// The API sandbox states. cancelled is terminal.
const (
	StateInit         SandboxState = "init"
	StateProvisioning SandboxState = "provisioning"
	StateProvisioned  SandboxState = "provisioned"
	StateCloning      SandboxState = "cloning"
	StateReady        SandboxState = "ready"
	StateIdle         SandboxState = "idle"
	StateRunning      SandboxState = "running"
	StateArchiving    SandboxState = "archiving"
	StateArchived     SandboxState = "archived"
	StateError        SandboxState = "error"
	StateCancelled    SandboxState = "cancelled"
)

// Sandbox is the public sandbox object. Fields the server always sets are
// plain values; nullable fields that v1 clients read are pointers so they
// serialize as explicit nulls. Optional fields that the API omits when unset use
// omitzero.
type Sandbox struct {
	ID     string            `json:"id"`
	Name   string            `json:"name"`
	State  SandboxState      `json:"state"`
	Error  *string           `json:"error,omitzero"`
	Labels map[string]string `json:"labels,omitzero"`

	Type              string  `json:"type,omitzero"`
	VCPU              int     `json:"vcpu,omitzero"`
	MemoryGB          int     `json:"memoryGB,omitzero"`
	BillingMultiplier float64 `json:"billingMultiplier,omitzero"`

	URL       *string `json:"url"`
	IP        *string `json:"ip"`
	Subdomain *string `json:"subdomain"`

	// SSHEndpoint is the node-confirmed host:port a client dials over SSH.
	// It is exposed only after a successful sshKey call for the current
	// boot; the v1 API document makes it nullable and the API sends null when
	// unknown. The host key is never part of the sandbox object.
	SSHEndpoint  *string    `json:"sshEndpoint"`
	CreatedAt    *time.Time `json:"createdAt"`
	UpdatedAt    *time.Time `json:"updatedAt"`
	ArchiveAfter *time.Time `json:"archiveAfter"`
	TTLSeconds   *int       `json:"ttlSeconds"`

	DesktopAvailable bool    `json:"desktopAvailable"`
	DesktopURL       *string `json:"desktopUrl"`

	Snapshots           *bool      `json:"snapshots,omitzero"`
	Monitoring          *bool      `json:"monitoring,omitzero"`
	SnapshotAvailable   bool       `json:"snapshotAvailable"`
	SnapshotCompletedAt *time.Time `json:"snapshotCompletedAt"`

	StoppedAt     *time.Time     `json:"stoppedAt,omitzero"`
	DataExpiredAt *time.Time     `json:"dataExpiredAt,omitzero"`
	Stop          *StopOperation `json:"stop,omitzero"`
}

// SandboxListResponse is the GET /api/v1/sandboxes envelope.
type SandboxListResponse struct {
	SuccessBase
	Sandboxes   []Sandbox `json:"sandboxes"`
	PageInfo    PageInfo  `json:"pageInfo"`
	DeletedCost *Cost     `json:"deletedCost,omitzero"`
}

// NewSandboxListResponse builds a list envelope with the fixed type. A nil
// slice is stored as an empty array because the pinned schema requires an
// array, never null.
func NewSandboxListResponse(sandboxes []Sandbox, page PageInfo) SandboxListResponse {
	if sandboxes == nil {
		sandboxes = []Sandbox{}
	}
	return SandboxListResponse{
		SuccessBase: success(TypeSandboxList),
		Sandboxes:   sandboxes,
		PageInfo:    page,
	}
}

// SandboxInfoResponse is the GET /api/v1/sandboxes/{sandboxId} envelope.
type SandboxInfoResponse struct {
	SuccessBase
	Sandbox         Sandbox `json:"sandbox"`
	GiftLimitNotice string  `json:"giftLimitNotice,omitzero"`
}

// NewSandboxInfoResponse builds a sandbox info envelope.
func NewSandboxInfoResponse(s Sandbox) SandboxInfoResponse {
	return SandboxInfoResponse{SuccessBase: success(TypeSandboxInfo), Sandbox: s}
}

// CreateStatus is the create response status. ready appears only when an
// idempotent retry finds a sandbox already past provisioning.
type CreateStatus string

// Create response statuses.
const (
	CreateProvisioning CreateStatus = "provisioning"
	CreateReady        CreateStatus = "ready"
)

// CreateSandboxResponse is the 202 envelope of POST /api/v1/sandboxes.
type CreateSandboxResponse struct {
	SuccessBase
	Status          CreateStatus     `json:"status"`
	TTLSeconds      *int             `json:"ttlSeconds"`
	Sandbox         Sandbox          `json:"sandbox"`
	Sandboxes       []Sandbox        `json:"sandboxes,omitzero"`
	HostedPorts     []HostedPortInfo `json:"hostedPorts,omitzero"`
	GiftLimitNotice string           `json:"giftLimitNotice,omitzero"`
}

// NewCreateSandboxResponse builds a create envelope. ttl is null when
// auto-stop is disabled.
func NewCreateSandboxResponse(status CreateStatus, ttl *int, s Sandbox) CreateSandboxResponse {
	return CreateSandboxResponse{
		SuccessBase: success(TypeSandboxCreated),
		Status:      status,
		TTLSeconds:  ttl,
		Sandbox:     s,
	}
}

// NewCreateSandboxBatchResponse builds the create envelope of a request with
// count above one: every sandbox in creation order, and the first one in
// sandbox.
func NewCreateSandboxBatchResponse(status CreateStatus, ttl *int, sandboxes []Sandbox) CreateSandboxResponse {
	out := NewCreateSandboxResponse(status, ttl, Sandbox{})
	if len(sandboxes) > 0 {
		out.Sandbox = sandboxes[0]
	}
	out.Sandboxes = sandboxes
	return out
}

// HostedPortInfo is one entry of the hostedPorts create response field. It is
// part of the pinned shape but hosting is outside the Phase 0.5 subset.
type HostedPortInfo struct {
	Port        int    `json:"port"`
	URL         string `json:"url,omitzero"`
	Access      string `json:"access,omitzero"`
	IsProtected *bool  `json:"isProtected,omitzero"`
	Localhost   *bool  `json:"localhost,omitzero"`
	Error       string `json:"error,omitzero"`
	Message     string `json:"message,omitzero"`
}

// Cost is machine time at list price. Billing is outside the Phase 0.5
// subset, but the pinned list response may carry it with include=cost.
type Cost struct {
	Since   time.Time `json:"since"`
	Seconds int64     `json:"seconds"`
	Dollars float64   `json:"dollars"`
}

// MeResponse is the GET /api/v1/me envelope.
type MeResponse struct {
	SuccessBase
	User MeUser `json:"user"`
}

// NewMeResponse builds the current-user envelope.
func NewMeResponse(u MeUser) MeResponse {
	return MeResponse{SuccessBase: success(TypeUserInfo), User: u}
}

// MeUser is the account object of GET /api/v1/me. Every field is optional in
// the v1 API document, so unset fields are omitted.
type MeUser struct {
	ID                         string     `json:"id,omitzero"`
	Login                      string     `json:"login,omitzero"`
	Email                      *string    `json:"email,omitzero"`
	DisplayName                string     `json:"displayName,omitzero"`
	ConnectionMethod           string     `json:"connectionMethod,omitzero"`
	ConnectionMethods          []string   `json:"connectionMethods,omitzero"`
	AccountPlan                string     `json:"accountPlan,omitzero"`
	ServiceAccount             *bool      `json:"serviceAccount,omitzero"`
	AccountStatus              string     `json:"accountStatus,omitzero"`
	Suspended                  *bool      `json:"suspended,omitzero"`
	SuspendedAt                *time.Time `json:"suspendedAt,omitzero"`
	SuspendedReason            *string    `json:"suspendedReason,omitzero"`
	Closed                     *bool      `json:"closed,omitzero"`
	ClosedAt                   *time.Time `json:"closedAt,omitzero"`
	PurgeAt                    *time.Time `json:"purgeAt,omitzero"`
	PurgedAt                   *time.Time `json:"purgedAt,omitzero"`
	ZeroDataRetention          *bool      `json:"zeroDataRetention,omitzero"`
	ZeroDataRetentionEnabledAt *time.Time `json:"zeroDataRetentionEnabledAt,omitzero"`
}
