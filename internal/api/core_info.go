package api

import (
	"fmt"
	"math"
	"net/http"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/version"
)

// PathInfo is the authenticated extension endpoint that advertises the
// command limits this server actually enforces. It lives outside /api/v1:
// v1 clients never see it, and the bbraft CLI uses it to pick the exact
// stream timeout instead of guessing.
const PathInfo = "/api/bbraft/info"

// TypeBBraftInfo is the success discriminator of GET /api/bbraft/info.
const TypeBBraftInfo = "bbraft.info"

// DefaultSyncMaxSeconds is the pinned synchronous command maximum. Per the
// approved Phase 0.5 difference only streamed commands exceed 600 seconds.
const DefaultSyncMaxSeconds = 600

// InfoResponse is the GET /api/bbraft/info envelope. It carries only build and
// limit metadata: no credential, node, host or account data.
type InfoResponse struct {
	SuccessBase
	Version  string       `json:"version"`
	Commands InfoCommands `json:"commands"`
}

// InfoCommands are the command timeouts this server enforces.
type InfoCommands struct {
	StreamMaxSeconds int `json:"streamMaxSeconds"`
	SyncMaxSeconds   int `json:"syncMaxSeconds"`
	// Detached reports that this server implements the pinned detached
	// command start, status, attach and stop operations. It is absent on a
	// server that only supports streamed commands.
	Detached bool `json:"detached,omitzero"`
}

// NewInfoResponse builds the extension info envelope.
func NewInfoResponse(buildVersion string, commands InfoCommands) InfoResponse {
	return InfoResponse{
		SuccessBase: success(TypeBBraftInfo),
		Version:     buildVersion,
		Commands:    commands,
	}
}

// InfoOptions are the metadata and actual limits of the extension endpoint.
type InfoOptions struct {
	// Version is the build version the server reports. Empty uses the
	// linked internal/version default.
	Version string
	// StreamMaxSeconds is the configured streamed-command cap. Required
	// and positive.
	StreamMaxSeconds int
	// SyncMaxSeconds is the synchronous cap. Zero uses
	// DefaultSyncMaxSeconds; values outside 1..600 are configuration
	// errors.
	SyncMaxSeconds int
	// Detached advertises implemented detached command operations.
	Detached bool
}

// ExtensionHandlers holds the /api/bbraft extension routes. The server
// registers them under the same authentication middleware as /api/v1.
type ExtensionHandlers struct {
	Info http.HandlerFunc
}

// RegisterExtensions adds the extension routes to mux. A nil handler leaves
// its route unregistered, matching [Register].
func RegisterExtensions(mux *http.ServeMux, handlers ExtensionHandlers) {
	if handlers.Info != nil {
		mux.HandleFunc("GET "+PathInfo, handlers.Info)
	}
}

// coreInfoHandler serves the extension info endpoint.
type coreInfoHandler struct {
	version   string
	streamMax int
	syncMax   int
	detached  bool
}

// NewInfoHandler returns the authenticated GET /api/bbraft/info handler. It
// rejects invalid limits at construction so the endpoint can never advertise
// a timeout the command handler will not honor.
func NewInfoHandler(options InfoOptions) (http.HandlerFunc, error) {
	if options.StreamMaxSeconds < 1 || uint64(options.StreamMaxSeconds) > math.MaxUint32 {
		return nil, fmt.Errorf("api: info stream max seconds %d must be positive and fit a command timeout", options.StreamMaxSeconds)
	}
	syncMax := options.SyncMaxSeconds
	if syncMax == 0 {
		syncMax = DefaultSyncMaxSeconds
	}
	if syncMax < 1 || syncMax > DefaultSyncMaxSeconds {
		return nil, fmt.Errorf("api: info sync max seconds %d must be from 1 to %d", syncMax, DefaultSyncMaxSeconds)
	}
	buildVersion := options.Version
	if buildVersion == "" {
		buildVersion = version.Version
	}
	handler := &coreInfoHandler{
		version:   buildVersion,
		streamMax: options.StreamMaxSeconds,
		syncMax:   syncMax,
		detached:  options.Detached,
	}
	return handler.serve, nil
}

// serve writes the info envelope.
func (h *coreInfoHandler) serve(w http.ResponseWriter, r *http.Request) {
	if _, apiErr := coreOwner(r); apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	apierr.Write(w, http.StatusOK, TypeBBraftInfo, NewInfoResponse(h.version, InfoCommands{
		StreamMaxSeconds: h.streamMax,
		SyncMaxSeconds:   h.syncMax,
		Detached:         h.detached,
	}))
}
