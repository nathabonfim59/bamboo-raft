package db

import "errors"

// Errors reported by Store operations. They are returned unwrapped so
// callers compare with errors.Is; every other failure is wrapped with
// context.
var (
	// ErrNotFound means the row does not exist or belongs to another
	// account. Owner-scoped lookups never distinguish the two.
	ErrNotFound = errors.New("db: not found")

	// ErrConflict means an optimistic check failed: the row changed since
	// the caller loaded it (version or from-state mismatch), or another
	// writer won a concurrent insert.
	ErrConflict = errors.New("db: conflict")

	// ErrNameTaken means an active row already uses this owner-scoped
	// name (saved snapshot names).
	ErrNameTaken = errors.New("db: name taken")

	// ErrNoCapacity means the node has less free capacity than
	// requested.
	ErrNoCapacity = errors.New("db: no capacity")

	// ErrIdempotencyReused means this create key was seen before with a
	// different request body.
	ErrIdempotencyReused = errors.New("db: idempotency key reused")

	// ErrIdempotencyInProgress means a concurrent create with the same
	// key committed first while this one was writing; the client retries
	// shortly with the same key and body. Handlers map it to 409
	// idempotency_in_progress.
	ErrIdempotencyInProgress = errors.New("db: idempotency create in progress")

	// ErrNoJobs means ClaimJob found no runnable job right now.
	ErrNoJobs = errors.New("db: no jobs")

	// ErrJobNotLeased means the job is no longer running under this
	// worker: the lease expired and another worker claimed it, or the
	// job already finished. The caller treats the work as delivered
	// elsewhere.
	ErrJobNotLeased = errors.New("db: job not leased")

	// ErrInvalidName rejects an empty sandbox display name or one longer
	// than [MaxSandboxNameLength] characters.
	ErrInvalidName = errors.New("db: invalid sandbox name")

	// ErrInvalidSubdomain rejects a subdomain outside the pinned shape:
	// lowercase letters, digits and single hyphens, 3..40 chars, no
	// leading/trailing/double hyphens and no reserved suffix.
	ErrInvalidSubdomain = errors.New("db: invalid sandbox subdomain")

	// ErrSubdomainTaken means another live sandbox, possibly owned by
	// another account, already holds the requested subdomain.
	ErrSubdomainTaken = errors.New("db: sandbox subdomain taken")

	// ErrInvalidState means the sandbox is not in a state the operation
	// accepts (for example resuming a running sandbox).
	ErrInvalidState = errors.New("db: sandbox state does not allow the operation")

	// ErrInvalidMachine rejects a resume that changes machine type without
	// a complete resource shape, or resources without a machine type.
	ErrInvalidMachine = errors.New("db: invalid machine shape")

	// ErrTypeTooSmall means a resume would move a retained disk onto a
	// smaller disk before the disk could be grown or compacted.
	ErrTypeTooSmall = errors.New("db: machine type too small for the retained disk")

	// ErrNodeUnavailable means the sandbox's node row is gone, so its
	// retained disk cannot be resumed on the same node.
	ErrNodeUnavailable = errors.New("db: sandbox node unavailable")

	// ErrStopSuperseded means a resume ended the stop operation before
	// this attempt could finalize it. The worker stops without archiving.
	ErrStopSuperseded = errors.New("db: stop operation superseded")

	// ErrInvalidJobPayload means the claimed job's payload does not decode
	// or does not match the operation the caller asked to mutate. It fails
	// closed instead of trusting a stale or forged payload.
	ErrInvalidJobPayload = errors.New("db: invalid or mismatched job payload")
)
