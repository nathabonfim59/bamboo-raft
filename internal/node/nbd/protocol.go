package nbd

import "fmt"

// Wire constants from the NBD protocol (doc/proto.md of the NBD project).
const (
	magicNBD          uint64 = 0x4e42444d41474943 // "NBDMAGIC"
	magicIHaveOpt     uint64 = 0x49484156454F5054 // "IHAVEOPT"
	magicOldstyle     uint64 = 0x00420281861253
	magicOptReply     uint64 = 0x3e889045565a9
	magicRequest      uint32 = 0x25609513
	magicSimpleReply  uint32 = 0x67446698
	magicStructured   uint32 = 0x668e33ef
	magicExtendedRply uint32 = 0x6e8a278c

	// Handshake flags (server) and client flags.
	flagFixedNewstyle   uint16 = 1 << 0
	flagNoZeroes        uint16 = 1 << 1
	flagCFixedNewstyle  uint32 = 1 << 0
	flagCNoZeroes       uint32 = 1 << 1
	optStructuredReply  uint32 = 8
	optGo               uint32 = 7
	optSetMetaContext   uint32 = 10
	repAck              uint32 = 1
	repInfo             uint32 = 3
	repMetaContext      uint32 = 4
	repErrBit           uint32 = 1 << 31
	infoExport          uint16 = 0
	infoBlockSize       uint16 = 3
	tflagHasFlags       uint16 = 1 << 0
	tflagReadOnly       uint16 = 1 << 1
	tflagSendFlush      uint16 = 1 << 2
	tflagSendWriteZeros uint16 = 1 << 6

	cmdRead        uint16 = 0
	cmdWrite       uint16 = 1
	cmdDisc        uint16 = 2
	cmdFlush       uint16 = 3
	cmdWriteZeroes uint16 = 6
	cmdBlockStatus uint16 = 7

	replyFlagDone       uint16 = 1 << 0
	replyNone           uint16 = 0
	replyOffsetData     uint16 = 1
	replyOffsetHole     uint16 = 2
	replyBlockStatus    uint16 = 5
	replyErrBit         uint16 = 1 << 15
	replyError          uint16 = replyErrBit | 1
	replyErrorOffset    uint16 = replyErrBit | 2
	requestHeaderLength        = 28
)

// Client-side bounds. Nothing the server sends is allocated or trusted beyond these.
const (
	// maxStringLen is the protocol's limit on export and meta context names.
	maxStringLen = 4096
	// maxMetaContexts bounds Options.MetaContexts.
	maxMetaContexts = 64
	// maxOptionReply bounds the payload of one option reply.
	maxOptionReply = 16 << 10
	// maxOptionReplies bounds the number of replies to one option.
	maxOptionReplies = 256
	// maxErrorMessage bounds the message of an error chunk.
	maxErrorMessage = 4096
	// maxChunks bounds the number of structured chunks in one reply.
	maxChunks = 1 << 16
	// maxBlockStatusPayload bounds one BLOCK_STATUS chunk: a context id and
	// up to 256Ki descriptors (QEMU sends at most 128Ki).
	maxBlockStatusPayload = 4 + 8*(1<<18)
	// maxPayloadCap caps the payload of one READ or WRITE request whatever the
	// server advertises (the protocol's default maximum).
	maxPayloadCap = 32 << 20
	// maxBlockStatusLen is the longest range asked in one BLOCK_STATUS request.
	maxBlockStatusLen = 1 << 31
	// maxWriteZeroesLen is the longest range zeroed by one WRITE_ZEROES request.
	maxWriteZeroesLen = 1 << 30
	// maxMinBlock is the protocol's limit on the minimum block size.
	maxMinBlock = 64 << 10
)

// Errno values the protocol defines for NBD_CMD replies.
const (
	errnoEPERM     = 1
	errnoEIO       = 5
	errnoENOMEM    = 12
	errnoEINVAL    = 22
	errnoENOSPC    = 28
	errnoEOVERFLOW = 75
	errnoENOTSUP   = 95
	errnoESHUTDOWN = 108
)

func errnoName(errno uint32) string {
	switch errno {
	case errnoEPERM:
		return "EPERM"
	case errnoEIO:
		return "EIO"
	case errnoENOMEM:
		return "ENOMEM"
	case errnoEINVAL:
		return "EINVAL"
	case errnoENOSPC:
		return "ENOSPC"
	case errnoEOVERFLOW:
		return "EOVERFLOW"
	case errnoENOTSUP:
		return "ENOTSUP"
	case errnoESHUTDOWN:
		return "ESHUTDOWN"
	}
	return fmt.Sprintf("errno %d", errno)
}

func optionName(opt uint32) string {
	switch opt {
	case optGo:
		return "NBD_OPT_GO"
	case optStructuredReply:
		return "NBD_OPT_STRUCTURED_REPLY"
	case optSetMetaContext:
		return "NBD_OPT_SET_META_CONTEXT"
	}
	return fmt.Sprintf("option %d", opt)
}

func repName(rep uint32) string {
	switch rep {
	case repAck:
		return "NBD_REP_ACK"
	case repInfo:
		return "NBD_REP_INFO"
	case repMetaContext:
		return "NBD_REP_META_CONTEXT"
	case repErrBit | 1:
		return "NBD_REP_ERR_UNSUP"
	case repErrBit | 2:
		return "NBD_REP_ERR_POLICY"
	case repErrBit | 3:
		return "NBD_REP_ERR_INVALID"
	case repErrBit | 4:
		return "NBD_REP_ERR_PLATFORM"
	case repErrBit | 5:
		return "NBD_REP_ERR_TLS_REQD"
	case repErrBit | 6:
		return "NBD_REP_ERR_UNKNOWN"
	case repErrBit | 7:
		return "NBD_REP_ERR_SHUTDOWN"
	case repErrBit | 8:
		return "NBD_REP_ERR_BLOCK_SIZE_REQD"
	case repErrBit | 9:
		return "NBD_REP_ERR_TOO_BIG"
	case repErrBit | 10:
		return "NBD_REP_ERR_EXT_HEADER_REQD"
	}
	return fmt.Sprintf("reply type %#x", rep)
}

func cmdName(cmd uint16) string {
	switch cmd {
	case cmdRead:
		return "read"
	case cmdWrite:
		return "write"
	case cmdFlush:
		return "flush"
	case cmdWriteZeroes:
		return "write zeroes"
	case cmdBlockStatus:
		return "block status"
	case cmdDisc:
		return "disconnect"
	}
	return fmt.Sprintf("command %d", cmd)
}
