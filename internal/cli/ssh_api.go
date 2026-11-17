package cli

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

const (
	// sshKeyPathPrefix is the v1 API sshkey path.
	sshKeyPathPrefix = "/api/v1/sandboxes/"
	// sshKeySuccessType is the pinned success discriminator.
	sshKeySuccessType = "ssh_key.configured"
	// sshAPIRequestTimeout bounds the key installation call so a hung server
	// cannot block an interactive command forever.
	sshAPIRequestTimeout = 30 * time.Second
)

// sshKeyResponse is the CLI view of the pinned sshKey success envelope. The
// two nullable members are pointers so an explicit null is distinguishable
// from an empty string.
type sshKeyResponse struct {
	OK          bool    `json:"ok"`
	Type        string  `json:"type"`
	MachineIP   *string `json:"machineIp"`
	SSHUser     string  `json:"sshUser"`
	SSHEndpoint *string `json:"sshEndpoint"`
	HostKey     string  `json:"hostKey"`
}

// sshTarget is the validated connection target of one sandbox. Host is always
// bare (no brackets) so callers can render IPv6 correctly.
type sshTarget struct {
	Host    string
	Port    int
	User    string
	HostKey sshHostKey
}

// installSSHKey posts the managed public key and validates the response. It
// requires exactly HTTP 200 with the pinned ok/type envelope; every other
// status is decoded as a v1 API error so the caller reports the server code.
func installSSHKey(ctx context.Context, client APIClient, sandboxID, publicKey string) (sshTarget, error) {
	if client == nil {
		return sshTarget{}, sshInvalidResponse("ssh key request has no API client")
	}
	if !ids.ValidSandbox(sandboxID) {
		return sshTarget{}, sshInvalidResponse("ssh key request has an invalid sandbox id")
	}
	if publicKey == "" || strings.ContainsAny(publicKey, "\r\n\x00") {
		return sshTarget{}, sshKeyInvalid("managed ssh public key is not valid")
	}
	rawBody, err := json.Marshal(struct {
		Key string `json:"key"`
	}{Key: publicKey})
	if err != nil {
		return sshTarget{}, sshInvalidResponse("encode ssh key request failed")
	}

	requestCtx, cancel := context.WithTimeout(ctx, sshAPIRequestTimeout)
	defer cancel()
	path := sshKeyPathPrefix + url.PathEscape(sandboxID) + "/sshkey"
	req, err := client.NewRequest(requestCtx, http.MethodPost, path, bytes.NewReader(rawBody))
	if err != nil {
		return sshTarget{}, sshInvalidResponse("build ssh key request failed")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return sshTarget{}, ctx.Err()
		}
		return sshTarget{}, err
	}
	defer resp.Body.Close()
	if err := ctx.Err(); err != nil {
		return sshTarget{}, err
	}
	raw, err := readBounded(resp.Body)
	if err != nil {
		if ctx.Err() != nil {
			return sshTarget{}, ctx.Err()
		}
		return sshTarget{}, sshInvalidResponse("read ssh key response failed")
	}
	if err := ctx.Err(); err != nil {
		return sshTarget{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return sshTarget{}, decodeAPIError(resp.StatusCode, raw)
	}
	if !execHasMediaType(resp.Header, "application/json") {
		return sshTarget{}, sshInvalidResponse("ssh key response has an unexpected media type")
	}
	var response sshKeyResponse
	if err := json.Unmarshal(raw, &response); err != nil ||
		!response.OK || response.Type != sshKeySuccessType {
		return sshTarget{}, sshInvalidResponse("ssh key response is invalid")
	}
	target, err := sshTargetFromResponse(response)
	if err != nil {
		return sshTarget{}, err
	}
	if err := ctx.Err(); err != nil {
		return sshTarget{}, err
	}
	return target, nil
}

// sshTargetFromResponse applies the pinned target rules: sshEndpoint wins when
// present (it names a forwarding host:port and machineIp may be null), else
// machineIp with port 22 is required.
func sshTargetFromResponse(response sshKeyResponse) (sshTarget, error) {
	if err := validateSSHUser(response.SSHUser); err != nil {
		return sshTarget{}, sshInvalidResponse("ssh key response has an invalid user")
	}
	hostKey, err := parseSSHHostKey(response.HostKey)
	if err != nil {
		return sshTarget{}, sshInvalidResponse("ssh key response has an invalid host key")
	}
	host := ""
	port := 22
	if response.SSHEndpoint != nil && *response.SSHEndpoint != "" {
		host, port, err = splitSSHEndpoint(*response.SSHEndpoint)
		if err != nil {
			return sshTarget{}, sshInvalidResponse("ssh key response has an invalid endpoint")
		}
	} else {
		if response.MachineIP == nil || *response.MachineIP == "" {
			return sshTarget{}, sshInvalidResponse("ssh key response has no SSH address")
		}
		host = trimSSHHostBrackets(*response.MachineIP)
	}
	if err := validateSSHHost(host); err != nil {
		return sshTarget{}, sshInvalidResponse("ssh key response has an invalid host")
	}
	return sshTarget{Host: host, Port: port, User: response.SSHUser, HostKey: hostKey}, nil
}

// validateSSHUser accepts a conservative ASCII user name. It rejects a leading
// dash so the value can never be read as an ssh option when rendered.
func validateSSHUser(user string) error {
	if user == "" || len(user) > 64 || user[0] == '-' {
		return errSSHHostKeyInvalid
	}
	for i := 0; i < len(user); i++ {
		c := user[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return errSSHHostKeyInvalid
		}
	}
	return nil
}

// validateSSHHost accepts an IP literal or a conservative DNS host name. It
// rejects whitespace, control bytes, options (leading dash) and path syntax.
func validateSSHHost(host string) error {
	if host == "" || len(host) > 253 || host[0] == '-' {
		return errSSHHostKeyInvalid
	}
	if net.ParseIP(host) != nil {
		return nil
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return errSSHHostKeyInvalid
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			switch {
			case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-':
			default:
				return errSSHHostKeyInvalid
			}
		}
	}
	return nil
}

// splitSSHEndpoint parses the pinned host:port endpoint. IPv6 must use
// brackets; the port must be ASCII decimal digits in 1..65535.
func splitSSHEndpoint(endpoint string) (string, int, error) {
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" {
		return "", 0, errSSHHostKeyInvalid
	}
	port, err := parseSSHPort(portText)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}

// parseSSHPort accepts only ASCII decimal digits and a valid port range.
func parseSSHPort(text string) (int, error) {
	if text == "" || len(text) > 5 {
		return 0, errSSHHostKeyInvalid
	}
	port := 0
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c < '0' || c > '9' {
			return 0, errSSHHostKeyInvalid
		}
		port = port*10 + int(c-'0')
	}
	if port < 1 || port > 65535 {
		return 0, errSSHHostKeyInvalid
	}
	return port, nil
}

// trimSSHHostBrackets removes one surrounding [] pair from a machineIp value.
func trimSSHHostBrackets(host string) string {
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		return host[1 : len(host)-1]
	}
	return host
}

// sshInvalidResponse is the safe malformed-response error.
func sshInvalidResponse(message string) error {
	return &CLIError{Code: "invalid_response", Message: message}
}

// sshKeyInvalid is the safe unusable-key error.
func sshKeyInvalid(message string) error {
	return &CLIError{Code: "ssh_key_invalid", Message: message}
}

// sshKeyError is the safe key filesystem error.
func sshKeyError(message string) error {
	return &CLIError{Code: "ssh_key_error", Message: message}
}

// sshHostKeyError is the safe known-hosts filesystem error.
func sshHostKeyError(message string) error {
	return &CLIError{Code: "ssh_host_key_error", Message: message}
}
