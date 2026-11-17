package compat

import (
	"encoding/json/v2"
	"net/http"
	"testing"
)

func errorEnvelopeBody(t *testing.T, status int, code string, mutate func(map[string]any)) []byte {
	t.Helper()
	value := map[string]any{
		"ok":        false,
		"type":      "sandbox.error",
		"status":    status,
		"code":      code,
		"message":   "message",
		"error":     map[string]any{"code": code, "message": "message", "status": status},
		"requestId": "req_example",
	}
	if mutate != nil {
		mutate(value)
	}
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal error envelope: %v", err)
	}
	return body
}

// withRetryAfter sets error.details.retryAfterSeconds to value, replacing an
// existing details object.
func withRetryAfter(value any) func(map[string]any) {
	return func(envelope map[string]any) {
		errorObject := envelope["error"].(map[string]any)
		errorObject["details"] = map[string]any{"retryAfterSeconds": value}
	}
}

func TestApprovedUndocumentedErrorResponses(t *testing.T) {
	validator := Load(t)
	for _, code := range []string{
		"invalid_json", "invalid_env", "invalid_name", "invalid_query", "unknown_environment",
	} {
		t.Run("create 400 "+code, func(t *testing.T) {
			body := errorEnvelopeBody(t, http.StatusBadRequest, code, nil)
			if err := validator.Response("create", http.StatusBadRequest, mediaType(jsonMediaType), body); err != nil {
				t.Errorf("approved create 400 %s did not validate: %v", code, err)
			}
		})
	}
	t.Run("create 400 fixture", func(t *testing.T) {
		if err := validator.Response("create", http.StatusBadRequest, mediaType(jsonMediaType), fixture(t, "error-invalid-json.json")); err != nil {
			t.Errorf("error-invalid-json did not validate: %v", err)
		}
	})
	t.Run("core 500 http_500", func(t *testing.T) {
		for _, operationID := range []string{
			"me", "sandboxes", "create", "get", "deleteSandbox", "getDeletionOperation", "createUploadUrl",
		} {
			t.Run(operationID, func(t *testing.T) {
				body := errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", nil)
				if err := validator.Response(operationID, http.StatusInternalServerError, mediaType(jsonMediaType), body); err != nil {
					t.Errorf("approved %s 500 did not validate: %v", operationID, err)
				}
			})
		}
	})
	t.Run("command 500 http_500", func(t *testing.T) {
		body := errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", nil)
		if err := validator.Response("command", http.StatusInternalServerError, mediaType(jsonMediaType), body); err != nil {
			t.Errorf("approved command 500 did not validate: %v", err)
		}
	})
	t.Run("command 500 fixture", func(t *testing.T) {
		if err := validator.Response("command", http.StatusInternalServerError, mediaType(jsonMediaType), fixture(t, "error-http-500.json")); err != nil {
			t.Errorf("error-http-500 did not validate: %v", err)
		}
	})
	t.Run("lifecycle 500 http_500", func(t *testing.T) {
		for _, operationID := range []string{"update", "stop", "getStop", "resume"} {
			t.Run(operationID, func(t *testing.T) {
				body := errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", nil)
				if err := validator.Response(operationID, http.StatusInternalServerError, mediaType(jsonMediaType), body); err != nil {
					t.Errorf("approved %s 500 did not validate: %v", operationID, err)
				}
			})
		}
	})
	t.Run("resume 400 approved", func(t *testing.T) {
		for _, code := range []string{"invalid_json", "invalid_env", "unknown_environment"} {
			t.Run(code, func(t *testing.T) {
				body := errorEnvelopeBody(t, http.StatusBadRequest, code, nil)
				if err := validator.Response("resume", http.StatusBadRequest, mediaType(jsonMediaType), body); err != nil {
					t.Errorf("approved resume 400 %s did not validate: %v", code, err)
				}
			})
		}
	})
	t.Run("command 502 sandbox_direct_failed", func(t *testing.T) {
		body := errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", nil)
		if err := validator.Response("command", http.StatusBadGateway, mediaType(jsonMediaType), body); err != nil {
			t.Errorf("approved command 502 did not validate: %v", err)
		}
	})
	t.Run("command 502 fixture", func(t *testing.T) {
		if err := validator.Response("command", http.StatusBadGateway, mediaType(jsonMediaType), fixture(t, "error-sandbox-direct-failed.json")); err != nil {
			t.Errorf("error-sandbox-direct-failed did not validate: %v", err)
		}
	})
	t.Run("command 503 out_of_capacity retry hint", func(t *testing.T) {
		body := errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", withRetryAfter(5))
		if err := validator.Response("command", http.StatusServiceUnavailable, mediaType(jsonMediaType), body); err != nil {
			t.Errorf("approved command 503 did not validate: %v", err)
		}
	})
	t.Run("command 503 fixture", func(t *testing.T) {
		if err := validator.Response("command", http.StatusServiceUnavailable, mediaType(jsonMediaType), fixture(t, "error-out-of-capacity.json")); err != nil {
			t.Errorf("error-out-of-capacity did not validate: %v", err)
		}
	})
	t.Run("commandStatus 502 sandbox_direct_failed", func(t *testing.T) {
		body := errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", nil)
		if err := validator.Response("commandStatus", http.StatusBadGateway, mediaType(jsonMediaType), body); err != nil {
			t.Errorf("approved commandStatus 502 did not validate: %v", err)
		}
	})
	t.Run("commandStatus 500 http_500", func(t *testing.T) {
		body := errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", nil)
		if err := validator.Response("commandStatus", http.StatusInternalServerError, mediaType(jsonMediaType), body); err != nil {
			t.Errorf("approved commandStatus 500 did not validate: %v", err)
		}
	})
	t.Run("attach and stop 500/502", func(t *testing.T) {
		for _, operationID := range []string{"attachCommand", "stopCommand"} {
			for _, test := range []struct {
				status int
				code   string
			}{
				{http.StatusInternalServerError, "http_500"},
				{http.StatusBadGateway, "sandbox_direct_failed"},
			} {
				t.Run(operationID+" "+test.code, func(t *testing.T) {
					body := errorEnvelopeBody(t, test.status, test.code, nil)
					if err := validator.Response(operationID, test.status, mediaType(jsonMediaType), body); err != nil {
						t.Errorf("approved %s %d %s did not validate: %v", operationID, test.status, test.code, err)
					}
				})
			}
		}
	})
	t.Run("file operations 401/404/409/500/502", func(t *testing.T) {
		fileOperations := []string{
			"readFile", "writeFile", "deleteFile", "listFiles",
			"getFileInfo", "createFolder", "moveFile", "setFilePermissions",
		}
		cases := []struct {
			status int
			codes  []string
		}{
			{http.StatusUnauthorized, []string{"unauthorized"}},
			{http.StatusNotFound, []string{"not_found"}},
			{http.StatusConflict, []string{"machine_not_running", "sandbox_not_ready", "sandbox_starting"}},
			{http.StatusInternalServerError, []string{"http_500"}},
			{http.StatusBadGateway, []string{"sandbox_direct_failed"}},
		}
		for _, operationID := range fileOperations {
			for _, test := range cases {
				for _, code := range test.codes {
					t.Run(operationID+" "+code, func(t *testing.T) {
						body := errorEnvelopeBody(t, test.status, code, nil)
						if err := validator.Response(operationID, test.status, mediaType(jsonMediaType), body); err != nil {
							t.Errorf("approved %s %d %s did not validate: %v", operationID, test.status, code, err)
						}
					})
				}
			}
		}
	})
	t.Run("file 409 fixture", func(t *testing.T) {
		if err := validator.Response("readFile", http.StatusConflict, mediaType(jsonMediaType), fixture(t, "error-machine-not-running.json")); err != nil {
			t.Errorf("error-machine-not-running did not validate: %v", err)
		}
	})
	t.Run("file 502 fixture", func(t *testing.T) {
		if err := validator.Response("deleteFile", http.StatusBadGateway, mediaType(jsonMediaType), fixture(t, "error-sandbox-direct-failed.json")); err != nil {
			t.Errorf("error-sandbox-direct-failed did not validate for deleteFile: %v", err)
		}
	})
	t.Run("createDownloadUrl 500 http_500", func(t *testing.T) {
		body := errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", nil)
		if err := validator.Response("createDownloadUrl", http.StatusInternalServerError, mediaType(jsonMediaType), body); err != nil {
			t.Errorf("approved createDownloadUrl 500 did not validate: %v", err)
		}
	})
	t.Run("sshKey approved responses", func(t *testing.T) {
		t.Run("200 success fixture", func(t *testing.T) {
			if err := validator.Response("sshKey", http.StatusOK, mediaType(jsonMediaType), fixture(t, "ssh-key-configured.json")); err != nil {
				t.Errorf("sshKey success fixture did not validate: %v", err)
			}
		})
		t.Run("500 http_500", func(t *testing.T) {
			body := errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", nil)
			if err := validator.Response("sshKey", http.StatusInternalServerError, mediaType(jsonMediaType), body); err != nil {
				t.Errorf("approved sshKey 500 did not validate: %v", err)
			}
		})
		t.Run("502 sandbox_direct_failed", func(t *testing.T) {
			body := errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", nil)
			if err := validator.Response("sshKey", http.StatusBadGateway, mediaType(jsonMediaType), body); err != nil {
				t.Errorf("approved sshKey 502 did not validate: %v", err)
			}
		})
		t.Run("503 ssh_host_key_unavailable retry hint", func(t *testing.T) {
			body := errorEnvelopeBody(t, http.StatusServiceUnavailable, "ssh_host_key_unavailable", withRetryAfter(5))
			if err := validator.Response("sshKey", http.StatusServiceUnavailable, mediaType(jsonMediaType), body); err != nil {
				t.Errorf("approved sshKey 503 did not validate: %v", err)
			}
		})
		t.Run("503 fixture", func(t *testing.T) {
			if err := validator.Response("sshKey", http.StatusServiceUnavailable, mediaType(jsonMediaType), fixture(t, "error-ssh-host-key-unavailable.json")); err != nil {
				t.Errorf("ssh host-key fixture did not validate: %v", err)
			}
		})
	})
	t.Run("declared create 503 still validates normally", func(t *testing.T) {
		if err := validator.Response("create", http.StatusServiceUnavailable, mediaType(jsonMediaType), fixture(t, "error-no-ready-machine.json")); err != nil {
			t.Errorf("declared create 503 did not validate: %v", err)
		}
	})
}

func TestUndocumentedErrorBoundaries(t *testing.T) {
	validator := Load(t)
	tests := []struct {
		name   string
		op     string
		status int
		header http.Header
		body   []byte
	}{
		{
			name:   "create unlisted code",
			op:     "create",
			status: http.StatusBadRequest,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadRequest, "invalid_timeout", nil),
		},
		{
			name:   "create code valid elsewhere but not approved",
			op:     "create",
			status: http.StatusBadRequest,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadRequest, "sandbox_starting", nil),
		},
		{
			name:   "create top-level status mismatch",
			op:     "create",
			status: http.StatusBadRequest,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadRequest, "invalid_json", func(value map[string]any) {
				value["status"] = http.StatusConflict
			}),
		},
		{
			name:   "create nested status mismatch",
			op:     "create",
			status: http.StatusBadRequest,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadRequest, "invalid_json", func(value map[string]any) {
				value["error"].(map[string]any)["status"] = http.StatusConflict
			}),
		},
		{
			name:   "create nested code mismatch",
			op:     "create",
			status: http.StatusBadRequest,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadRequest, "invalid_json", func(value map[string]any) {
				value["error"].(map[string]any)["code"] = "invalid_env"
			}),
		},
		{
			name:   "create ok true",
			op:     "create",
			status: http.StatusBadRequest,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadRequest, "invalid_json", func(value map[string]any) {
				value["ok"] = true
			}),
		},
		{
			name:   "create type not sandbox.error",
			op:     "create",
			status: http.StatusBadRequest,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadRequest, "invalid_json", func(value map[string]any) {
				value["type"] = "sandbox.info"
			}),
		},
		{
			name:   "create missing requestId",
			op:     "create",
			status: http.StatusBadRequest,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadRequest, "invalid_json", func(value map[string]any) {
				delete(value, "requestId")
			}),
		},
		{
			name:   "create missing nested error",
			op:     "create",
			status: http.StatusBadRequest,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadRequest, "invalid_json", func(value map[string]any) {
				delete(value, "error")
			}),
		},
		{
			name:   "create wrong content type",
			op:     "create",
			status: http.StatusBadRequest,
			header: mediaType("text/plain"),
			body:   errorEnvelopeBody(t, http.StatusBadRequest, "invalid_json", nil),
		},
		{
			name:   "create missing content type",
			op:     "create",
			status: http.StatusBadRequest,
			header: http.Header{},
			body:   errorEnvelopeBody(t, http.StatusBadRequest, "invalid_json", nil),
		},
		{
			name:   "create 404 has no exception",
			op:     "create",
			status: http.StatusNotFound,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusNotFound, "invalid_json", nil),
		},
		{
			name:   "create 201 undocumented success",
			op:     "create",
			status: http.StatusCreated,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusCreated, "invalid_json", nil),
		},
		{
			name:   "command stale direct_failed rejected",
			op:     "command",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadGateway, "direct_failed", nil),
		},
		{
			name:   "command ok true",
			op:     "command",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", func(value map[string]any) {
				value["ok"] = true
			}),
		},
		{
			name:   "command status mismatch",
			op:     "command",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", func(value map[string]any) {
				value["status"] = http.StatusInternalServerError
				value["error"].(map[string]any)["status"] = http.StatusInternalServerError
			}),
		},
		{
			name:   "command 501 not approved",
			op:     "command",
			status: http.StatusNotImplemented,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusNotImplemented, "sandbox_direct_failed", nil),
		},
		{
			name:   "command 201 undocumented success",
			op:     "command",
			status: http.StatusCreated,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusCreated, "sandbox_direct_failed", nil),
		},
		{
			name:   "commandStatus 503 out_of_capacity rejected",
			op:     "commandStatus",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", withRetryAfter(5)),
		},
		{
			name:   "commandStatus 502 stale code rejected",
			op:     "commandStatus",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadGateway, "direct_failed", nil),
		},
		{
			name:   "commandStatus 502 unlisted code rejected",
			op:     "commandStatus",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadGateway, "out_of_capacity", withRetryAfter(5)),
		},
		{
			name:   "commandStatus 503 undocumented rejected",
			op:     "commandStatus",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "sandbox_direct_failed", nil),
		},
		{
			name:   "commandStatus 500 wrong code",
			op:     "commandStatus",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusInternalServerError, "sandbox_direct_failed", nil),
		},
		{
			name:   "commandStatus 500 malformed envelope",
			op:     "commandStatus",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", func(value map[string]any) {
				value["ok"] = true
			}),
		},
		{
			name:   "commandStatus 502 malformed envelope",
			op:     "commandStatus",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", func(value map[string]any) {
				value["error"].(map[string]any)["code"] = "out_of_capacity"
			}),
		},
		{
			name:   "attachCommand 503 out_of_capacity rejected",
			op:     "attachCommand",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", withRetryAfter(5)),
		},
		{
			name:   "attachCommand 502 stale code rejected",
			op:     "attachCommand",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadGateway, "direct_failed", nil),
		},
		{
			name:   "attachCommand 500 wrong code",
			op:     "attachCommand",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusInternalServerError, "sandbox_direct_failed", nil),
		},
		{
			name:   "attachCommand 501 not approved",
			op:     "attachCommand",
			status: http.StatusNotImplemented,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusNotImplemented, "sandbox_direct_failed", nil),
		},
		{
			name:   "stopCommand 503 out_of_capacity rejected",
			op:     "stopCommand",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", withRetryAfter(5)),
		},
		{
			name:   "stopCommand 502 unlisted code rejected",
			op:     "stopCommand",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadGateway, "out_of_capacity", withRetryAfter(5)),
		},
		{
			name:   "stopCommand 500 wrong code",
			op:     "stopCommand",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusInternalServerError, "sandbox_direct_failed", nil),
		},
		{
			name:   "stopCommand 201 undocumented success",
			op:     "stopCommand",
			status: http.StatusCreated,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusCreated, "sandbox_direct_failed", nil),
		},
		{
			name:   "attachCommand 500 ok true",
			op:     "attachCommand",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", func(value map[string]any) {
				value["ok"] = true
			}),
		},
		{
			name:   "attachCommand 500 nested code mismatch",
			op:     "attachCommand",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", func(value map[string]any) {
				value["error"].(map[string]any)["code"] = "sandbox_direct_failed"
			}),
		},
		{
			name:   "attachCommand 502 ok true",
			op:     "attachCommand",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", func(value map[string]any) {
				value["ok"] = true
			}),
		},
		{
			name:   "attachCommand 502 nested code mismatch",
			op:     "attachCommand",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", func(value map[string]any) {
				value["error"].(map[string]any)["code"] = "http_500"
			}),
		},
		{
			name:   "stopCommand 500 ok true",
			op:     "stopCommand",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", func(value map[string]any) {
				value["ok"] = true
			}),
		},
		{
			name:   "stopCommand 500 nested code mismatch",
			op:     "stopCommand",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", func(value map[string]any) {
				value["error"].(map[string]any)["code"] = "sandbox_direct_failed"
			}),
		},
		{
			name:   "stopCommand 502 ok true",
			op:     "stopCommand",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", func(value map[string]any) {
				value["ok"] = true
			}),
		},
		{
			name:   "stopCommand 502 nested code mismatch",
			op:     "stopCommand",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", func(value map[string]any) {
				value["error"].(map[string]any)["code"] = "http_500"
			}),
		},
		{
			name:   "readFile 409 conflict rejected",
			op:     "readFile",
			status: http.StatusConflict,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusConflict, "conflict", nil),
		},
		{
			name:   "writeFile 409 unknown state code rejected",
			op:     "writeFile",
			status: http.StatusConflict,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusConflict, "already_exists", nil),
		},
		{
			name:   "readFile 503 out_of_capacity rejected",
			op:     "readFile",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", withRetryAfter(5)),
		},
		{
			name:   "writeFile 503 no_ready_machine rejected",
			op:     "writeFile",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "no_ready_machine", nil),
		},
		{
			name:   "readFile 502 http_500 rejected",
			op:     "readFile",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadGateway, "http_500", nil),
		},
		{
			name:   "readFile 401 wrong code rejected",
			op:     "readFile",
			status: http.StatusUnauthorized,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusUnauthorized, "sandbox_not_ready", nil),
		},
		{
			name:   "setFilePermissions 404 wrong code rejected",
			op:     "setFilePermissions",
			status: http.StatusNotFound,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusNotFound, "invalid_path", nil),
		},
		{
			name:   "moveFile 201 undocumented success",
			op:     "moveFile",
			status: http.StatusCreated,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusCreated, "sandbox_direct_failed", nil),
		},
		{
			name:   "readFile 500 nested status mismatch",
			op:     "readFile",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusInternalServerError, "http_500", func(value map[string]any) {
				value["error"].(map[string]any)["status"] = http.StatusBadGateway
			}),
		},
		{
			name:   "deleteFile 502 ok true rejected",
			op:     "deleteFile",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", func(value map[string]any) {
				value["ok"] = true
			}),
		},
		{
			name:   "createDownloadUrl 502 rejected",
			op:     "createDownloadUrl",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", nil),
		},
		{
			name:   "createDownloadUrl 503 rejected",
			op:     "createDownloadUrl",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", withRetryAfter(5)),
		},
		{
			name:   "createDownloadUrl 500 wrong code rejected",
			op:     "createDownloadUrl",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusInternalServerError, "sandbox_direct_failed", nil),
		},
		{
			name:   "sshKey 503 unlisted code rejected",
			op:     "sshKey",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", withRetryAfter(5)),
		},
		{
			name:   "sshKey 503 missing retry hint rejected",
			op:     "sshKey",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "ssh_host_key_unavailable", nil),
		},
		{
			name:   "sshKey 503 zero retry hint rejected",
			op:     "sshKey",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "ssh_host_key_unavailable", withRetryAfter(0)),
		},
		{
			name:   "sshKey 503 fractional retry hint rejected",
			op:     "sshKey",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "ssh_host_key_unavailable", withRetryAfter(1.5)),
		},
		{
			name:   "sshKey 503 string retry hint rejected",
			op:     "sshKey",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "ssh_host_key_unavailable", withRetryAfter("5")),
		},
		{
			name:   "sshKey 503 status mismatch rejected",
			op:     "sshKey",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusServiceUnavailable, "ssh_host_key_unavailable", func(value map[string]any) {
				withRetryAfter(5)(value)
				value["status"] = http.StatusBadGateway
			}),
		},
		{
			name:   "sshKey 502 stale code rejected",
			op:     "sshKey",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadGateway, "direct_failed", nil),
		},
		{
			name:   "sshKey 502 ok true rejected",
			op:     "sshKey",
			status: http.StatusBadGateway,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusBadGateway, "sandbox_direct_failed", func(value map[string]any) {
				value["ok"] = true
			}),
		},
		{
			name:   "sshKey 500 wrong code rejected",
			op:     "sshKey",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusInternalServerError, "sandbox_direct_failed", nil),
		},
		{
			name:   "sshKey 409 has no exception",
			op:     "sshKey",
			status: http.StatusConflict,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusConflict, "conflict", nil),
		},
		{
			name:   "sshKey 201 undocumented success",
			op:     "sshKey",
			status: http.StatusCreated,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusCreated, "ssh_host_key_unavailable", withRetryAfter(5)),
		},
		{
			name:   "resume unlisted 400 code",
			op:     "resume",
			status: http.StatusBadRequest,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadRequest, "sandbox_starting", nil),
		},
		{
			name:   "resume invalid_name 400 not approved",
			op:     "resume",
			status: http.StatusBadRequest,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusBadRequest, "invalid_name", nil),
		},
		{
			name:   "update 409 has no exception",
			op:     "update",
			status: http.StatusConflict,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusConflict, "conflict", nil),
		},
		{
			name:   "stop 409 has no exception",
			op:     "stop",
			status: http.StatusConflict,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusConflict, "conflict", nil),
		},
		{
			name:   "createUploadUrl 500 wrong code",
			op:     "createUploadUrl",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusInternalServerError, "sandbox_direct_failed", nil),
		},
		{
			name:   "command 500 wrong code",
			op:     "command",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusInternalServerError, "sandbox_direct_failed", nil),
		},
		{
			name:   "create 500 wrong code",
			op:     "create",
			status: http.StatusInternalServerError,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusInternalServerError, "sandbox_direct_failed", nil),
		},
		{
			name:   "command 503 wrong code",
			op:     "command",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "no_ready_machine", withRetryAfter(5)),
		},
		{
			name:   "command 503 missing retry hint",
			op:     "command",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", nil),
		},
		{
			name:   "command 503 string retry hint",
			op:     "command",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", withRetryAfter("5")),
		},
		{
			name:   "command 503 zero retry hint",
			op:     "command",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", withRetryAfter(0)),
		},
		{
			name:   "command 503 negative retry hint",
			op:     "command",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", withRetryAfter(-1)),
		},
		{
			name:   "command 503 fractional retry hint",
			op:     "command",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body:   errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", withRetryAfter(1.5)),
		},
		{
			name:   "command 503 status mismatch",
			op:     "command",
			status: http.StatusServiceUnavailable,
			header: mediaType(jsonMediaType),
			body: errorEnvelopeBody(t, http.StatusServiceUnavailable, "out_of_capacity", func(value map[string]any) {
				withRetryAfter(5)(value)
				value["status"] = http.StatusBadGateway
			}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validator.Response(test.op, test.status, test.header, test.body); err == nil {
				t.Fatal("validator accepted an unapproved error response")
			}
		})
	}
}
