package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequireConfirmDelete(t *testing.T) {
	const target = "bx_23456789"

	request := httptest.NewRequest(http.MethodDelete, "/", nil)
	err := RequireConfirmDelete(request, target)
	if err == nil {
		t.Fatal("RequireConfirmDelete accepted a missing header")
	}
	confirmErr, ok := errors.AsType[*ConfirmDeleteError](err)
	if !ok || confirmErr.Reason != ConfirmDeleteMissing {
		t.Errorf("error = %T %v, want missing", err, err)
	}

	request.Header.Set(ConfirmDeleteHeader, "bx_zzzzzzzz")
	err = RequireConfirmDelete(request, target)
	confirmErr, ok = errors.AsType[*ConfirmDeleteError](err)
	if !ok || confirmErr.Reason != ConfirmDeleteMismatch {
		t.Errorf("error = %T %v, want mismatch", err, err)
	}

	request.Header.Set(ConfirmDeleteHeader, target)
	if err := RequireConfirmDelete(request, target); err != nil {
		t.Errorf("RequireConfirmDelete(exact) = %v", err)
	}

	for _, value := range []string{" " + target, target + " ", "BX_23456789"} {
		request.Header.Set(ConfirmDeleteHeader, value)
		if err := RequireConfirmDelete(request, target); err == nil {
			t.Errorf("RequireConfirmDelete accepted %q", value)
		}
	}
}

func TestConfirmDeleteErrorDoesNotEchoHeader(t *testing.T) {
	request := httptest.NewRequest(http.MethodDelete, "/", nil)
	request.Header.Set(ConfirmDeleteHeader, "secret-value")
	err := RequireConfirmDelete(request, "bx_23456789")
	if err == nil {
		t.Fatal("expected an error")
	}
	if contains := err.Error(); contains == "" || contains == "secret-value" {
		t.Errorf("error text leaks or is empty: %q", contains)
	}
}

func TestNoStore(t *testing.T) {
	handler := NoStore(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := w.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control inside handler = %q", got)
		}
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q", got)
	}
}

func TestChainOrder(t *testing.T) {
	var order []string
	middleware := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name+" before")
				next.ServeHTTP(w, r)
				order = append(order, name+" after")
			})
		}
	}
	handler := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "handler")
	}), middleware("outer"), middleware("inner"))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	want := []string{"outer before", "inner before", "handler", "inner after", "outer after"}
	if len(order) != len(want) {
		t.Fatalf("order = %v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}
