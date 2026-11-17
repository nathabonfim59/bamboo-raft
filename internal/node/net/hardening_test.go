package nodenet

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

// hardeningTemplatePath is the Ansible template that owns the host firewall
// (infra/ansible/roles/hardening). This package must never edit it; the
// privileged coexistence test loads the real rendered file to prove that the
// node's own tables work behind it.
const hardeningTemplatePath = "../../../infra/ansible/roles/hardening/templates/nftables.conf.j2"

// hardeningVars are the role variables that affect the rendered rules.
type hardeningVars struct {
	// TrustedInterfaces are accepted unconditionally on the input hook.
	TrustedInterfaces []string
	// OpenUDPPorts are extra inbound UDP ports.
	OpenUDPPorts []int
	// VMBridgeInterfaces are the bridges the forward chain accepts IPv4 on.
	VMBridgeInterfaces []string
}

// renderHardeningTemplate reads and renders the role template with vars. It
// fails on any Jinja construct it does not understand, so a future template
// change cannot silently weaken the coexistence test.
func renderHardeningTemplate(t *testing.T, vars hardeningVars) string {
	t.Helper()
	src, err := os.ReadFile(hardeningTemplatePath)
	if err != nil {
		t.Fatalf("read hardening template: %v", err)
	}
	out, err := renderTemplate(string(src), vars)
	if err != nil {
		t.Fatalf("render hardening template: %v", err)
	}
	return out
}

// renderTemplate is a deliberately tiny Jinja subset: `{% for %}`/`{% endfor %}`,
// `{% if %}`/`{% endif %}`, `{{ var }}` and the `| default([])`, `| length` and
// `| join(', ')` filters used by the hardening role. Anything else is an
// error rather than a quietly wrong ruleset.
func renderTemplate(src string, vars hardeningVars) (string, error) {
	lines := strings.Split(src, "\n")
	out, _, err := renderBlock(lines, 0, nil, vars, "")
	return out, err
}

func renderBlock(lines []string, start int, ctx map[string]string, vars hardeningVars, stop string) (string, int, error) {
	var b strings.Builder
	for i := start; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		switch {
		case strings.HasPrefix(trimmed, "{% for "):
			directive := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "{% for "), "%}"))
			name, expr, ok := strings.Cut(directive, " in ")
			if !ok {
				return "", 0, fmt.Errorf("unsupported for directive %q", trimmed)
			}
			items, err := templateItems(strings.TrimSpace(expr), vars)
			if err != nil {
				return "", 0, err
			}
			end, err := templateBlockEnd(lines, i, "for")
			if err != nil {
				return "", 0, err
			}
			for _, item := range items {
				inner := ctx
				if inner == nil {
					inner = make(map[string]string, 1)
				}
				inner[strings.TrimSpace(name)] = item
				rendered, _, err := renderBlock(lines[i+1:end], 0, inner, vars, "")
				if err != nil {
					return "", 0, err
				}
				b.WriteString(rendered)
			}
			i = end
		case strings.HasPrefix(trimmed, "{% if "):
			cond := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "{% if "), "%}"))
			emit, err := templateCondition(cond, vars)
			if err != nil {
				return "", 0, err
			}
			end, err := templateBlockEnd(lines, i, "if")
			if err != nil {
				return "", 0, err
			}
			if emit {
				rendered, _, err := renderBlock(lines[i+1:end], 0, ctx, vars, "")
				if err != nil {
					return "", 0, err
				}
				b.WriteString(rendered)
			}
			i = end
		case trimmed == "{% endfor %}" || trimmed == "{% endif %}":
			want := stop
			if want == "" {
				return "", 0, fmt.Errorf("unexpected %q", trimmed)
			}
			if !strings.Contains(trimmed, want) {
				return "", 0, fmt.Errorf("unexpected %q inside %s block", trimmed, want)
			}
			return b.String(), i, nil
		case strings.HasPrefix(trimmed, "{%"):
			return "", 0, fmt.Errorf("unsupported template directive %q", trimmed)
		default:
			line, err := templateInterpolate(lines[i], ctx, vars)
			if err != nil {
				return "", 0, err
			}
			b.WriteString(line)
			b.WriteString("\n")
		}
	}
	if stop != "" {
		return "", 0, fmt.Errorf("missing %q", "{% end"+stop+" %}")
	}
	return b.String(), len(lines), nil
}

// templateBlockEnd finds the line of the end tag matching the block opener at
// start, honouring nesting.
func templateBlockEnd(lines []string, start int, kind string) (int, error) {
	depth := 0
	for i := start + 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		switch {
		case strings.HasPrefix(trimmed, "{% for "), strings.HasPrefix(trimmed, "{% if "):
			depth++
		case trimmed == "{% endfor %}", trimmed == "{% endif %}":
			if depth == 0 {
				if !strings.Contains(trimmed, kind) {
					return 0, fmt.Errorf("block at line %d closes with the wrong tag", start+1)
				}
				return i, nil
			}
			depth--
		}
	}
	return 0, fmt.Errorf("unterminated %s block at line %d", kind, start+1)
}

func templateItems(expr string, vars hardeningVars) ([]string, error) {
	switch strings.TrimSpace(strings.TrimSuffix(expr, "| default([])")) {
	case "hardening_trusted_interfaces":
		return vars.TrustedInterfaces, nil
	case "hardening_vm_bridge_interfaces":
		return vars.VMBridgeInterfaces, nil
	default:
		return nil, fmt.Errorf("unsupported for expression %q", expr)
	}
}

func templateCondition(cond string, vars hardeningVars) (bool, error) {
	switch strings.TrimSpace(cond) {
	case "hardening_open_udp_ports | length > 0":
		return len(vars.OpenUDPPorts) > 0, nil
	default:
		return false, fmt.Errorf("unsupported condition %q", cond)
	}
}

func templateInterpolate(line string, ctx map[string]string, vars hardeningVars) (string, error) {
	for {
		open := strings.Index(line, "{{")
		if open < 0 {
			return line, nil
		}
		closeIdx := strings.Index(line[open:], "}}")
		if closeIdx < 0 {
			return "", fmt.Errorf("unterminated interpolation in %q", line)
		}
		expr := strings.TrimSpace(line[open+2 : open+closeIdx])
		value, err := templateValue(expr, ctx, vars)
		if err != nil {
			return "", err
		}
		line = line[:open] + value + line[open+closeIdx+2:]
	}
}

func templateValue(expr string, ctx map[string]string, vars hardeningVars) (string, error) {
	if value, ok := ctx[expr]; ok {
		return value, nil
	}
	switch strings.TrimSpace(expr) {
	case "hardening_open_udp_ports | join(', ')":
		ports := make([]string, len(vars.OpenUDPPorts))
		for i, port := range vars.OpenUDPPorts {
			ports[i] = strconv.Itoa(port)
		}
		return strings.Join(ports, ", "), nil
	default:
		return "", fmt.Errorf("unsupported expression %q", expr)
	}
}

func TestRenderHardeningTemplate(t *testing.T) {
	src, err := os.ReadFile(hardeningTemplatePath)
	if err != nil {
		t.Fatalf("read hardening template: %v", err)
	}
	if !strings.Contains(string(src), "hardening_vm_bridge_interfaces") {
		// The template patch (the vm bridge allow-list) is a lead-owned change
		// of infra/ansible/roles/hardening. Until it is applied in this
		// checkout there is no bridge acceptance to render; the privileged
		// coexistence test requires it instead of skipping.
		t.Skip("hardening role does not yet accept hardening_vm_bridge_interfaces")
	}
	rendered := renderHardeningTemplate(t, hardeningVars{
		VMBridgeInterfaces: []string{"bbraft0"},
	})
	if !strings.Contains(rendered, "table inet bbraft_filter") ||
		!strings.Contains(rendered, "delete table inet bbraft_filter") {
		t.Fatal("rendered hardening ruleset does not replace its own table")
	}
	// The patched forward chain accepts IPv4 forwarded traffic on the bridge
	// in both directions; the node's own chain then drops what is not allowed.
	for _, want := range []string{
		"type filter hook forward priority filter; policy drop;",
		`meta nfproto ipv4 iifname "bbraft0" accept`,
		`meta nfproto ipv4 oifname "bbraft0" accept`,
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("rendered hardening ruleset is missing %q", want)
		}
	}
	if strings.Contains(rendered, "{{") || strings.Contains(rendered, "{%") {
		t.Error("rendered template still contains template syntax")
	}

	// The default empty bridge list keeps a host that routes nothing.
	empty := renderHardeningTemplate(t, hardeningVars{})
	if strings.Contains(empty, "iifname \"bbraft0\"") {
		t.Error("empty bridge list accepted a bridge")
	}
	if !strings.Contains(empty, "policy drop;") {
		t.Error("empty bridge list must keep the forward policy drop")
	}
}

func TestRenderTemplateRejectsUnknownDirectives(t *testing.T) {
	tests := []struct {
		name string
		src  string
	}{
		{"unknown directive", "{% include other.j2 %}\n"},
		{"unknown expression", "{{ hardening_secret }}\n"},
		{"unknown loop", "{% for x in hardening_nothing %}\n{% endfor %}\n"},
		{"unterminated", "{% for x in hardening_trusted_interfaces %}\n"},
		{"mismatched", "{% for x in hardening_trusted_interfaces %}\n{% endif %}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := renderTemplate(tt.src, hardeningVars{}); err == nil {
				t.Fatal("renderer accepted an unknown construct")
			}
		})
	}
}
