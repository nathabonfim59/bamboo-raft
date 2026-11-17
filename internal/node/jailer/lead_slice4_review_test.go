package jailer

import "testing"

func TestLeadAdmissionMountOrderIsNotAuthority(t *testing.T) {
	const bad = "20 10 0:1 / /probe/mnt rw,nosuid,nodev,noexec shared:8 - tmpfs tmpfs rw\n"
	const good = "10 1 0:1 / /probe/mnt rw,nosuid,nodev,noexec - tmpfs tmpfs rw\n"
	for name, raw := range map[string]string{"top first": bad + good, "top last": good + bad} {
		t.Run(name, func(t *testing.T) {
			if err := checkAdmissionMount([]byte(raw), "/probe/mnt"); err == nil {
				t.Fatal("accepted shared effective mount based on listing order")
			}
		})
	}
}
