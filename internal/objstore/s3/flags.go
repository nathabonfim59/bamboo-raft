// Object-store flags shared by every binary (-s3.*), each with a
// BBRAFT_S3_* environment fallback. The secret access key never comes from a
// flag value: it is read from BBRAFT_S3_SECRET_ACCESS_KEY or from a file
// (regular, mode 0600 or stricter).
package s3

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// maxSecretBytes bounds a secret read from an environment variable or a
// file; real keys are short, so anything larger is a configuration mistake.
const maxSecretBytes = 8192

// Flags are the object-store flags every binary registers (-s3.*), each with
// a BBRAFT_S3_* fallback. Register them on a flag set, parse it, then call
// Config. A flag set on the command line wins over its environment variable.
type Flags struct {
	fs *flag.FlagSet

	endpoint            string
	region              string
	bucket              string
	pathStyle           bool
	accessKeyID         string
	createBucket        bool
	locationHint        string
	secretAccessKeyFile string
}

// RegisterFlags registers the -s3.* flags on fs and returns their holder.
func RegisterFlags(fs *flag.FlagSet) *Flags {
	f := &Flags{fs: fs}
	fs.StringVar(&f.endpoint, "s3.endpoint", "",
		"S3 endpoint such as http://127.0.0.1:3900; empty means AWS S3 (env BBRAFT_S3_ENDPOINT)")
	fs.StringVar(&f.region, "s3.region", "",
		"bucket region: the service's configured region, \"auto\" on R2, the AWS region (env BBRAFT_S3_REGION)")
	fs.StringVar(&f.bucket, "s3.bucket", "",
		"bucket holding chunks and manifests; empty disables the object store (env BBRAFT_S3_BUCKET)")
	fs.BoolVar(&f.pathStyle, "s3.path-style", false,
		"address the bucket in the URL path instead of a virtual host (env BBRAFT_S3_PATH_STYLE)")
	fs.StringVar(&f.accessKeyID, "s3.access-key-id", "",
		"access key id; the matching secret never comes from a flag (env BBRAFT_S3_ACCESS_KEY_ID)")
	fs.BoolVar(&f.createBucket, "s3.create-bucket", false,
		"create the bucket when it does not exist (env BBRAFT_S3_CREATE_BUCKET)")
	fs.StringVar(&f.locationHint, "s3.location-hint", "",
		"location hint for a created bucket (R2 only) (env BBRAFT_S3_LOCATION_HINT)")
	fs.StringVar(&f.secretAccessKeyFile, "s3.secret-access-key-file", "",
		"file holding the secret access key: a regular file, mode 0600 or stricter (env BBRAFT_S3_SECRET_ACCESS_KEY_FILE; BBRAFT_S3_SECRET_ACCESS_KEY is the fallback)")
	return f
}

// Config resolves the flags, then BBRAFT_S3_* through getenv (os.LookupEnv in
// production). configured is false when no s3 setting is set anywhere,
// meaning the object store is off: Config is then zero and err is nil. A
// bucket that is unset while any other setting is set is an error, not "off":
// that is a half-applied configuration (for example an ignored environment
// file that left only the credential variable) and the node must not start
// quietly without its store.
//
// Errors name the flag or variable at fault, never a secret value or a
// signature.
func (f *Flags) Config(getenv func(string) (string, bool)) (cfg Config, configured bool, err error) {
	if getenv == nil {
		getenv = func(string) (string, bool) { return "", false }
	}
	set := make(map[string]bool, 8)
	if f.fs != nil {
		f.fs.Visit(func(fl *flag.Flag) { set[fl.Name] = true })
	}

	var errs []error
	str := func(name, fromFlag string) string {
		if set[name] {
			return fromFlag
		}
		if v, ok := getenv(envName(name)); ok {
			return v
		}
		return fromFlag
	}
	boolean := func(name string, fromFlag bool) bool {
		if set[name] {
			return fromFlag
		}
		v, ok := getenv(envName(name))
		if !ok {
			return fromFlag
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s is not a boolean", envName(name)))
			return fromFlag
		}
		return b
	}

	cfg = Config{
		Endpoint:     str("s3.endpoint", f.endpoint),
		Region:       str("s3.region", f.region),
		Bucket:       str("s3.bucket", f.bucket),
		PathStyle:    boolean("s3.path-style", f.pathStyle),
		AccessKeyID:  str("s3.access-key-id", f.accessKeyID),
		CreateBucket: boolean("s3.create-bucket", f.createBucket),
		LocationHint: str("s3.location-hint", f.locationHint),
	}
	secretFile := str("s3.secret-access-key-file", f.secretAccessKeyFile)
	envSecret, envSecretSet := getenv("BBRAFT_S3_SECRET_ACCESS_KEY")

	if cfg.Bucket == "" {
		// Off only when nothing else is set either. A malformed boolean is
		// not looked at here: only which settings are present matters.
		var present []string
		for _, name := range incompleteCheckedSettings {
			if set[name] {
				present = append(present, settingName(name))
				continue
			}
			if v, ok := getenv(envName(name)); ok && v != "" {
				present = append(present, settingName(name))
			}
		}
		if v, ok := getenv("BBRAFT_S3_SECRET_ACCESS_KEY"); ok && v != "" {
			present = append(present, "BBRAFT_S3_SECRET_ACCESS_KEY")
		}
		if len(present) > 0 {
			return Config{}, false, fmt.Errorf("object store settings are incomplete: -s3.bucket (%s) is not set but %s %s set",
				envName("s3.bucket"), strings.Join(present, ", "), map[bool]string{true: "is", false: "are"}[len(present) == 1])
		}
		return Config{}, false, nil
	}
	if envSecretSet && secretFile != "" {
		errs = append(errs, errors.New("set only one of BBRAFT_S3_SECRET_ACCESS_KEY and -s3.secret-access-key-file (BBRAFT_S3_SECRET_ACCESS_KEY_FILE)"))
	}
	switch {
	case secretFile != "":
		secret, err := readSecretFile(secretFile)
		if err != nil {
			errs = append(errs, err)
		}
		cfg.SecretAccessKey = secret
	case envSecretSet:
		if strings.TrimSpace(envSecret) == "" {
			errs = append(errs, errors.New("BBRAFT_S3_SECRET_ACCESS_KEY is set but empty"))
		}
		cfg.SecretAccessKey = trimNewline(envSecret)
	default:
		errs = append(errs, errors.New("no secret access key: set BBRAFT_S3_SECRET_ACCESS_KEY or pass -s3.secret-access-key-file (BBRAFT_S3_SECRET_ACCESS_KEY_FILE)"))
	}
	if cfg.AccessKeyID == "" {
		errs = append(errs, errors.New("s3.bucket is set but no access key id: pass -s3.access-key-id or set BBRAFT_S3_ACCESS_KEY_ID"))
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, false, err
	}
	return cfg, true, nil
}

// incompleteCheckedSettings are the settings whose presence without a bucket
// is a misconfiguration (the secret variable is checked separately: it has no
// flag).
var incompleteCheckedSettings = []string{
	"s3.endpoint", "s3.region", "s3.path-style", "s3.access-key-id",
	"s3.create-bucket", "s3.location-hint", "s3.secret-access-key-file",
}

// settingName is how an error names a setting: its flag and its variable.
func settingName(flagName string) string {
	return "-" + flagName + " (" + envName(flagName) + ")"
}

// envName maps a flag name to its BBRAFT_* variable: dots and dashes become
// underscores, upper-cased.
func envName(flagName string) string {
	rep := strings.NewReplacer(".", "_", "-", "_")
	return "BBRAFT_" + strings.ToUpper(rep.Replace(flagName))
}

// readSecretFile reads a secret from a regular file with private
// permissions. The descriptor itself is validated, not the path: O_NOFOLLOW
// refuses a symlink swapped in after any lstat, O_NONBLOCK keeps a FIFO or
// device from blocking the open, and the read is bounded.
func readSecretFile(path string) (string, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return "", fmt.Errorf("secret file %s is a symlink; refusing", path)
		}
		return "", fmt.Errorf("secret file %s: %w", path, err)
	}
	f := os.NewFile(uintptr(fd), path)
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("secret file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("secret file %s is not a regular file", path)
	}
	if perm := info.Mode().Perm(); perm&^0o600 != 0 {
		return "", fmt.Errorf("secret file %s has mode %04o; must be 0600 or stricter", path, perm)
	}
	if info.Size() > maxSecretBytes {
		return "", fmt.Errorf("secret file %s is %d bytes; at most %d", path, info.Size(), maxSecretBytes)
	}
	b, err := io.ReadAll(io.LimitReader(f, maxSecretBytes+1))
	if err != nil {
		return "", fmt.Errorf("secret file %s: %w", path, err)
	}
	if len(b) > maxSecretBytes {
		return "", fmt.Errorf("secret file %s exceeds %d bytes", path, maxSecretBytes)
	}
	secret := trimNewline(string(b))
	if secret == "" {
		return "", fmt.Errorf("secret file %s is empty", path)
	}
	return secret, nil
}

// trimNewline removes one trailing newline (and its carriage return) from a
// secret, the way editors and secret files write it.
func trimNewline(s string) string {
	s = strings.TrimSuffix(s, "\n")
	return strings.TrimSuffix(s, "\r")
}
