package server

import (
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/cursus-io/cursus/util"
)

const registrationLogInterval = time.Minute

// localRegistrationError preserves the final transport error for typed TLS
// diagnostics while retaining the local failure and attempted leader as context.
type localRegistrationError struct {
	local, final error
	leader       string
}

func (e *localRegistrationError) Error() string {
	return fmt.Sprintf("leader=%q: %v (local registration: %v)", e.leader, e.final, e.local)
}

func (e *localRegistrationError) Unwrap() error { return e.final }

// registrationFailureLog belongs to one registration worker. Log throttling
// never changes its retry schedule or the FSM condition for successful recovery.
type registrationFailureLog struct {
	key         string
	first, last time.Time
	attempts    uint64
	suppressed  uint64
}

func (s *registrationFailureLog) failed(now time.Time, err error) {
	key := registrationFailureKey(err)
	if s.attempts == 0 {
		s.first = now
	}
	s.attempts++
	if s.attempts == 1 || key != s.key || now.Sub(s.last) >= registrationLogInterval {
		util.Warn("local process registration failed; retrying: attempts=%d elapsed=%s suppressed=%d error=%v", s.attempts, now.Sub(s.first), s.suppressed, err)
		s.key, s.last, s.suppressed = key, now, 0
		return
	}
	s.suppressed++
}

func (s *registrationFailureLog) recovered(now time.Time) {
	if s.attempts == 0 {
		return
	}
	util.Info("local process registration recovered: attempts=%d elapsed=%s suppressed=%d; current registration observed in local FSM", s.attempts, now.Sub(s.first), s.suppressed)
	*s = registrationFailureLog{}
}

// registrationFailureKey excludes changing clock text in certificate validity
// errors and ephemeral local ports in network errors, retaining the full error
// in emitted logs. A different leader, certificate or failure still logs at once.
func registrationFailureKey(err error) string {
	var registration *localRegistrationError
	if errors.As(err, &registration) {
		return registration.leader + ":" + registrationFailureKey(registration.local) + ":" + registrationFailureKey(registration.final)
	}
	var invalid x509.CertificateInvalidError
	if errors.As(err, &invalid) && invalid.Reason == x509.Expired && invalid.Cert != nil {
		return fmt.Sprintf("certificate-validity:%x:%s:%s", sha256.Sum256(invalid.Cert.Raw), invalid.Cert.NotBefore, invalid.Cert.NotAfter)
	}
	var network *net.OpError
	if errors.As(err, &network) {
		return fmt.Sprintf("%s:%s:%v:%s", network.Op, network.Net, network.Addr, registrationFailureKey(network.Err))
	}
	return err.Error()
}
