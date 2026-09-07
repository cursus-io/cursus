package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateClientAuthenticationRequiresExplicitPermissions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableSASL = true
	cfg.SASLUsers = []SASLUser{{Principal: "operator", Token: "secret"}}

	if err := cfg.ValidateClientAuthentication(); err == nil || !strings.Contains(err.Error(), "requires at least one permission") {
		t.Fatalf("ValidateClientAuthentication() error = %v, want missing permission", err)
	}
}

func TestOverrideEnvSASLUsersAcceptsStrictJSONSecret(t *testing.T) {
	t.Setenv("TEST_SASL_USERS", `[{"principal":"processor","token":"secret","permissions":["topic.read","group"]}]`)
	var users []SASLUser
	require.NoError(t, overrideEnvSASLUsers(&users, "TEST_SASL_USERS"))
	require.Equal(t, []SASLUser{{Principal: "processor", Token: "secret", Permissions: []string{"topic.read", "group"}}}, users)

	t.Setenv("TEST_SASL_USERS", `[{"principal":"processor","token":"secret","permissions":["group"],"unexpected":true}]`)
	require.ErrorContains(t, overrideEnvSASLUsers(&users, "TEST_SASL_USERS"), "unknown field")
	t.Setenv("TEST_SASL_USERS", `[{"principal":"processor"}] trailing`)
	require.ErrorContains(t, overrideEnvSASLUsers(&users, "TEST_SASL_USERS"), "trailing data")
}

func TestValidateClientAuthenticationAcceptsKnownPermissions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableSASL = true
	cfg.SASLUsers = []SASLUser{{
		Principal:   "processor",
		Token:       "secret",
		Permissions: []string{"topic.read", "topic.write", "group", "transaction"},
	}}

	if err := cfg.ValidateClientAuthentication(); err != nil {
		t.Fatalf("ValidateClientAuthentication() error = %v", err)
	}
}

func TestOverrideEnvSASLUsersRequiresPermissions(t *testing.T) {
	t.Setenv("TEST_SASL_USERS", "operator:secret:admin|group")
	var users []SASLUser
	if err := overrideEnvSASLUsers(&users, "TEST_SASL_USERS"); err != nil {
		t.Fatalf("overrideEnvSASLUsers() error = %v", err)
	}
	if len(users) != 1 || len(users[0].Permissions) != 2 || users[0].Permissions[1] != "group" {
		t.Fatalf("overrideEnvSASLUsers() users = %#v", users)
	}

	t.Setenv("TEST_SASL_USERS", "operator:secret")
	if err := overrideEnvSASLUsers(&users, "TEST_SASL_USERS"); err == nil {
		t.Fatal("overrideEnvSASLUsers() accepted an entry without permissions")
	}
}
