//go:build !e2e_faults

package controller

import "github.com/cursus-io/cursus/pkg/types"

func injectedReplicaAppendSkip(string, int, []types.Message) bool { return false }

func injectedReplicaAppendFailure(string, int, []types.Message) bool { return false }

func injectedReplicaCatchupError(string) error { return nil }
