//go:build !e2e_faults

package controller

import (
	"context"
	"github.com/cursus-io/cursus/pkg/types"
)

func injectedReplicaAppendSkip(string, int, []types.Message) bool { return false }

func injectedReplicaAppendFailure(string, int, []types.Message) bool { return false }

func injectedReplicaCatchupError(string) error { return nil }

func injectedReplicaResponseDelay(context.Context, string) error { return nil }

func injectedReplicaCatchupCrash(string, int) {}
