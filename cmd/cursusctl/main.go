// cursusctl is the bounded Wire v2 operator client. It intentionally runs one
// explicit command at a time so production runbooks retain their approval and
// audit boundaries.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
)

const defaultTimeout = 10 * time.Second

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("cursusctl", flag.ContinueOnError)
	flags.SetOutput(stderr)
	broker := flags.String("broker", "", "broker host:port (required)")
	compression := flags.String("compression", "none", "Wire v2 compression: none, gzip, snappy, lz4")
	timeout := flags.Duration("timeout", defaultTimeout, "dial, handshake, and command timeout")
	principal := flags.String("principal", "", "optional authenticated principal")
	authTokenEnv := flags.String("auth-token-env", "", "environment variable containing the principal token")
	tlsCA := flags.String("tls-ca", "", "optional PEM CA bundle; enables TLS")
	tlsServerName := flags.String("tls-server-name", "", "optional TLS certificate server name")
	tlsCert := flags.String("tls-cert", "", "optional TLS client certificate")
	tlsKey := flags.String("tls-key", "", "optional TLS client private key")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: cursusctl --broker host:port [options] COMMAND [key=value ...]")
		fmt.Fprintln(stderr, "Examples: cursusctl --broker broker:9000 LIST")
		fmt.Fprintln(stderr, "          cursusctl --broker broker:9000 CREATE topic=orders partitions=3")
		fmt.Fprintln(stderr, "          cursusctl --broker broker:9000 REGISTER_GROUP topic=orders group=workers")
	}
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*broker) == "" || flags.NArg() == 0 || *timeout <= 0 {
		flags.Usage()
		return 2
	}
	if (*principal == "") != (*authTokenEnv == "") {
		fmt.Fprintln(stderr, "--principal and --auth-token-env must be provided together")
		return 2
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		fmt.Fprintln(stderr, "--tls-cert and --tls-key must be provided together")
		return 2
	}
	command := strings.Join(flags.Args(), " ")
	if _, _, err := wire.ParseCommandText(command); err != nil {
		fmt.Fprintf(stderr, "invalid Wire v2 command: %v\n", err)
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	conn, err := dialBroker(ctx, *broker, *tlsCA, *tlsServerName, *tlsCert, *tlsKey)
	if err != nil {
		fmt.Fprintf(stderr, "connect broker: %v\n", err)
		return 1
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(*timeout)); err != nil {
		fmt.Fprintf(stderr, "set connection deadline: %v\n", err)
		return 1
	}
	client, err := wire.NewClientConn(conn, *compression)
	if err != nil {
		fmt.Fprintf(stderr, "negotiate Wire v2: %v\n", err)
		return 1
	}
	if *principal != "" {
		token := getenv(*authTokenEnv)
		if token == "" {
			fmt.Fprintf(stderr, "authentication token environment variable %q is empty\n", *authTokenEnv)
			return 2
		}
		if err := execute(client, "AUTH principal="+*principal+" token="+token, stdout); err != nil {
			fmt.Fprintf(stderr, "authenticate: %v\n", err)
			return 1
		}
	}
	if err := execute(client, command, stdout); err != nil {
		var brokerErr *wire.BrokerError
		if errors.As(err, &brokerErr) {
			fmt.Fprintf(stderr, "broker rejected command: %v\n", brokerErr)
		} else {
			fmt.Fprintf(stderr, "execute command: %v\n", err)
		}
		return 1
	}
	return 0
}

func dialBroker(ctx context.Context, address, caPath, serverName, certPath, keyPath string) (net.Conn, error) {
	dialer := net.Dialer{}
	useTLS := caPath != "" || serverName != "" || certPath != ""
	if !useTLS {
		return dialer.DialContext(ctx, "tcp", address)
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if caPath != "" {
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		caPEM, err := os.ReadFile(caPath) // #nosec G304 -- explicit operator-supplied trust path.
		if err != nil {
			return nil, fmt.Errorf("read TLS CA bundle: %w", err)
		}
		if !roots.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("TLS CA bundle contains no valid certificates")
		}
		config.RootCAs = roots
	}
	if certPath != "" {
		certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load TLS client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	tlsDialer := tls.Dialer{NetDialer: &dialer, Config: config}
	return tlsDialer.DialContext(ctx, "tcp", address)
}

func execute(client *wire.ClientConn, command string, stdout io.Writer) error {
	commandType, _, err := wire.ParseCommandText(command)
	if err != nil {
		return err
	}
	if err := client.Send([]byte(command)); err != nil {
		return err
	}
	response, err := client.Receive()
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(stdout, string(response)); err != nil {
		return err
	}

	switch commandType {
	case wire.CommandBrowseMessages, wire.CommandReadStream, wire.CommandReadStreamHistory:
		batchPayload, err := client.Receive()
		if err != nil {
			return err
		}
		batch, err := wire.DecodeBatch(batchPayload)
		if err != nil {
			return fmt.Errorf("decode %s batch: %w", commandType, err)
		}
		encoded, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("encode %s batch output: %w", commandType, err)
		}
		_, err = fmt.Fprintln(stdout, string(encoded))
		return err
	default:
		return nil
	}
}
