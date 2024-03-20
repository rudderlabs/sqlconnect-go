package sshtunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/armon/go-socks5"
	"golang.org/x/crypto/ssh"
)

// NewSocks5Tunnel creates a new socks5 proxy using the ssh tunnel
func NewSocks5Tunnel(c Config) (Tunnel, error) {
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("invalid ssh tunnel configuration: %w", err)
	}
	sshSigner, err := ssh.ParsePrivateKey([]byte(c.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("parsing private key: %w", err)
	}

	endpoint := net.JoinHostPort(c.Host, c.Port)
	sshClient, err := ssh.Dial("tcp", endpoint, &ssh.ClientConfig{
		User: c.User,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(sshSigner),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		BannerCallback:  ssh.BannerDisplayStderr(),
		Timeout:         10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("server %q dial error: %w", endpoint, err)
	}

	conf := &socks5.Config{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return sshClient.Dial(network, addr)
		},
	}
	socksServer, _ := socks5.New(conf)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("creating listener: %w", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = socksServer.Serve(l)
	}()
	return &socksTunnel{
		sshClient: sshClient,
		listener:  l,
		addr:      l.Addr().String(),
	}, nil
}

// Socks5HTTPTransport returns an http.Transport that uses the provided host and port as a socks5 proxy.
// This is useful for making http requests through a socks5 tunnel.
// It uses http.DefaultTransport  as a base.
func Socks5HTTPTransport(host string, port int) *http.Transport {
	// Copy the default transport and replace the Proxy function
	defaultTransport := http.DefaultTransport.(*http.Transport)
	return &http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) {
			return &url.URL{
				Scheme: "socks5",
				Host:   fmt.Sprintf("%s:%d", host, port),
			}, nil
		},
		DialContext:           defaultTransport.DialContext,
		ForceAttemptHTTP2:     defaultTransport.ForceAttemptHTTP2,
		MaxIdleConns:          defaultTransport.MaxIdleConns,
		IdleConnTimeout:       defaultTransport.IdleConnTimeout,
		TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
	}
}

type socksTunnel struct {
	wg        sync.WaitGroup
	sshClient *ssh.Client
	listener  net.Listener
	addr      string
}

func (t *socksTunnel) Addr() string {
	return t.addr
}

func (t *socksTunnel) Host() string {
	host, _, _ := net.SplitHostPort(t.Addr())
	return host
}

func (t *socksTunnel) Port() int {
	_, port, _ := net.SplitHostPort(t.Addr())
	p, _ := strconv.Atoi(port)
	return p
}

func (t *socksTunnel) Close() error {
	err := errors.Join(t.listener.Close(), t.sshClient.Close())
	t.wg.Wait()
	return err
}
