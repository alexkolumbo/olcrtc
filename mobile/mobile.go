// Package mobile provides a gomobile-compatible API for olcRTC.
// Build with: gomobile bind -target=android ./mobile
package mobile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/app/session"
	"github.com/openlibrecommunity/olcrtc/internal/client"
	"github.com/openlibrecommunity/olcrtc/internal/control"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/protect"

	"github.com/openlibrecommunity/olcrtc/internal/transport/vp8channel"

	_ "golang.org/x/mobile/bind"                       // ensure gomobile bind is available
	_ "google.golang.org/genproto/protobuf/field_mask" // keep gomobile on post-split genproto modules
)

// SocketProtector protects sockets from VPN routing on Android.
// Implement this interface in Kotlin/Java and pass to SetProtector.
type SocketProtector interface {
	Protect(fd int) bool
}

// LogWriter receives log messages from olcRTC.
type LogWriter interface {
	WriteLog(msg string)
}

var (
	errAlreadyRunning       = errors.New("olcRTC already running")
	errCarrierRequired      = errors.New("carrier is required")
	errRoomIDRequired       = errors.New("roomID is required")
	errClientIDRequired     = errors.New("clientID is required")
	errKeyHexRequired       = errors.New("keyHex is required")
	errNotRunning           = errors.New("olcRTC is not running")
	errStoppedBeforeReady   = errors.New("olcRTC stopped before becoming ready")
	errStartTimedOut        = errors.New("olcRTC start timed out")
	errHTTPPingTimedOut     = errors.New("HTTP ping timed out")
	errUnexpectedHTTPStatus = errors.New("unexpected HTTP status")
)

const (
	defaultTransport   = "vp8channel"
	dataTransport      = "datachannel"
	defaultDNSServer   = "8.8.8.8:53"
	defaultHTTPPingURL = "https://www.google.com/generate_204"
	defaultSocksHost   = "127.0.0.1"
	carrierWBStream    = "wbstream"
)

const (
	httpPingWarmupTimeout = 1500 * time.Millisecond
	httpPingSampleTimeout = 1500 * time.Millisecond
	httpPingSamples       = 3
	httpPingSampleDelay   = 80 * time.Millisecond
)

var (
	mu                 sync.Mutex            //nolint:gochecknoglobals // package-level state intentional
	defaults           mobileConfig          //nolint:gochecknoglobals // package-level state intentional
	defaultsSet        sync.Once             //nolint:gochecknoglobals // package-level state intentional
	registerSet        sync.Once             //nolint:gochecknoglobals // package-level state intentional
	runClientWithReady = client.RunWithReady //nolint:gochecknoglobals // package-level state intentional
	cancel             context.CancelFunc    //nolint:gochecknoglobals // package-level state intentional
	done               chan struct{}         //nolint:gochecknoglobals // package-level state intentional
	ready              chan struct{}         //nolint:gochecknoglobals // package-level state intentional
	errRun             error
)

type mobileConfig struct {
	transport        string
	dnsServer        string
	socksListenHost  string
	authToken        string
	vp8FPS           int
	vp8BatchSize     int
	livenessInterval time.Duration
	livenessTimeout  time.Duration
	livenessFailures int
}

// SetProtector sets the Android VPN socket protector.
// Must be called before Start.
func SetProtector(p SocketProtector) {
	if p == nil {
		protect.Protector = nil
		return
	}
	protect.Protector = func(fd int) bool {
		return p.Protect(fd)
	}
}

// SetLogWriter sets a custom log writer for olcRTC output.
func SetLogWriter(w LogWriter) {
	if w != nil {
		log.SetOutput(&logBridge{w: w})
	}
}

// SetProviders registers built-in carriers, links, and transports.
func SetProviders() {
	registerDefaults()
}

// SetTransport selects the transport used by Start.
// Supported values: vp8channel and datachannel.
func SetTransport(transport string) {
	mu.Lock()
	defer mu.Unlock()
	ensureDefaultConfigLocked()
	defaults.transport = normalizeTransport(transport)
}

// SetDNS selects the DNS server used by the tunnel.
func SetDNS(dnsServer string) {
	mu.Lock()
	defer mu.Unlock()
	ensureDefaultConfigLocked()
	defaults.dnsServer = dnsServer
}

// SetWBToken sets the pre-issued wbstream account token (auth.token).
// When set, the session joins as that account instead of an anonymous guest;
// empty keeps the guest flow. Required for datachannel over wbstream, which
// needs an account/moderator token with canPublishData=true.
func SetWBToken(token string) {
	mu.Lock()
	defer mu.Unlock()
	ensureDefaultConfigLocked()
	defaults.authToken = strings.TrimSpace(token)
}

// SetSocksListenHost selects the local bind host for the SOCKS5 listener.
// Use 0.0.0.0 to accept connections from other Android network interfaces.
func SetSocksListenHost(host string) {
	mu.Lock()
	defer mu.Unlock()
	ensureDefaultConfigLocked()
	defaults.socksListenHost = normalizeSocksListenHost(host)
}

// SetVP8Options configures vp8channel.
func SetVP8Options(fps, batchSize int) {
	mu.Lock()
	defer mu.Unlock()
	ensureDefaultConfigLocked()
	defaults.vp8FPS = clampAtLeastOne(fps, 120)
	defaults.vp8BatchSize = clampAtLeastOne(batchSize, 64)
}

// SetLivenessOptions configures control-stream ping/pong checks.
// Values <= 0 reset that field to its default. Durations are milliseconds.
func SetLivenessOptions(intervalMillis, timeoutMillis, failures int) {
	mu.Lock()
	defer mu.Unlock()
	ensureDefaultConfigLocked()
	defaults.livenessInterval = durationFromMillisOrDefault(intervalMillis, control.DefaultInterval)
	defaults.livenessTimeout = durationFromMillisOrDefault(timeoutMillis, control.DefaultTimeout)
	if failures <= 0 {
		defaults.livenessFailures = control.DefaultFailures
		return
	}
	defaults.livenessFailures = failures
}

// SetDebug enables or disables verbose logging.
func SetDebug(enabled bool) {
	logger.SetVerbose(enabled)
	if enabled {
		log.SetFlags(log.Ltime | log.Lshortfile)
		return
	}

	log.SetFlags(log.Ltime)
}

// Start launches the olcRTC client in background.
// carrierName: carrier name ("telemost", "wbstream", "jitsi")
// roomID: carrier-specific room ID
// clientID: client identifier that must match the server's -client-id
// keyHex: 64-char hex encryption key
// socksPort: local SOCKS5 proxy port (e.g. 10808)
// socksUser/socksPass: SOCKS5 credentials (empty = no auth).
func Start(carrierName, roomID, clientID, keyHex string, socksPort int, socksUser, socksPass string) error {
	mu.Lock()
	ensureDefaultConfigLocked()
	cfg := defaults
	mu.Unlock()

	return startWithConfig(carrierName, cfg.transport, roomID, clientID, keyHex, socksPort, socksUser, socksPass, cfg)
}

// StartWithTransport launches the client with an explicit transport for this start.
func StartWithTransport(
	carrierName, transportName, roomID, clientID, keyHex string,
	socksPort int,
	socksUser, socksPass string,
) error {
	mu.Lock()
	ensureDefaultConfigLocked()
	cfg := defaults
	cfg.transport = transportName
	mu.Unlock()

	return startWithConfig(carrierName, transportName, roomID, clientID, keyHex, socksPort, socksUser, socksPass, cfg)
}

// Check starts an isolated short-lived client and returns elapsed milliseconds once ready.
// It does not use the singleton Start/Stop runtime, so callers may run checks in parallel.
func Check(
	carrierName, transportName, roomID, clientID, keyHex string,
	socksPort int,
	timeoutMillis int,
	vp8FPS int,
	vp8BatchSize int,
) (int64, error) {
	registerDefaults()
	mu.Lock()
	ensureDefaultConfigLocked()
	cfg := defaults
	mu.Unlock()

	carrierName = normalizeCarrier(carrierName)
	transportName = normalizeTransport(transportName)
	if err := validateStartArgs(carrierName, roomID, clientID, keyHex); err != nil {
		return 0, err
	}

	if timeoutMillis <= 0 {
		timeoutMillis = 8000
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	readyCh := make(chan struct{})
	doneCh := make(chan error, 1)
	var readyOnce sync.Once
	startedAt := time.Now()

	go func() {
		doneCh <- runClientWithReady(
			ctx,
			client.Config{
				Transport: transportName,
				Carrier:   carrierName,
				RoomURL:   buildRoomURL(carrierName, roomID),
				KeyHex:    keyHex,
				DeviceID:  clientID,
				LocalAddr: socksListenAddr(cfg.socksListenHost, socksPort),
				DNSServer: defaultDNSServer,
				AuthToken: cfg.authToken,
				TransportOptions: vp8channel.Options{
					FPS:       clampAtLeastOne(vp8FPS, 120),
					BatchSize: clampAtLeastOne(vp8BatchSize, 64),
				},
				Liveness: livenessConfig(cfg),
			},
			func() {
				readyOnce.Do(func() {
					close(readyCh)
				})
			},
		)
	}()

	timer := time.NewTimer(time.Duration(timeoutMillis) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-readyCh:
		elapsed := time.Since(startedAt).Milliseconds()
		cancelFunc()
		waitForCheckDone(doneCh)
		return elapsed, nil
	case err := <-doneCh:
		if err != nil {
			return 0, err
		}
		return 0, errStoppedBeforeReady
	case <-timer.C:
		cancelFunc()
		waitForCheckDone(doneCh)
		return 0, errStartTimedOut
	}
}

// Ping starts an isolated short-lived client, waits until its SOCKS listener is ready,
// performs HTTP requests through that SOCKS tunnel, and returns HTTP latency in milliseconds.
//
// The returned value does not include RTC startup time. It measures only HTTP request latency
// after the tunnel is ready.
func Ping(
	carrierName, transportName, roomID, clientID, keyHex string,
	socksPort int,
	timeoutMillis int,
	pingURL string,
	vp8FPS int,
	vp8BatchSize int,
) (int64, error) {
	registerDefaults()
	mu.Lock()
	ensureDefaultConfigLocked()
	cfg := defaults
	mu.Unlock()

	carrierName = normalizeCarrier(carrierName)
	transportName = normalizeTransport(transportName)

	if err := validateStartArgs(carrierName, roomID, clientID, keyHex); err != nil {
		return 0, err
	}

	if timeoutMillis <= 0 {
		timeoutMillis = 10000
	}
	if pingURL == "" {
		pingURL = defaultHTTPPingURL
	}

	ctx, cancelFunc := context.WithTimeout(
		context.Background(),
		time.Duration(timeoutMillis)*time.Millisecond,
	)
	defer cancelFunc()

	readyCh := make(chan struct{})
	doneCh := make(chan error, 1)

	var readyOnce sync.Once

	go func() {
		doneCh <- runClientWithReady(
			ctx,
			client.Config{
				Transport: transportName,
				Carrier:   carrierName,
				RoomURL:   buildRoomURL(carrierName, roomID),
				KeyHex:    keyHex,
				DeviceID:  clientID,
				LocalAddr: socksListenAddr(cfg.socksListenHost, socksPort),
				DNSServer: defaultDNSServer,
				AuthToken: cfg.authToken,
				TransportOptions: vp8channel.Options{
					FPS:       clampAtLeastOne(vp8FPS, 120),
					BatchSize: clampAtLeastOne(vp8BatchSize, 64),
				},
				Liveness: livenessConfig(cfg),
			},
			func() {
				readyOnce.Do(func() {
					close(readyCh)
				})
			},
		)
	}()

	select {
	case <-readyCh:
		elapsed, err := httpPingThroughSocks(
			ctx,
			socksDialAddr(cfg.socksListenHost, socksPort),
			pingURL,
		)

		cancelFunc()
		waitForCheckDone(doneCh)

		if err != nil {
			return 0, err
		}

		return elapsed, nil

	case err := <-doneCh:
		if err != nil {
			return 0, err
		}

		return 0, errStoppedBeforeReady

	case <-ctx.Done():
		cancelFunc()
		waitForCheckDone(doneCh)

		return 0, errStartTimedOut
	}
}

func httpPingThroughSocks(
	parentCtx context.Context,
	socksAddr string,
	targetURL string,
) (int64, error) {
	normalizedURL, err := normalizeHTTPPingURL(targetURL)
	if err != nil {
		return 0, err
	}

	client, closeClient := newHTTPPingClient(socksAddr)
	defer closeClient()

	// Warm up the SOCKS/TCP/TLS path. This request is intentionally not included
	// in the returned latency.
	_, _ = singleHTTPPingRequest(
		parentCtx,
		client,
		normalizedURL,
		httpPingWarmupTimeout,
	)

	return bestHTTPPingSample(parentCtx, client, normalizedURL)
}

func normalizeHTTPPingURL(targetURL string) (string, error) {
	if targetURL == "" {
		targetURL = defaultHTTPPingURL
	}

	if _, err := url.ParseRequestURI(targetURL); err != nil {
		return "", fmt.Errorf("parse HTTP ping URL: %w", err)
	}

	return targetURL, nil
}

func newHTTPPingClient(socksAddr string) (*http.Client, func()) {
	proxyURL := &url.URL{
		Scheme: "socks5",
		Host:   socksAddr,
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),

		DisableKeepAlives:   false,
		MaxIdleConns:        4,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     10 * time.Second,

		ForceAttemptHTTP2:     false,
		TLSHandshakeTimeout:   httpPingSampleTimeout,
		ResponseHeaderTimeout: httpPingSampleTimeout,
		ExpectContinueTimeout: 500 * time.Millisecond,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   httpPingSampleTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	return client, transport.CloseIdleConnections
}

func bestHTTPPingSample(
	parentCtx context.Context,
	client *http.Client,
	targetURL string,
) (int64, error) {
	var best int64
	var lastErr error

	for i := range httpPingSamples {
		elapsed, err := singleHTTPPingRequest(
			parentCtx,
			client,
			targetURL,
			httpPingSampleTimeout,
		)
		if err != nil {
			lastErr = err
		} else {
			best = bestPositiveLatency(best, elapsed)
		}

		if i < httpPingSamples-1 {
			time.Sleep(httpPingSampleDelay)
		}
	}

	if best > 0 {
		return best, nil
	}

	if lastErr != nil {
		return 0, lastErr
	}

	return 0, errHTTPPingTimedOut
}

func bestPositiveLatency(currentBest, next int64) int64 {
	if next <= 0 {
		return currentBest
	}

	if currentBest == 0 || next < currentBest {
		return next
	}

	return currentBest
}

func singleHTTPPingRequest(
	parentCtx context.Context,
	client *http.Client,
	targetURL string,
	timeout time.Duration,
) (int64, error) {
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return 0, fmt.Errorf("create HTTP ping request: %w", err)
	}

	req.Header.Set("User-Agent", "Olcbox-Android")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Cache-Control", "no-cache")

	startedAt := time.Now()

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("perform HTTP ping request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	elapsed := time.Since(startedAt).Milliseconds()

	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusPermanentRedirect {
		return 0, fmt.Errorf("%w: %d", errUnexpectedHTTPStatus, resp.StatusCode)
	}

	return elapsed, nil
}

func startWithConfig(
	carrierName, transportName, roomID, clientID, keyHex string,
	socksPort int,
	socksUser, socksPass string,
	cfg mobileConfig,
) error {
	mu.Lock()
	defer mu.Unlock()

	registerDefaults()
	carrierName = normalizeCarrier(carrierName)
	if transportName != "" {
		cfg.transport = normalizeTransport(transportName)
	}

	if cancel != nil {
		return errAlreadyRunning
	}
	if err := validateStartArgs(carrierName, roomID, clientID, keyHex); err != nil {
		return err
	}

	roomURL := buildRoomURL(carrierName, roomID)

	ctx, cancelFunc := context.WithCancel(context.Background())
	cancel = cancelFunc
	done = make(chan struct{})
	ready = make(chan struct{})
	localReady := ready
	errRun = nil

	var readyOnce sync.Once
	go func() {
		defer cancelFunc()

		err := runClientWithReady(
			ctx,
			client.Config{
				Transport: cfg.transport,
				Carrier:   carrierName,
				RoomURL:   roomURL,
				KeyHex:    keyHex,
				DeviceID:  clientID,
				LocalAddr: socksListenAddr(cfg.socksListenHost, socksPort),
				DNSServer: cfg.dnsServer,
				AuthToken: cfg.authToken,
				SOCKSUser: socksUser,
				SOCKSPass: socksPass,
				TransportOptions: vp8channel.Options{
					FPS:       cfg.vp8FPS,
					BatchSize: cfg.vp8BatchSize,
				},
				Liveness: livenessConfig(cfg),
			},
			func() {
				readyOnce.Do(func() {
					close(localReady)
				})
			},
		)

		mu.Lock()
		cancel = nil
		errRun = err
		mu.Unlock()
		close(done)
	}()

	return nil
}

// WaitReady blocks until the selected transport is connected and the local SOCKS5 listener is ready.
//
//nolint:cyclop // straightforward state-machine waits with multiple terminal conditions
func WaitReady(timeoutMillis int) error {
	mu.Lock()
	r := ready
	d := done
	runErr := errRun
	running := cancel != nil
	mu.Unlock()

	if r == nil {
		if runErr != nil {
			return runErr
		}

		return errNotRunning
	}

	select {
	case <-r:
		return nil
	default:
	}

	if !running {
		if runErr != nil {
			return runErr
		}

		return errStoppedBeforeReady
	}

	timer := time.NewTimer(time.Duration(timeoutMillis) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-r:
		return nil
	case <-d:
		mu.Lock()
		runErr = errRun
		mu.Unlock()
		if runErr != nil {
			return runErr
		}

		return errStoppedBeforeReady
	case <-timer.C:
		return errStartTimedOut
	}
}

// Stop gracefully stops the olcRTC client.
func Stop() {
	mu.Lock()
	cancelFunc := cancel
	doneCh := done
	mu.Unlock()

	if cancelFunc == nil {
		return
	}

	cancelFunc()

	if doneCh != nil {
		<-doneCh
	}
}

// IsRunning returns true if the olcRTC client is active.
func IsRunning() bool {
	mu.Lock()
	defer mu.Unlock()
	return cancel != nil
}

func registerDefaults() {
	registerSet.Do(session.RegisterDefaults)
}

func waitForCheckDone(doneCh <-chan error) {
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
	}
}

func ensureDefaultConfigLocked() {
	defaultsSet.Do(func() {
		defaults = mobileConfig{
			transport:        defaultTransport,
			dnsServer:        defaultDNSServer,
			socksListenHost:  defaultSocksHost,
			vp8FPS:           30,
			vp8BatchSize:     8,
			livenessInterval: control.DefaultInterval,
			livenessTimeout:  control.DefaultTimeout,
			livenessFailures: control.DefaultFailures,
		}
	})
}

func normalizeSocksListenHost(host string) string {
	host = strings.TrimSpace(host)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	}
	if host == "" {
		return defaultSocksHost
	}
	return host
}

func socksListenAddr(host string, port int) string {
	return net.JoinHostPort(normalizeSocksListenHost(host), strconv.Itoa(port))
}

func socksDialAddr(host string, port int) string {
	switch normalizeSocksListenHost(host) {
	case "0.0.0.0", "::":
		return socksListenAddr(defaultSocksHost, port)
	default:
		return socksListenAddr(host, port)
	}
}

func livenessConfig(cfg mobileConfig) control.Config {
	interval := cfg.livenessInterval
	if interval <= 0 {
		interval = control.DefaultInterval
	}
	timeout := cfg.livenessTimeout
	if timeout <= 0 {
		timeout = control.DefaultTimeout
	}
	failures := cfg.livenessFailures
	if failures <= 0 {
		failures = control.DefaultFailures
	}
	return control.Config{
		Interval: interval,
		Timeout:  timeout,
		Failures: failures,
	}
}

func normalizeTransport(value string) string {
	switch value {
	case dataTransport, "data", "dc":
		return dataTransport
	case defaultTransport, "vp8":
		return defaultTransport
	default:
		return defaultTransport
	}
}

func normalizeCarrier(carrierName string) string {
	if carrierName == carrierWBStream {
		return carrierWBStream
	}
	return carrierName
}

func validateStartArgs(carrierName, roomID, clientID, keyHex string) error {
	switch {
	case carrierName == "":
		return errCarrierRequired
	case roomID == "":
		return errRoomIDRequired
	case clientID == "":
		return errClientIDRequired
	case keyHex == "":
		return errKeyHexRequired
	default:
		return nil
	}
}

func buildRoomURL(_ string, roomID string) string {
	// Keep the same RoomURL value the CLI/YAML path passes into transports.
	// Auth providers may expand it for service HTTP calls, but transports
	// such as vp8channel derive peer binding from the raw room value.
	return roomID
}

func clampAtLeastOne(value, maxValue int) int {
	if value < 1 {
		return 1
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func durationFromMillisOrDefault(value int, def time.Duration) time.Duration {
	if value <= 0 {
		return def
	}
	d := time.Duration(value) * time.Millisecond
	if d <= 0 {
		return def
	}
	return d
}

// logBridge adapts LogWriter to io.Writer.
type logBridge struct {
	w LogWriter
}

func (b *logBridge) Write(p []byte) (int, error) {
	b.w.WriteLog(string(p))
	return len(p), nil
}

// --- Second isolated leg: direct LiveKit carrier ("Reserv-2") ---
// The primary Start() is a package singleton (guarded by cancel). StartLK brings up a SECOND,
// fully independent olcRTC leg on its own SOCKS port using its own lkCancel, so a direct
// LiveKit carrier (WB Stream / self-hosted LiveKit, uncorrelated with the Telemost primary)
// runs concurrently in the same process. Mirrors the isolated-client pattern of Check()/Ping().
var (
	errLKURLRequired   = errors.New("livekit url is required")
	errLKTokenRequired = errors.New("livekit token is required")

	lkMu     sync.Mutex         //nolint:gochecknoglobals // second-leg state, intentional
	lkCancel context.CancelFunc //nolint:gochecknoglobals // second-leg state, intentional
	lkDone   chan struct{}      //nolint:gochecknoglobals // second-leg state, intentional
)

// StartLK starts an isolated direct-LiveKit leg (auth "none", engine "livekit", transport
// datachannel) on socksPort, concurrent with the primary Start() carrier. url/token are the
// LiveKit signaling URL + access token (the token embeds the room); room is passed through for
// peer binding; keyHex is the shared olcRTC payload cipher. Non-blocking: returns after launch
// (validation errors are returned synchronously); connection status surfaces via the log.
func StartLK(url, token, room, keyHex, clientID string, socksPort int) error {
	registerDefaults()
	switch {
	case url == "":
		return errLKURLRequired
	case token == "":
		return errLKTokenRequired
	case keyHex == "":
		return errKeyHexRequired
	}

	mu.Lock()
	ensureDefaultConfigLocked()
	cfg := defaults
	mu.Unlock()

	lkMu.Lock()
	defer lkMu.Unlock()
	if lkCancel != nil {
		return errAlreadyRunning
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	lkCancel = cancelFunc
	lkDone = make(chan struct{})
	localDone := lkDone

	go func() {
		defer cancelFunc()
		err := runClientWithReady(ctx, client.Config{
			Transport: dataTransport,
			Carrier:   "none",
			Engine:    "livekit",
			URL:       url,
			Token:     token,
			RoomURL:   room,
			KeyHex:    keyHex,
			DeviceID:  clientID,
			LocalAddr: socksListenAddr(cfg.socksListenHost, socksPort),
			DNSServer: cfg.dnsServer,
			Liveness:  livenessConfig(cfg),
		}, nil)
		lkMu.Lock()
		lkCancel = nil
		lkMu.Unlock()
		if err != nil {
			log.Printf("olcRTC LK leg exited: %v", err)
		}
		close(localDone)
	}()
	return nil
}

// StopLK tears down the second LiveKit leg started by StartLK (no-op if not running).
func StopLK() {
	lkMu.Lock()
	c := lkCancel
	lkCancel = nil
	lkMu.Unlock()
	if c != nil {
		c()
	}
}

// --- Generic multi-leg runner (N concurrent carrier legs) ---
// StartLeg generalizes StartLK: it brings up an arbitrary number of fully
// independent olcRTC legs, each on its own SOCKS port with its own cancel func,
// so a mihomo load-balance group can aggregate throughput across N parallel
// carrier rooms (e.g. N Telemost/vp8channel legs). Each leg bypasses the
// package singleton used by Start()/cancel; legs are tracked in the legs map by
// a monotonically increasing legID and torn down individually or en masse.
var (
	legMu  sync.Mutex                     //nolint:gochecknoglobals // multi-leg registry, intentional
	legs   = map[int]context.CancelFunc{} //nolint:gochecknoglobals // multi-leg registry, intentional
	legSeq int                            //nolint:gochecknoglobals // multi-leg id sequence, intentional
)

// StartLeg starts an isolated carrier leg on socksPort, concurrent with the
// primary Start() carrier and with every other leg. It mirrors StartLK but is
// PARAMETERIZED: carrier is the carrier name ("telemost", "wbstream", ...),
// room is the room identifier (used as RoomURL), token is the (usually empty
// for telemost) auth/access token, keyHex is the shared olcRTC payload cipher,
// clientID is the per-leg device identifier, transport selects the transport
// ("vp8channel" or "datachannel"; empty falls back to the configured default).
// DNSServer, liveness, socks host and vp8 options are snapshotted from the
// package defaults. Returns the legID for use with StopLeg, or an error if
// validation fails. Non-blocking: connection status surfaces via the log.
func StartLeg(carrier, room, token, keyHex, clientID, transport string, socksPort int) (int, error) {
	registerDefaults()
	carrier = normalizeCarrier(carrier)
	switch {
	case carrier == "":
		return 0, errCarrierRequired
	case room == "":
		return 0, errRoomIDRequired
	case clientID == "":
		return 0, errClientIDRequired
	case keyHex == "":
		return 0, errKeyHexRequired
	}

	mu.Lock()
	ensureDefaultConfigLocked()
	cfg := defaults
	mu.Unlock()

	transportName := cfg.transport
	if transport != "" {
		transportName = normalizeTransport(transport)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	legMu.Lock()
	legSeq++
	id := legSeq
	legs[id] = cancelFunc
	legMu.Unlock()

	go func() {
		defer cancelFunc()
		err := runClientWithReady(ctx, client.Config{
			Transport: transportName,
			Carrier:   carrier,
			RoomURL:   room,
			KeyHex:    keyHex,
			DeviceID:  clientID,
			Token:     token,
			LocalAddr: socksListenAddr(cfg.socksListenHost, socksPort),
			DNSServer: cfg.dnsServer,
			TransportOptions: vp8channel.Options{
				FPS:       cfg.vp8FPS,
				BatchSize: cfg.vp8BatchSize,
			},
			Liveness: livenessConfig(cfg),
		}, nil)
		legMu.Lock()
		delete(legs, id)
		legMu.Unlock()
		if err != nil {
			log.Printf("olcRTC leg %d (%s) exited: %v", id, carrier, err)
		}
	}()
	return id, nil
}

// StopLeg cancels and removes a single leg started by StartLeg (no-op if the id
// is unknown or the leg already exited).
func StopLeg(id int) {
	legMu.Lock()
	c := legs[id]
	delete(legs, id)
	legMu.Unlock()
	if c != nil {
		c()
	}
}

// StopAllLegs cancels and removes every leg started by StartLeg.
func StopAllLegs() {
	legMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(legs))
	for id, c := range legs {
		cancels = append(cancels, c)
		delete(legs, id)
	}
	legMu.Unlock()
	for _, c := range cancels {
		if c != nil {
			c()
		}
	}
}
