package src

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultReconnectDelay     = 5 * time.Second
	maxReconnectDelay         = 60 * time.Second
	reconnectDelayMultiplier  = 2.0
	pingInterval              = 30 * time.Second
	websocketHandshakeTimeout = 10 * time.Second
	websocketWriteTimeout     = 10 * time.Second
	websocketIPDialTimeout    = 3 * time.Second
)

// dialWebSocketHost resolves the WebSocket hostname and tries every returned
// address. A single unreachable CDN address must not consume the whole
// WebSocket handshake timeout and prevent trying the next address.
//
// The URL passed to gorilla/websocket is unchanged, so TLS still uses the
// hostname as SNI and validates the server certificate against that hostname.
func dialWebSocketHost(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid WebSocket address %q: %w", address, err)
	}

	dialer := &net.Dialer{}
	if net.ParseIP(host) != nil {
		return dialer.DialContext(ctx, network, address)
	}

	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve WebSocket host %s: %w", host, err)
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("no IP addresses returned for WebSocket host %s", host)
	}

	var lastErr error
	for _, resolved := range addresses {
		ipAddress := net.JoinHostPort(resolved.IP.String(), port)
		attemptCtx, cancel := context.WithTimeout(ctx, websocketIPDialTimeout)
		conn, err := dialer.DialContext(attemptCtx, network, ipAddress)
		cancel()
		if err == nil {
			Logf("Connected to WebSocket host %s via %s", host, resolved.IP.String())
			return conn, nil
		}

		lastErr = err
		Logf("Failed to connect to WebSocket host %s via %s: %v", host, resolved.IP.String(), err)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("failed to connect to all resolved IPs for WebSocket host %s: %w", host, lastErr)
}

type ClientHandler interface {
	GetFromWebSocketHandlers() []func(string)
	GetToWebSocketHandlers() []func(string)
	GetDisconnectedHandlers() []func(string)
	GetBeforeReconnectHandlers() []func(string)
	GetAfterReconnectHandlers() []func()
	ReportAuthenticationFailure(error)
	ResetAuthenticationFailure()
	SetMe(Me)
	SetChats([]Chat)
	SetConnectionTime(time.Time)
}

type Connection struct {
	config              *Config
	client              ClientHandler
	conn                *websocket.Conn
	connected           bool
	requestBuilder      *RequestBuilder
	messageQueue        chan WebSocketPayload
	responseFutures     map[int]chan WebSocketPayload
	pingTicker          *time.Ticker
	lastPongTime        time.Time
	reconnectAttempts   int
	currentDelay        time.Duration
	maxBackoffDelay     time.Duration
	manualStop          bool
	performingReconnect bool
	reconnectLock       sync.Mutex
	stopCh              chan struct{}
	stopOnce            sync.Once
	writeMu             sync.Mutex

	mu sync.RWMutex
}

func NewConnection(config *Config, client ClientHandler) *Connection {
	return &Connection{
		config:          config,
		client:          client,
		connected:       false,
		requestBuilder:  NewRequestBuilder(config),
		messageQueue:    make(chan WebSocketPayload, 1000),
		responseFutures: make(map[int]chan WebSocketPayload),
		currentDelay:    defaultReconnectDelay,
		maxBackoffDelay: maxReconnectDelay,
	}
}

func (c *Connection) closeStopCh() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
}

func (c *Connection) Connect() error {
	if c.connected {
		return nil
	}

	if c.config.UserAgent.UserAgent == "" {
		return ErrInvalidUserAgent
	}

	headers := map[string][]string{
		"User-Agent":            {c.config.UserAgent.UserAgent},
		"Accept":                {"*/*"},
		"Accept-Language":       {c.config.UserAgent.DeviceLocale + ";q=0.5"},
		"Accept-Encoding":       {"gzip, deflate, br, zstd"},
		"Sec-WebSocket-Version": {"13"},
		"Origin":                {"https://web.max.ru"},
		"Sec-Fetch-Dest":        {"empty"},
		"Sec-Fetch-Mode":        {"websocket"},
		"Sec-Fetch-Site":        {"cross-site"},
		"Pragma":                {"no-cache"},
		"Cache-Control":         {"no-cache"},
	}

	dialer := websocket.Dialer{
		HandshakeTimeout:  websocketHandshakeTimeout,
		EnableCompression: true,
	}

	maxProxy := GetMaxProxy(c.config)
	if maxProxy != nil {
		socks5Dialer, err := BuildSOCKS5Dialer(maxProxy)
		if err != nil {
			return fmt.Errorf("failed to build SOCKS5 dialer for MAX: %w", err)
		}
		Logf("Connecting to WebSocket via SOCKS5 proxy %s:%d", maxProxy.Host, maxProxy.Port)
		dialer.NetDial = socks5Dialer.Dial
	} else {
		dialer.NetDialContext = dialWebSocketHost
	}

	conn, _, err := dialer.Dial(WEBSOCKET_URL, headers)
	if err != nil {
		return NewConnectionError("failed to connect: " + err.Error())
	}

	c.conn = conn
	c.connected = true
	c.reconnectAttempts = 0
	c.currentDelay = defaultReconnectDelay

	c.requestBuilder = NewRequestBuilder(c.config)

	c.stopCh = make(chan struct{})
	c.stopOnce = sync.Once{}

	go c.receiveMessages(c.stopCh)

	if err := c.sendInit(); err != nil {
		c.closeStopCh()
		conn.Close()
		c.connected = false
		return err
	}

	c.startPing(c.stopCh)

	Logf("Connected to WebSocket")
	return nil
}

func (c *Connection) sendInit() error {
	initData := c.requestBuilder.Init()
	if err := c.send(initData); err != nil {
		return err
	}

	response, err := c.receiveWithTimeout(10 * time.Second)
	if err != nil {
		return err
	}

	opcode, _ := parseInt(response["opcode"])
	cmd, _ := parseInt(response["cmd"])
	if opcode != int(INIT) || cmd != 1 {
		return NewInvalidResponseError("invalid initialization response")
	}

	return nil
}

func (c *Connection) receiveWithTimeout(timeout time.Duration) (WebSocketPayload, error) {
	select {
	case payload, ok := <-c.messageQueue:
		if !ok {
			return nil, NewConnectionError("message queue closed")
		}
		return payload, nil
	case <-c.stopCh:
		return nil, NewConnectionError("receiver stopped")
	case <-time.After(timeout):
		return nil, NewTimeoutError("receive timeout")
	}
}

func (c *Connection) Authenticate(chatsCount int) (WebSocketPayload, error) {
	if !c.connected {
		return nil, NewConnectionError("WebSocket not connected")
	}

	authData := c.requestBuilder.Authenticate(chatsCount)
	response, err := c.sendAndReceive(authData, 10*time.Second)
	if err != nil {
		return nil, err
	}

	opcode, _ := parseInt(response["opcode"])
	cmd, _ := parseInt(response["cmd"])
	if opcode != int(AUTH) {
		return nil, NewInvalidResponseError("authentication failed: invalid response")
	}
	if cmd == 3 {
		err := authenticationErrorFromPayload(response["payload"])
		if c.client != nil {
			c.client.ReportAuthenticationFailure(err)
		}
		return nil, err
	}
	if cmd != 1 {
		return nil, NewInvalidResponseError("authentication failed: invalid response")
	}

	payload, ok := response["payload"].(map[string]interface{})
	if !ok {
		err := NewInvalidTokenError("invalid response payload")
		if c.client != nil {
			c.client.ReportAuthenticationFailure(err)
		}
		return nil, err
	}

	_, hasProfile := payload["profile"]
	if _, hasDirectContact := payload["contact"].(map[string]interface{}); hasDirectContact {
		hasProfile = true
	}
	// The binary web API returns the authenticated person's display profile
	// directly, while the legacy JSON API nests it under "profile".
	if !hasProfile && stringValue(payload, "type") != "ONEME" {
		err := NewInvalidTokenError("no profile data")
		if c.client != nil {
			c.client.ReportAuthenticationFailure(err)
		}
		return nil, err
	}
	if c.client != nil {
		c.client.ResetAuthenticationFailure()
	}

	return response, nil
}

func authenticationErrorFromPayload(raw interface{}) error {
	payload, _ := raw.(map[string]interface{})
	code := stringValue(payload, "error")
	message := stringValue(payload, "localizedMessage")
	if message == "" {
		message = stringValue(payload, "message")
	}
	if message == "" {
		message = stringValue(payload, "title")
	}
	if message == "" {
		message = "MAX rejected authentication"
	}
	return NewAuthenticationError(code, message)
}

func (c *Connection) startPing(stopCh <-chan struct{}) {
	c.mu.Lock()
	c.lastPongTime = time.Now()
	c.mu.Unlock()

	ticker := time.NewTicker(pingInterval)
	c.pingTicker = ticker

	go func() {
		defer ticker.Stop()
		defer func() {
			if r := recover(); r != nil {
				Logf("PANIC in ping goroutine: %v", r)
			}
		}()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				if !c.connected {
					return
				}

				c.mu.RLock()
				lastPong := c.lastPongTime
				c.mu.RUnlock()

				if time.Since(lastPong) > c.config.PingTimeout {
					Logf("Ping timeout: server did not respond for %v, initiating reconnect", time.Since(lastPong).Round(time.Second))
					c.connected = false
					if c.conn != nil {
						c.conn.Close()
					}
					go c.HandleReconnect("Ping timeout: server stopped responding")
					return
				}

				pingData := c.requestBuilder.Ping()
				if err := c.send(pingData); err != nil {
					Logf("Ping failed: %v", err)
					return
				}
			}
		}
	}()
}

func (c *Connection) receiveMessages(stopCh <-chan struct{}) {
	defer c.closeStopCh()
	defer func() {
		if r := recover(); r != nil {
			Logf("PANIC in receiveMessages: %v", r)
			if c.connected {
				c.connected = false
				go c.HandleReconnect(fmt.Sprintf("panic in receiveMessages: %v", r))
			}
		}
	}()

	for {
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			if c.connected {
				Logf("WebSocket read error: %v", err)
				c.connected = false
				go c.HandleReconnect("WebSocket connection closed")
			}
			return
		}

		if messageType != websocket.BinaryMessage {
			Logf("Ignoring non-binary WebSocket message (type %d)", messageType)
			continue
		}

		payload, err := decodeWebSocketFrame(message)
		if err != nil {
			if isShortMessageNotification(message) {
				Logf("Undecoded MAX message notification (%s body_hex=%x): %v", binaryFrameSummary(message), message[mobileFrameHeader:], err)
			} else {
				Logf("Failed to parse binary WebSocket frame (%s): %v", binaryFrameSummary(message), err)
			}
			continue
		}
		loggedPayload := formatWebSocketLog(payload)
		Logf("↓ %s", loggedPayload)

		if c.client != nil {
			for _, handler := range c.client.GetFromWebSocketHandlers() {
				handler(formatWebSocketPayload(payload))
			}
		}

		c.mu.Lock()
		c.lastPongTime = time.Now()
		c.mu.Unlock()

		if seq, ok := parseInt(payload["seq"]); ok && c.isResponse(payload) {
			c.mu.Lock()
			if future, exists := c.responseFutures[seq]; exists {
				select {
				case future <- payload:
				default:
				}
				delete(c.responseFutures, seq)
			}
			c.mu.Unlock()
		}

		select {
		case c.messageQueue <- payload:
		case <-stopCh:
			return
		default:
			Logf("Message queue full, dropping message")
		}
	}
}

func isShortMessageNotification(frame []byte) bool {
	return len(frame) >= mobileFrameHeader &&
		binary.BigEndian.Uint16(frame[4:6]) == uint16(ON_MESSAGE+1) &&
		len(frame)-mobileFrameHeader <= 256
}

func (c *Connection) isResponse(payload WebSocketPayload) bool {
	cmd, ok := parseInt(payload["cmd"])
	return ok && (cmd == 1 || cmd == 3)
}

func (c *Connection) send(payload WebSocketPayload) error {
	if !c.connected {
		return NewConnectionError("WebSocket not connected")
	}

	data, err := buildWebSocketFrame(c.config, payload)
	if err != nil {
		return err
	}

	loggedPayload := formatWebSocketLog(payload)
	Logf("↑ %s", loggedPayload)

	if c.client != nil {
		for _, handler := range c.client.GetToWebSocketHandlers() {
			handler(formatWebSocketPayload(payload))
		}
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(websocketWriteTimeout))
	if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		return NewConnectionError("failed to send: " + err.Error())
	}

	return nil
}

func formatWebSocketPayload(payload WebSocketPayload) string {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "<unavailable>"
	}
	return string(encoded)
}

func formatWebSocketLog(payload WebSocketPayload) string {
	redacted := make(map[string]interface{}, len(payload))
	for key, value := range payload {
		redacted[key] = redactMobileDebugValue(value, key)
	}
	encoded, err := json.Marshal(redacted)
	if err != nil {
		return "<unavailable>"
	}
	return string(encoded)
}

func binaryFrameSummary(frame []byte) string {
	if len(frame) < mobileFrameHeader {
		return fmt.Sprintf("bytes=%d", len(frame))
	}
	return fmt.Sprintf("cmd=%d seq=%d opcode=%d flags=%d body=%d",
		frame[1], binary.BigEndian.Uint16(frame[2:4]), binary.BigEndian.Uint16(frame[4:6]), frame[6], len(frame)-mobileFrameHeader)
}

// buildWebSocketFrame encodes the protocol used by the current MAX web
// application: a 10-byte command header followed by a MessagePack payload.
// The payload is LZ4-compressed (flag 2), as in the browser client.
func buildWebSocketFrame(config *Config, request WebSocketPayload) ([]byte, error) {
	seq, ok := parseInt(request["seq"])
	if !ok {
		return nil, NewConnectionError("WebSocket request missing sequence number")
	}
	opcode, ok := parseInt(request["opcode"])
	if !ok {
		return nil, NewConnectionError("WebSocket request missing opcode")
	}

	var body interface{}
	if opcode == int(INIT) {
		if config == nil || config.UserAgent == nil || config.DeviceID == "" {
			return nil, NewConnectionError("binary WebSocket INIT requires device_id and user_agent")
		}
		ua := config.UserAgent
		body = webBinarySessionInit{
			UserAgent: webBinaryUserAgent{
				DeviceType: "WEB", PushDeviceType: "WEBPUSH", Locale: ua.Locale,
				DeviceLocale: ua.DeviceLocale, OSVersion: ua.OSVersion, DeviceName: ua.DeviceName,
				HeaderUserAgent: ua.UserAgent, IsPWA: false, AppVersion: ua.AppVersion,
				Screen: ua.Screen, Timezone: ua.Timezone,
			},
			DeviceID: config.DeviceID,
		}
	} else {
		var exists bool
		body, exists = request["payload"]
		if !exists {
			body = map[string]interface{}{}
		}
	}

	return buildMobileRequestFrame(uint16(seq), uint16(opcode), body, 2, nil)
}

// decodeWebSocketFrame converts a MAX binary WebSocket response into the
// envelope used by the rest of the bridge.
func decodeWebSocketFrame(frame []byte) (WebSocketPayload, error) {
	if len(frame) < mobileFrameHeader {
		return nil, fmt.Errorf("binary frame is too short: %d bytes", len(frame))
	}
	if frame[0] != 0x0a {
		return nil, fmt.Errorf("invalid binary frame magic %#x", frame[0])
	}

	length := int(frame[7])<<16 | int(frame[8])<<8 | int(frame[9])
	if len(frame) != mobileFrameHeader+length {
		return nil, fmt.Errorf("invalid binary frame length: header=%d actual=%d", length, len(frame)-mobileFrameHeader)
	}
	opcode := int(binary.BigEndian.Uint16(frame[4:6]))
	// The binary API acknowledges PING with an empty response body. It is a
	// valid pong, not a malformed MessagePack packet.
	if length == 0 && opcode == int(PING) {
		return WebSocketPayload{
			"cmd":     int(frame[1]),
			"seq":     int(binary.BigEndian.Uint16(frame[2:4])),
			"opcode":  opcode,
			"payload": map[string]interface{}{},
		}, nil
	}

	body, err := decodeMobilePayload(frame[mobileFrameHeader:], frame[6])
	if err != nil {
		return nil, err
	}
	return WebSocketPayload{
		"cmd":     int(frame[1]),
		"seq":     int(binary.BigEndian.Uint16(frame[2:4])),
		"opcode":  opcode,
		"payload": body,
	}, nil
}

func (c *Connection) sendAndReceive(payload WebSocketPayload, timeout time.Duration) (WebSocketPayload, error) {
	if !c.connected {
		return nil, NewConnectionError("WebSocket not connected")
	}

	seqRaw, ok := payload["seq"]
	if !ok {
		return nil, NewConnectionError("payload missing seq field")
	}
	seq, ok := seqRaw.(int)
	if !ok {
		return nil, NewConnectionError(fmt.Sprintf("payload seq has unexpected type %T", seqRaw))
	}
	responseChan := make(chan WebSocketPayload, 1)

	c.mu.Lock()
	c.responseFutures[seq] = responseChan
	c.mu.Unlock()

	if err := c.send(payload); err != nil {
		c.mu.Lock()
		delete(c.responseFutures, seq)
		c.mu.Unlock()
		return nil, err
	}

	select {
	case response, ok := <-responseChan:
		if !ok {
			return nil, NewConnectionError("connection lost while waiting for response")
		}
		return response, nil
	case <-time.After(timeout):
		c.mu.Lock()
		delete(c.responseFutures, seq)
		c.mu.Unlock()
		return nil, NewTimeoutError("request timeout")
	}
}

func (c *Connection) Receive() (WebSocketPayload, error) {
	select {
	case payload, ok := <-c.messageQueue:
		if !ok {
			return nil, NewConnectionError("message queue closed")
		}
		return payload, nil
	case <-c.stopCh:
		return nil, NewConnectionError("receiver stopped")
	}
}

func (c *Connection) HandleReconnect(reason string) {
	c.reconnectLock.Lock()

	if c.manualStop {
		c.manualStop = false
		c.connected = false
		c.reconnectLock.Unlock()
		return
	}

	if c.performingReconnect {
		Logf("Reconnect already in progress, skipping duplicate call")
		c.reconnectLock.Unlock()
		return
	}

	c.performingReconnect = true
	c.reconnectLock.Unlock()

	c.mu.Lock()
	for seq, ch := range c.responseFutures {
		close(ch)
		delete(c.responseFutures, seq)
	}
	c.mu.Unlock()

	if c.client != nil {
		for _, handler := range c.client.GetDisconnectedHandlers() {
			handler(reason)
		}
		for _, handler := range c.client.GetBeforeReconnectHandlers() {
			handler(reason)
		}
	}

	c.reconnectAttempts++
	Logf("Reconnect attempt #%d due to: %s", c.reconnectAttempts, reason)

	delay := c.currentDelay
	if c.currentDelay < c.maxBackoffDelay {
		c.currentDelay = time.Duration(float64(c.currentDelay) * reconnectDelayMultiplier)
		if c.currentDelay > c.maxBackoffDelay {
			c.currentDelay = c.maxBackoffDelay
		}
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	if c.conn != nil {
		c.conn.Close()
	}
	c.connected = false

	for {
		if err := c.Connect(); err != nil {
			Logf("Reconnect failed: %v, retrying...", err)
			retryDelay := time.Duration(float64(c.currentDelay) * 1.5)
			time.Sleep(retryDelay)
			continue
		}

		if c.client != nil {
			response, err := c.Authenticate(40)
			if err != nil {
				Logf("Authentication after reconnect failed: %v", err)
				c.closeStopCh()
				if c.conn != nil {
					c.conn.Close()
				}
				c.connected = false
				time.Sleep(2 * time.Second)
				continue
			}

			if payload, ok := response["payload"].(map[string]interface{}); ok {
				c.client.SetMe(parseProfile(payload))
				if chatsData, ok := payload["chats"].([]interface{}); ok {
					chats := parseChats(castToMapArray(chatsData))
					c.client.SetChats(chats)
				}
			}
			c.client.SetConnectionTime(time.Now())

			for _, handler := range c.client.GetAfterReconnectHandlers() {
				handler()
			}
		}

		break
	}

	c.reconnectLock.Lock()
	c.performingReconnect = false
	c.reconnectLock.Unlock()
}

func castToMapArray(arr []interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, len(arr))
	for i, item := range arr {
		if m, ok := item.(map[string]interface{}); ok {
			result[i] = m
		}
	}
	return result
}

func (c *Connection) Close() {
	c.reconnectLock.Lock()
	c.manualStop = true
	c.reconnectLock.Unlock()

	if c.stopCh != nil {
		c.closeStopCh()
	}

	if c.conn != nil && c.connected {
		c.conn.Close()
	}
	c.connected = false
	c.requestBuilder = NewRequestBuilder(c.config)

	if c.pingTicker != nil {
		c.pingTicker.Stop()
	}

	oldQueue := c.messageQueue
	c.messageQueue = make(chan WebSocketPayload, 1000)
	close(oldQueue)
	for range oldQueue {
	}

	c.mu.Lock()
	for seq, ch := range c.responseFutures {
		close(ch)
		delete(c.responseFutures, seq)
	}
	c.mu.Unlock()

	c.reconnectAttempts = 0
	c.currentDelay = defaultReconnectDelay
	c.performingReconnect = false
}

func (c *Connection) IsConnected() bool {
	return c.connected
}

func (c *Connection) IsReconnecting() bool {
	c.reconnectLock.Lock()
	defer c.reconnectLock.Unlock()
	return c.performingReconnect
}

func (c *Connection) GetRequestBuilder() *RequestBuilder {
	return c.requestBuilder
}
