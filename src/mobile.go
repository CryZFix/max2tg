package src

// The mobile protocol is deliberately kept separate from Connection: MAX uses
// JSON over WebSocket for web sessions and framed MessagePack over TLS for
// Android sessions.  Audio upload tokens are scoped to the latter.

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	mobileHost        = "api.oneme.ru:443"
	mobileFrameHeader = 10
	mobileSessionFile = "data/max-mobile-session.json"
	// Keep this paired version/build fingerprint from a currently supported
	// Android release. The server rejects older versions during AUTH_REQUEST.
	mobileAppVersion  = "26.30.1"
	mobileBuildNumber = 6819
	// The mobile fingerprint should agree with the bridge host's declared
	// location. This deployment runs in Samara (UTC+4), not Moscow.
	mobileTimezone = "Europe/Samara"
)

// MobileSession contains only credentials that are unavailable in a web MAX
// session. It is written with mode 0600 by SaveMobileSession.
type MobileSession struct {
	DeviceID string `json:"device_id"`
	// Retained to read sessions written by older builds. Current SessionInit
	// identifies a client with clientSessionId instead.
	InstanceID string `json:"instance_id,omitempty"`
	LoginToken string `json:"login_token"`
	CreatedAt  string `json:"created_at"`
}

func (c *Config) MobileSessionPath() string {
	if c.MobileSessionFile != "" {
		return c.MobileSessionFile
	}
	return mobileSessionFile
}

func LoadMobileSession(path string) (*MobileSession, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("mobile session is not configured; run `max2tg mobile-login`")
		}
		return nil, err
	}
	var session MobileSession
	if err := json.Unmarshal(b, &session); err != nil {
		return nil, fmt.Errorf("invalid mobile session: %w", err)
	}
	if !isMobileDeviceID(session.DeviceID) || session.LoginToken == "" {
		return nil, errors.New("mobile session is incomplete; run `max2tg mobile-login`")
	}
	return &session, nil
}

func SaveMobileSession(path string, session *MobileSession) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(session, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, b, 0600); err != nil {
		return err
	}
	return os.Chmod(path, 0600)
}

// The HELLO payload has to match the Android client byte-for-byte. Only these
// two installation identifiers are variable.
const mobileHelloTemplateHex = "0a000001000602000130f35a84ad6d745f696e7374616e63656964d92430393437366632372d663230362d343139362d383662612d656537663434643566363233a9757365724167656e748baa64657669636554797065a7414e44524f4944aa61707056657273696f6ea732362e31352e31a96f731200f322aa416e64726f6964203134a874696d657a6f6e65ad457ುರ6f70652f4d6f73636f77a673637265656eb7343430647069200700f500313038307832313338ae70757368447500f31aa347434da461726368a67838365f3634a66c6f63616c65a27275ab6275696c644e756d626572cd1a22a900f20a4e616d65ba476f6f676c652073646b5f6770686f6e6536345f3f0012accf00144c4500a0af636c69656e74536573cf004249640ba82100f0044964b066646630393164333564346535343162"

// NOTE: the literal above is patched in init because older sources used a
// unicode-escaped Europe segment. The canonical bytes are supplied below.
const mobileHelloTemplateCanonical = "0a000001000602000130f35a84ad6d745f696e7374616e63656964d92430393437366632372d663230362d343139362d383662612d656537663434643566363233a9757365724167656e748baa64657669636554797065a7414e44524f4944aa61707056657273696f6ea732362e31352e31a96f731200f322aa416e64726f6964203134a874696d657a6f6e65ad4575726f70652f4d6f73636f77a673637265656eb7343430647069200700f500313038307832313338ae70757368447500f31aa347434da461726368a67838365f3634a66c6f63616c65a27275ab6275696c644e756d626572cd1a22a900f20a4e616d65ba476f6f676c652073646b5f6770686f6e6536345f3f0012accf00144c4500a0af636c69656e74536573cf004249640ba82100f0044964b066646630393164333564346535343162"

var mobileHelloTemplate = mustHelloTemplate()

func mustHelloTemplate() []byte {
	b, err := hex.DecodeString(mobileHelloTemplateCanonical)
	if err != nil {
		panic(err)
	}
	return b
}

type mobileTransport struct {
	conn        net.Conn
	seq         uint16
	debug       io.Writer
	unsafeDebug bool
}

// Structures, rather than maps, preserve the MessagePack field ordering used
// by the Android SessionInit fingerprint.
type mobileUserAgent struct {
	DeviceType     string `msgpack:"deviceType"`
	PushDeviceType string `msgpack:"pushDeviceType"`
	AppVersion     string `msgpack:"appVersion"`
	Arch           string `msgpack:"arch"`
	BuildNumber    int    `msgpack:"buildNumber"`
	OSVersion      string `msgpack:"osVersion"`
	Locale         string `msgpack:"locale"`
	DeviceLocale   string `msgpack:"deviceLocale"`
	DeviceName     string `msgpack:"deviceName"`
	Screen         string `msgpack:"screen"`
	Timezone       string `msgpack:"timezone"`
	NetworkType    string `msgpack:"networkType"`
}

type mobileSessionInit struct {
	UserAgent       mobileUserAgent `msgpack:"userAgent"`
	DeviceID        string          `msgpack:"deviceId"`
	ClientSessionID int64           `msgpack:"clientSessionId"`
}

type mobileStartAuth struct {
	Phone string `msgpack:"phone"`
	Type  string `msgpack:"type"`
}

type mobileVerifyAuth struct {
	Token         string `msgpack:"token"`
	VerifyCode    string `msgpack:"verifyCode"`
	AuthTokenType string `msgpack:"authTokenType"`
}

type mobileLoginRequest struct {
	ChatCacheFingerprint []byte                 `msgpack:"chatCacheFingerprint"`
	Exp                  map[string]interface{} `msgpack:"exp"`
	Token                string                 `msgpack:"token"`
	PresenceSync         int                    `msgpack:"presenceSync"`
	Interactive          bool                   `msgpack:"interactive"`
}

// webBinaryUserAgent mirrors the browser's binary voice transport rather than
// the JSON WebSocket user-agent. Field order is significant for MessagePack.
type webBinaryUserAgent struct {
	DeviceType      string `msgpack:"deviceType"`
	PushDeviceType  string `msgpack:"pushDeviceType"`
	Locale          string `msgpack:"locale"`
	DeviceLocale    string `msgpack:"deviceLocale"`
	OSVersion       string `msgpack:"osVersion"`
	DeviceName      string `msgpack:"deviceName"`
	HeaderUserAgent string `msgpack:"headerUserAgent"`
	IsPWA           bool   `msgpack:"isPwa"`
	AppVersion      string `msgpack:"appVersion"`
	Screen          string `msgpack:"screen"`
	Timezone        string `msgpack:"timezone"`
}

type webBinarySessionInit struct {
	UserAgent webBinaryUserAgent `msgpack:"userAgent"`
	DeviceID  string             `msgpack:"deviceId"`
}

type webBinaryLoginRequest struct {
	Token        string `msgpack:"token"`
	ChatsCount   int    `msgpack:"chatsCount"`
	Interactive  bool   `msgpack:"interactive"`
	ChatsSync    int    `msgpack:"chatsSync"`
	ContactsSync int    `msgpack:"contactsSync"`
	PresenceSync int    `msgpack:"presenceSync"`
	DraftsSync   int    `msgpack:"draftsSync"`
}

func dialMobile(config *Config, debug io.Writer, unsafeDebug ...bool) (*mobileTransport, error) {
	dialer, err := BuildSOCKS5Dialer(GetMaxProxy(config))
	if err != nil {
		return nil, err
	}
	raw, err := dialer.Dial("tcp", mobileHost)
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(raw, &tls.Config{ServerName: "api.oneme.ru", MinVersion: tls.VersionTLS12})
	if err := tlsConn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		raw.Close()
		return nil, err
	}
	if err := tlsConn.Handshake(); err != nil {
		raw.Close()
		return nil, err
	}
	_ = tlsConn.SetDeadline(time.Time{})
	transport := &mobileTransport{conn: tlsConn, debug: debug}
	if len(unsafeDebug) > 0 {
		transport.unsafeDebug = unsafeDebug[0]
	}
	transport.debugf("TLS connected to %s", mobileHost)
	return transport, nil
}

func (t *mobileTransport) Close() { _ = t.conn.Close() }

func (t *mobileTransport) hello(session *MobileSession) (map[string]interface{}, error) {
	if !isMobileDeviceID(session.DeviceID) {
		return nil, errors.New("mobile device ID must be 16 lowercase hex characters")
	}
	frame, err := buildMobileSessionInitFrame(session.DeviceID, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	t.seq = 1
	t.debugf("TX op=6 seq=1 flags=0 bytes=%d payload=%s", len(frame), t.debugPayload(frame[mobileFrameHeader:]))
	return t.roundTripFrame(frame, 6, 1)
}

func (t *mobileTransport) webHello(config *Config) (map[string]interface{}, error) {
	if config == nil || config.DeviceID == "" || config.UserAgent == nil {
		return nil, errors.New("web binary HELLO requires device_id and user_agent")
	}
	ua := config.UserAgent
	payload := webBinarySessionInit{
		UserAgent: webBinaryUserAgent{
			DeviceType: "WEB", PushDeviceType: "WEBPUSH", Locale: ua.Locale,
			DeviceLocale: ua.DeviceLocale, OSVersion: ua.OSVersion, DeviceName: ua.DeviceName,
			HeaderUserAgent: ua.UserAgent, IsPWA: false, AppVersion: ua.AppVersion,
			Screen: ua.Screen, Timezone: ua.Timezone,
		},
		DeviceID: config.DeviceID,
	}
	frame, err := buildMobileRequestFrame(0, 6, payload, 2, nil)
	if err != nil {
		return nil, err
	}
	t.seq = 0
	t.debugf("TX op=6 seq=0 flags=2 bytes=%d payload=%s", len(frame), t.debugPayload(frame[mobileFrameHeader:]))
	return t.roundTripFrame(frame, 6, 0)
}

func buildMobileSessionInitFrame(deviceID string, clientSessionID int64) ([]byte, error) {
	payload := mobileSessionInit{
		UserAgent: mobileUserAgent{
			DeviceType: "ANDROID", PushDeviceType: "GCM", AppVersion: mobileAppVersion,
			Arch: "arm64-v8a", BuildNumber: mobileBuildNumber, OSVersion: "34",
			Locale: "ru", DeviceLocale: "ru_RU", DeviceName: "Android device",
			Screen: "1080x1920", Timezone: mobileTimezone, NetworkType: "wifi",
		},
		DeviceID: deviceID, ClientSessionID: clientSessionID,
	}
	return buildMobileRequestFrame(1, 6, payload, 0, nil)
}

func (t *mobileTransport) request(opcode uint16, payload interface{}, flags byte, prefix []byte) (map[string]interface{}, error) {
	t.seq++
	frame, err := buildMobileRequestFrame(t.seq, opcode, payload, flags, prefix)
	if err != nil {
		return nil, err
	}
	t.debugf("TX op=%d seq=%d flags=%d bytes=%d payload=%s", opcode, t.seq, flags, len(frame), t.debugPayload(frame[mobileFrameHeader:]))
	return t.roundTripFrame(frame, opcode, t.seq)
}

func buildMobileRequestFrame(seq, opcode uint16, payload interface{}, flags byte, prefix []byte) ([]byte, error) {
	body, err := msgpack.Marshal(payload)
	if err != nil {
		return nil, err
	}
	body = append(append([]byte(nil), prefix...), body...)
	if flags&2 != 0 {
		body, err = compressMobilePayload(body)
		if err != nil {
			return nil, err
		}
	}
	frame := make([]byte, mobileFrameHeader+len(body))
	frame[0] = 0x0a
	frame[1] = 0
	binary.BigEndian.PutUint16(frame[2:4], seq)
	binary.BigEndian.PutUint16(frame[4:6], opcode)
	frame[6] = flags
	frame[7] = byte(len(body) >> 16)
	frame[8] = byte(len(body) >> 8)
	frame[9] = byte(len(body))
	copy(frame[10:], body)
	return frame, nil
}

func compressMobilePayload(body []byte) ([]byte, error) {
	compressed := make([]byte, lz4.CompressBlockBound(len(body)))
	n, err := lz4.CompressBlockHC(body, compressed, lz4.Level9, nil, nil)
	if err != nil {
		return nil, err
	}
	if n > 0 {
		return compressed[:n], nil
	}
	// The protocol still expects a valid LZ4 block when the body is too small
	// to benefit from compression. Encode it as one literal-only sequence.
	result := make([]byte, 0, len(body)+len(body)/255+2)
	if len(body) < 15 {
		result = append(result, byte(len(body)<<4))
	} else {
		result = append(result, 0xf0)
		remaining := len(body) - 15
		for remaining >= 255 {
			result = append(result, 255)
			remaining -= 255
		}
		result = append(result, byte(remaining))
	}
	return append(result, body...), nil
}

func (t *mobileTransport) roundTripFrame(frame []byte, wantOp, wantSeq uint16) (map[string]interface{}, error) {
	_ = t.conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer t.conn.SetDeadline(time.Time{})
	if _, err := t.conn.Write(frame); err != nil {
		return nil, err
	}
	for {
		header := make([]byte, mobileFrameHeader)
		if _, err := io.ReadFull(t.conn, header); err != nil {
			return nil, err
		}
		if header[0] != 0x0a {
			return nil, fmt.Errorf("invalid mobile frame magic %#x", header[0])
		}
		length := int(header[7])<<16 | int(header[8])<<8 | int(header[9])
		body := make([]byte, length)
		if _, err := io.ReadFull(t.conn, body); err != nil {
			return nil, err
		}
		op := binary.BigEndian.Uint16(header[4:6])
		seq := binary.BigEndian.Uint16(header[2:4])
		if op != wantOp || seq != wantSeq {
			continue // Push notification; the short-lived transport does not use it.
		}
		payload, err := decodeMobilePayload(body, header[6])
		if err != nil {
			t.debugf("RX op=%d seq=%d flags=%d bytes=%d decode=failed", op, seq, header[6], length)
			return nil, err
		}
		t.debugf("RX op=%d seq=%d flags=%d bytes=%d payload=%s", op, seq, header[6], length, t.debugValue(payload))
		if serverError, _ := payload["error"].(string); serverError != "" {
			return nil, errors.New(serverError)
		}
		return payload, nil
	}
}

func (t *mobileTransport) debugf(format string, args ...interface{}) {
	if t.debug == nil {
		return
	}
	fmt.Fprintf(t.debug, "[mobile-debug] "+format+"\n", args...)
}

func (t *mobileTransport) debugValue(value interface{}) string {
	if !t.unsafeDebug {
		return mobileDebugSchema(value)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "<unavailable>"
	}
	return string(encoded)
}

func (t *mobileTransport) debugPayload(body []byte) string {
	payload, err := decodeMobilePayload(body, 0)
	if err != nil {
		return "<unavailable>"
	}
	return t.debugValue(payload)
}

func mobileDebugSchema(value interface{}) string {
	encoded, err := json.Marshal(redactMobileDebugValue(value, ""))
	if err != nil {
		return "<unavailable>"
	}
	return string(encoded)
}

func redactMobileDebugValue(value interface{}, key string) interface{} {
	if isSensitiveMobileDebugKey(key) {
		return "<redacted>"
	}
	switch typed := value.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		result := make(map[string]interface{}, len(typed))
		for _, nestedKey := range keys {
			result[nestedKey] = redactMobileDebugValue(typed[nestedKey], nestedKey)
		}
		return result
	case []interface{}:
		if len(typed) == 0 {
			return []interface{}{}
		}
		return []interface{}{redactMobileDebugValue(typed[0], key)}
	case string:
		if isSafeMobileDebugKey(key) {
			return typed
		}
		return "<string>"
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return typed
	case nil:
		return nil
	default:
		return fmt.Sprintf("<%T>", value)
	}
}

func isSensitiveMobileDebugKey(key string) bool {
	key = strings.ToLower(key)
	for _, marker := range []string{"token", "secret", "password", "phone", "code", "signature", "url", "device", "instance"} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}

func isSafeMobileDebugKey(key string) bool {
	switch key {
	case "status", "type", "error", "delivery", "deliveryType", "channel", "reason":
		return true
	default:
		return false
	}
}

func decodeMobilePayload(body []byte, flags byte) (map[string]interface{}, error) {
	working := body
	if flags != 0 {
		var decoded []byte
		for size := len(body)*3 + 1024; size <= len(body)*512+1024; size *= 2 {
			candidate := make([]byte, size)
			if n, err := lz4.UncompressBlock(body, candidate); err == nil && n > 0 {
				decoded = candidate[:n]
				break
			}
		}
		if len(decoded) > 0 {
			working = decoded
		}
	}
	for offset := 0; offset < len(working) && offset < 12; offset++ {
		h := working[offset]
		if !((h >= 0x80 && h <= 0x8f) || h == 0xde || h == 0xdf) {
			continue
		}
		var result map[string]interface{}
		if err := msgpack.NewDecoder(bytes.NewReader(working[offset:])).Decode(&result); err == nil && result != nil {
			return result, nil
		}
	}
	return nil, errors.New("could not decode mobile response")
}

func isMobileDeviceID(value string) bool {
	if len(value) != 16 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func newMobileSession() (*MobileSession, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	instance, err := randomUUID()
	if err != nil {
		return nil, err
	}
	return &MobileSession{DeviceID: hex.EncodeToString(b), InstanceID: instance, CreatedAt: time.Now().UTC().Format(time.RFC3339)}, nil
}

func randomUUID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", binary.BigEndian.Uint32(b[:4]), binary.BigEndian.Uint16(b[4:6]), binary.BigEndian.Uint16(b[6:8]), binary.BigEndian.Uint16(b[8:10]), b[10:]), nil
}

type MobileLoginOptions struct {
	Debug bool
}

func (t *mobileTransport) webBinaryLogin(token string) (map[string]interface{}, error) {
	return t.request(19, webBinaryLoginRequest{
		Token: token, ChatsCount: 15, Interactive: true,
		ChatsSync: 0, ContactsSync: 0, PresenceSync: 0, DraftsSync: 0,
	}, 2, nil)
}

// LoginMobileSession performs the only interactive operation. The saved token
// is later used by SendNativeVoice without needing a phone or SMS again.
func LoginMobileSession(config *Config, input io.Reader, output io.Writer) error {
	return LoginMobileSessionWithOptions(config, input, output, MobileLoginOptions{})
}

func LoginMobileSessionWithOptions(config *Config, input io.Reader, output io.Writer, options MobileLoginOptions) error {
	reader := bufio.NewReader(input)
	ask := func(label string) (string, error) {
		fmt.Fprint(output, label)
		value, err := reader.ReadString('\n')
		return strings.TrimSpace(value), err
	}
	phone, err := ask("MAX phone (+79990000000): ")
	if err != nil || phone == "" {
		return errors.New("phone number was not provided")
	}
	session, err := newMobileSession()
	if err != nil {
		return err
	}
	var debug io.Writer
	if options.Debug {
		debug = output
		fmt.Fprintln(output, "[mobile-debug] UNSAFE: payload values include phone numbers, SMS codes, tokens and signed URLs. Do not share or save this output.")
	}
	transport, err := dialMobile(config, debug, options.Debug)
	if err != nil {
		return fmt.Errorf("connect mobile MAX: %w", err)
	}
	defer transport.Close()
	if _, err := transport.hello(session); err != nil {
		return fmt.Errorf("mobile HELLO: %w", err)
	}
	// Android marks START_AUTH with the observed f0/15 subtype and frame flag.
	// The server may return an empty verification token for the plain-map form,
	// even though it sends no explicit protocol error.
	start, err := transport.request(17, mobileStartAuth{Phone: phone, Type: "START_AUTH"}, 1, []byte{0xf0, 0x15})
	if err != nil {
		return fmt.Errorf("request SMS: %w", err)
	}
	authToken, _ := start["token"].(string)
	if authToken == "" {
		return errors.New("SMS request returned no token")
	}
	code, err := ask("SMS code: ")
	if err != nil || code == "" {
		return errors.New("SMS code was not provided")
	}
	verified, err := transport.request(18, mobileVerifyAuth{Token: authToken, VerifyCode: code, AuthTokenType: "CHECK_CODE"}, 0, nil)
	if err != nil {
		return fmt.Errorf("verify SMS: %w", err)
	}
	session.LoginToken = loginTokenFromResponse(verified)
	if session.LoginToken == "" {
		challenge, _ := verified["passwordChallenge"].(map[string]interface{})
		trackID, _ := challenge["trackId"].(string)
		if trackID != "" {
			password, askErr := ask("MAX 2FA cloud password: ")
			if askErr != nil || password == "" {
				return errors.New("2FA password was not provided")
			}
			final, requestErr := transport.request(115, struct {
				TrackID  string `msgpack:"trackId"`
				Password string `msgpack:"password"`
			}{TrackID: trackID, Password: password}, 0, nil)
			if requestErr != nil {
				return fmt.Errorf("verify 2FA password: %w", requestErr)
			}
			session.LoginToken = loginTokenFromResponse(final)
		}
	}
	if session.LoginToken == "" {
		return errors.New("mobile login token was not issued")
	}
	if _, err := transport.mobileLogin(session.LoginToken); err != nil {
		return fmt.Errorf("validate mobile login: %w", err)
	}
	if err := SaveMobileSession(config.MobileSessionPath(), session); err != nil {
		return err
	}
	fmt.Fprintf(output, "Mobile session saved to %s\n", config.MobileSessionPath())
	return nil
}

func loginTokenFromResponse(response map[string]interface{}) string {
	attrs, _ := response["tokenAttrs"].(map[string]interface{})
	login, _ := attrs["LOGIN"].(map[string]interface{})
	token, _ := login["token"].(string)
	return token
}

func (t *mobileTransport) mobileLogin(token string) (map[string]interface{}, error) {
	// Older captures prepend an f0 subtype to this operation. Current Android
	// clients use an ordinary MessagePack login request, like auth operations.
	return t.request(19, mobileLoginRequest{
		ChatCacheFingerprint: make([]byte, 96),
		Exp:                  map[string]interface{}{"chatsCountGroups": []byte{0x09, 0x32}},
		Token:                token,
		PresenceSync:         0,
		Interactive:          true,
	}, 0, nil)
}
