package src

import (
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func TestBuildWebSocketFrameUsesBinaryWebProtocol(t *testing.T) {
	cfg := &Config{
		DeviceID: "device-id",
		UserAgent: &UserAgentConfig{
			UserAgent: "test-agent", Locale: "en", DeviceLocale: "en-US",
			OSVersion: "Linux", DeviceName: "Firefox", AppVersion: "26.11.9",
			Screen: "1080x1920 1.0x", Timezone: "Europe/Samara",
		},
	}
	frame, err := buildWebSocketFrame(cfg, WebSocketPayload{"seq": 0, "opcode": int(INIT), "payload": map[string]interface{}{}})
	if err != nil {
		t.Fatalf("buildWebSocketFrame: %v", err)
	}
	if frame[0] != 0x0a || binary.BigEndian.Uint16(frame[4:6]) != uint16(INIT) || frame[6] != 2 {
		t.Fatalf("unexpected frame header: %x", frame[:mobileFrameHeader])
	}
	payload, err := decodeMobilePayload(frame[mobileFrameHeader:], frame[6])
	if err != nil {
		t.Fatalf("decodeMobilePayload: %v", err)
	}
	if payload["deviceId"] != cfg.DeviceID {
		t.Fatalf("deviceId = %v, want %q", payload["deviceId"], cfg.DeviceID)
	}
}

func TestDecodeWebSocketFrame(t *testing.T) {
	frame, err := buildMobileRequestFrame(7, uint16(GET_CHATS), map[string]interface{}{"chats": []interface{}{}}, 2, nil)
	if err != nil {
		t.Fatalf("buildMobileRequestFrame: %v", err)
	}
	frame[1] = 1 // response command
	payload, err := decodeWebSocketFrame(frame)
	if err != nil {
		t.Fatalf("decodeWebSocketFrame: %v", err)
	}
	if cmd, _ := parseInt(payload["cmd"]); cmd != 1 {
		t.Fatalf("cmd = %d, want 1", cmd)
	}
	if seq, _ := parseInt(payload["seq"]); seq != 7 {
		t.Fatalf("seq = %d, want 7", seq)
	}
	if opcode, _ := parseInt(payload["opcode"]); opcode != int(GET_CHATS) {
		t.Fatalf("opcode = %d, want %d", opcode, GET_CHATS)
	}
}

func TestDecodeWebSocketFrameAcceptsEmptyPing(t *testing.T) {
	frame := make([]byte, mobileFrameHeader)
	frame[0] = 0x0a
	frame[1] = 1
	binary.BigEndian.PutUint16(frame[2:4], 12)
	binary.BigEndian.PutUint16(frame[4:6], uint16(PING))
	payload, err := decodeWebSocketFrame(frame)
	if err != nil {
		t.Fatalf("decodeWebSocketFrame: %v", err)
	}
	if opcode, _ := parseInt(payload["opcode"]); opcode != int(PING) {
		t.Fatalf("opcode = %d, want %d", opcode, PING)
	}
	if _, ok := payload["payload"].(map[string]interface{}); !ok {
		t.Fatalf("empty PING response payload has type %T", payload["payload"])
	}
}

func TestDecodeMAXIntegerExtension(t *testing.T) {
	body, err := hex.DecodeString("82a6636861744964c70501d20de1e13aa6757365724964c70501d200210cc4")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := decodeMobilePayload(body, 0)
	if err != nil {
		t.Fatalf("decodeMobilePayload: %v", err)
	}
	if chatID, ok := parseInt(payload["chatId"]); !ok || chatID != 232907066 {
		t.Fatalf("chatId = %v, want 232907066", payload["chatId"])
	}
}

func TestMessageIDRequestsUseIntegers(t *testing.T) {
	builder := NewRequestBuilder(nil)
	const messageID int64 = 117640012376257622

	tests := []struct {
		name    string
		payload WebSocketPayload
		key     string
	}{
		{"get", builder.GetMessage(42, messageID), "messageIds"},
		{"edit", builder.EditMessage(42, messageID, "text", nil), "messageId"},
		{"delete", builder.DeleteMessage(42, messageID), "messageIds"},
		{"reaction", builder.SetReaction(42, messageID, "👍"), "messageId"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := tt.payload["payload"].(map[string]interface{})
			value := body[tt.key]
			if ids, ok := value.([]int64); ok {
				if len(ids) != 1 || ids[0] != messageID {
					t.Fatalf("message IDs = %v, want [%d]", ids, messageID)
				}
				return
			}
			if id, ok := value.(int64); !ok || id != messageID {
				t.Fatalf("message ID has type/value %T/%v, want int64/%d", value, value, messageID)
			}
		})
	}
}
