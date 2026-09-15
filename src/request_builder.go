package src

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

type RequestBuilder struct {
	config  *Config
	version int
	seq     int
	mu      sync.Mutex
}

func NewRequestBuilder(config *Config) *RequestBuilder {
	return &RequestBuilder{
		config:  config,
		version: 11,
		seq:     0,
	}
}

func (rb *RequestBuilder) buildBaseRequest(opcode Opcode, payload map[string]interface{}) WebSocketPayload {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	request := WebSocketPayload{
		"ver":     rb.version,
		"cmd":     0,
		"seq":     rb.seq,
		"opcode":  int(opcode),
		"payload": payload,
	}
	rb.seq++
	return request
}

func (rb *RequestBuilder) incrementSeq() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.seq++
}

func (rb *RequestBuilder) ImageUpload(count int) WebSocketPayload {
	return rb.buildBaseRequest(IMAGE_UPLOAD, map[string]interface{}{"count": count})
}

func (rb *RequestBuilder) VideoUpload(chatID, count int) WebSocketPayload {
	return rb.buildBaseRequest(VIDEO_UPLOAD, map[string]interface{}{"chatId": chatID, "count": count})
}

func (rb *RequestBuilder) FileUpload(name string, size int64) WebSocketPayload {
	ext := ""
	if dot := strings.LastIndex(name, "."); dot >= 0 && dot < len(name)-1 {
		ext = name[dot+1:]
	}
	return rb.buildBaseRequest(FILE_UPLOAD, map[string]interface{}{"name": name, "size": size, "ext": ext, "count": 1})
}

func (rb *RequestBuilder) Init() WebSocketPayload {
	ua := rb.config.UserAgent
	payload := map[string]interface{}{
		"userAgent": map[string]interface{}{
			"deviceType":      "WEB",
			"locale":          ua.Locale,
			"deviceLocale":    ua.DeviceLocale,
			"osVersion":       ua.OSVersion,
			"deviceName":      ua.DeviceName,
			"headerUserAgent": ua.UserAgent,
			"appVersion":      ua.AppVersion,
			"screen":          ua.Screen,
			"timezone":        ua.Timezone,
		},
		"deviceId": rb.config.DeviceID,
	}
	return rb.buildBaseRequest(INIT, payload)
}

func (rb *RequestBuilder) Ping() WebSocketPayload {
	payload := map[string]interface{}{
		"interactive": true,
	}
	return rb.buildBaseRequest(PING, payload)
}

func (rb *RequestBuilder) Authenticate(chatsCount int) WebSocketPayload {
	payload := map[string]interface{}{
		"interactive":  true,
		"token":        rb.config.Token,
		"chatsCount":   chatsCount,
		"chatsSync":    0,
		"contactsSync": 0,
		"presenceSync": 0,
		"draftsSync":   0,
	}
	return rb.buildBaseRequest(AUTH, payload)
}

func (rb *RequestBuilder) GetContacts(contactIDs []int) WebSocketPayload {
	payload := map[string]interface{}{
		"contactIds": contactIDs,
	}
	return rb.buildBaseRequest(GET_CONTACTS, payload)
}

func (rb *RequestBuilder) GetChatMessages(chatID int, fromTime int64, forward, backward int) WebSocketPayload {
	payload := map[string]interface{}{
		"chatId":      chatID,
		"from":        fromTime,
		"forward":     forward,
		"backward":    backward,
		"getMessages": true,
	}
	return rb.buildBaseRequest(GET_MESSAGES, payload)
}

func (rb *RequestBuilder) GetMessage(chatID int, messageID int64) WebSocketPayload {
	payload := map[string]interface{}{
		"chatId":     chatID,
		"messageIds": []string{strconv.FormatInt(messageID, 10)},
	}
	return rb.buildBaseRequest(GET_MESSAGE, payload)
}

func (rb *RequestBuilder) EditMessage(chatID int, messageID int64, text string, attachments []interface{}) WebSocketPayload {
	payload := map[string]interface{}{
		"chatId":      chatID,
		"messageId":   strconv.FormatInt(messageID, 10),
		"text":        text,
		"elements":    []interface{}{},
		"attachments": attachments,
	}
	return rb.buildBaseRequest(EDIT_MESSAGE, payload)
}

func (rb *RequestBuilder) DeleteMessage(chatID int, messageID int64) WebSocketPayload {
	payload := map[string]interface{}{
		"chatId":     chatID,
		"messageIds": []string{strconv.FormatInt(messageID, 10)},
		"forMe":      false,
	}
	return rb.buildBaseRequest(DELETE_MESSAGE, payload)
}

func (rb *RequestBuilder) GetVideoLink(videoID, chatID, messageID int) WebSocketPayload {
	payload := map[string]interface{}{
		"videoId":   videoID,
		"chatId":    chatID,
		"messageId": messageID,
	}
	return rb.buildBaseRequest(GET_VIDEO, payload)
}

func (rb *RequestBuilder) GetFileLink(fileID, chatID, messageID int) WebSocketPayload {
	payload := map[string]interface{}{
		"fileId":    fileID,
		"chatId":    chatID,
		"messageId": messageID,
	}
	return rb.buildBaseRequest(GET_FILE, payload)
}

func (rb *RequestBuilder) SubscribeToChat(chatID int, subscribe bool) WebSocketPayload {
	payload := map[string]interface{}{
		"chatId":    chatID,
		"subscribe": subscribe,
	}
	return rb.buildBaseRequest(SUBSCRIBE_CHAT, payload)
}

func (rb *RequestBuilder) GetChats(chatIDs []int) WebSocketPayload {
	payload := map[string]interface{}{
		"chatIds": chatIDs,
	}
	return rb.buildBaseRequest(GET_CHATS, payload)
}

func (rb *RequestBuilder) SendMessage(chatID int, text string, replyToMsgID *int) WebSocketPayload {
	return rb.SendMessageWithAttachments(chatID, text, nil, replyToMsgID)
}

func (rb *RequestBuilder) SendMessageWithAttachments(chatID int, text string, attaches []map[string]interface{}, replyToMsgID *int) WebSocketPayload {
	cid := -(time.Now().UnixNano() / 1e6)
	msgPayload := map[string]interface{}{
		"text":     text,
		"cid":      cid,
		"elements": []interface{}{},
		"attaches": attaches,
	}
	if replyToMsgID != nil {
		msgPayload["link"] = map[string]interface{}{
			"type":      "REPLY",
			"messageId": strconv.FormatInt(int64(*replyToMsgID), 10),
		}
	}
	payload := map[string]interface{}{
		"chatId":  chatID,
		"message": msgPayload,
		"notify":  true,
	}
	return rb.buildBaseRequest(SEND_MESSAGE, payload)
}

func (rb *RequestBuilder) SetReaction(chatID int, messageID int64, emoji string) WebSocketPayload {
	payload := map[string]interface{}{
		"chatId":    chatID,
		"messageId": strconv.FormatInt(messageID, 10),
		"reaction": map[string]interface{}{
			"reactionType": "EMOJI",
			"id":           emoji,
		},
	}
	return rb.buildBaseRequest(SET_REACTION, payload)
}
