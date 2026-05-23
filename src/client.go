package src

import (
	"fmt"
	"sync"
	"time"
)

type Client struct {
	config            *Config
	connection        *Connection
	me                *Me
	chats             []Chat
	connectionTime    time.Time
	disconnectionTime time.Time

	messageHandlers         []func(Message)
	deletedHandlers         []func(Message)
	editedHandlers          []func(Message)
	beforeReconnectHandlers []func(string)
	afterReconnectHandlers  []func()
	disconnectedHandlers    []func(string)
	startHandlers           []func()
	stopHandlers            []func()
	connectedHandlers       []func()
	fromWebSocketHandlers   []func(string)
	toWebSocketHandlers     []func(string)

	mu       sync.RWMutex
	running  bool
	stopping bool
}

func NewClient(config *Config) *Client {
	return &Client{
		config:                  config,
		connection:              NewConnection(config, nil),
		me:                      nil,
		chats:                   []Chat{},
		messageHandlers:         []func(Message){},
		deletedHandlers:         []func(Message){},
		editedHandlers:          []func(Message){},
		beforeReconnectHandlers: []func(string){},
		afterReconnectHandlers:  []func(){},
		disconnectedHandlers:    []func(string){},
		startHandlers:           []func(){},
		stopHandlers:            []func(){},
		connectedHandlers:       []func(){},
		fromWebSocketHandlers:   []func(string){},
		toWebSocketHandlers:     []func(string){},
	}
}

func (c *Client) OnMessage(handler func(Message)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messageHandlers = append(c.messageHandlers, handler)
}

func (c *Client) OnDeleted(handler func(Message)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletedHandlers = append(c.deletedHandlers, handler)
}

func (c *Client) OnEdited(handler func(Message)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.editedHandlers = append(c.editedHandlers, handler)
}

func (c *Client) OnBeforeReconnect(handler func(string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.beforeReconnectHandlers = append(c.beforeReconnectHandlers, handler)
}

func (c *Client) OnAfterReconnect(handler func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.afterReconnectHandlers = append(c.afterReconnectHandlers, handler)
}

func (c *Client) OnDisconnected(handler func(string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disconnectedHandlers = append(c.disconnectedHandlers, handler)
}

func (c *Client) OnStart(handler func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.startHandlers = append(c.startHandlers, handler)
}

func (c *Client) OnStop(handler func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopHandlers = append(c.stopHandlers, handler)
}

func (c *Client) OnConnected(handler func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connectedHandlers = append(c.connectedHandlers, handler)
}

func (c *Client) OnFromWebSocket(handler func(string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fromWebSocketHandlers = append(c.fromWebSocketHandlers, handler)
}

func (c *Client) OnToWebSocket(handler func(string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.toWebSocketHandlers = append(c.toWebSocketHandlers, handler)
}

func (c *Client) GetFromWebSocketHandlers() []func(string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.fromWebSocketHandlers
}

func (c *Client) GetToWebSocketHandlers() []func(string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.toWebSocketHandlers
}

func (c *Client) GetDisconnectedHandlers() []func(string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.disconnectedHandlers
}

func (c *Client) GetBeforeReconnectHandlers() []func(string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.beforeReconnectHandlers
}

func (c *Client) GetAfterReconnectHandlers() []func() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.afterReconnectHandlers
}

func (c *Client) SetMe(me Me) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.me = &me
}

func (c *Client) SetChats(chats []Chat) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.chats = chats
}

func (c *Client) SetConnectionTime(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connectionTime = t
}

func (c *Client) Connect() error {
	if err := c.connection.Connect(); err != nil {
		return err
	}

	response, err := c.connection.Authenticate(40)
	if err != nil {
		return err
	}

	rawPayload, ok := response["payload"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("authentication response missing payload")
	}
	profile := parseProfile(rawPayload)
	c.me = &profile
	if chatsData, ok := rawPayload["chats"].([]interface{}); ok {
		c.chats = parseChats(castToMapArray(chatsData))
	}
	c.connectionTime = time.Now()

	for _, handler := range c.connectedHandlers {
		handler()
	}

	c.connection.client = c

	return nil
}

func (c *Client) Start() error {
	if c.running {
		return nil
	}

	if err := c.Connect(); err != nil {
		return err
	}

	c.running = true
	c.stopping = false

	for _, handler := range c.startHandlers {
		handler()
	}

	go c.runEventLoop()

	return nil
}

func (c *Client) runEventLoop() {
	defer func() {
		if r := recover(); r != nil {
			Logf("PANIC in runEventLoop: %v", r)
		}
	}()
	wasConnected := true
	for c.running && !c.stopping {
		if !c.connection.IsConnected() {
			if !c.connection.IsReconnecting() && wasConnected {
				wasConnected = false
				go c.connection.HandleReconnect("Connection lost")
			}
			time.Sleep(1 * time.Second)
			continue
		}

		wasConnected = true

		payload, err := c.connection.Receive()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if opcode, ok := parseInt(payload["opcode"]); ok && opcode == int(ON_MESSAGE) {
			go c.handleMessage(payload)
		}
	}
}

func (c *Client) handleMessage(messageData map[string]interface{}) {
	defer func() {
		if r := recover(); r != nil {
			Logf("PANIC in handleMessage: %v", r)
		}
	}()
	payload, ok := messageData["payload"].(map[string]interface{})
	if !ok {
		return
	}

	chatID, _ := payload["chatId"].(float64)
	messagePayload, ok := payload["message"].(map[string]interface{})
	if !ok {
		return
	}

	msg := parseMessage(messagePayload, int(chatID))

	switch msg.Status {
	case MessageStatusNORMAL:
		for _, handler := range c.messageHandlers {
			handler(msg)
		}
	case MessageStatusEDITED:
		for _, handler := range c.editedHandlers {
			handler(msg)
		}
	case MessageStatusREMOVED:
		for _, handler := range c.deletedHandlers {
			handler(msg)
		}
	}
}

func (c *Client) Stop() error {
	if !c.running {
		return nil
	}

	c.stopping = true
	c.running = false

	for _, handler := range c.stopHandlers {
		handler()
	}

	time.Sleep(200 * time.Millisecond)

	return c.Close()
}

func (c *Client) Close() error {
	if c.connection != nil {
		c.connection.Close()
	}
	c.disconnectionTime = time.Now()
	return nil
}

func (c *Client) IsAlive() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connection.IsConnected() && c.me != nil && c.running
}

func (c *Client) GetMe() *Me {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.me
}

func (c *Client) GetChats() []Chat {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.chats
}

func (c *Client) GetChat(chatID int) *Chat {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, chat := range c.chats {
		if chat.ID == chatID {
			return &chat
		}
	}
	return nil
}

func (c *Client) GetContacts(contactIDs []int) ([]Contact, error) {
	if !c.connection.IsConnected() {
		return nil, NewConnectionError("WebSocket not connected")
	}
	reqData := c.connection.GetRequestBuilder().GetContacts(contactIDs)
	response, err := c.connection.sendAndReceive(reqData, 10*time.Second)
	if err != nil {
		return nil, err
	}
	opcodeFloat, _ := response["opcode"].(float64)
	cmdFloat, _ := response["cmd"].(float64)
	opcode := int(opcodeFloat)
	cmd := int(cmdFloat)
	if opcode != int(GET_CONTACTS) || cmd != 1 {
		return nil, NewInvalidResponseError("invalid contacts response")
	}
	payload, ok := response["payload"].(map[string]interface{})
	if !ok {
		return nil, NewInvalidResponseError("invalid contacts response payload")
	}
	contactsData, ok := payload["contacts"].([]interface{})
	if !ok {
		return nil, NewInvalidResponseError("missing contacts in response")
	}
	return parseContacts(castToMapArray(contactsData)), nil
}

func (c *Client) GetMessages(chatID int, backward, forward int, fromTime *int64) ([]Message, error) {
	if !c.connection.IsConnected() {
		return nil, NewConnectionError("WebSocket not connected")
	}
	if fromTime == nil {
		now := time.Now().UnixNano() / 1e6
		fromTime = &now
	}
	reqData := c.connection.GetRequestBuilder().GetChatMessages(chatID, *fromTime, forward, backward)
	response, err := c.connection.sendAndReceive(reqData, 10*time.Second)
	if err != nil {
		return nil, err
	}
	opcodeFloat, _ := response["opcode"].(float64)
	cmdFloat, _ := response["cmd"].(float64)
	opcode := int(opcodeFloat)
	cmd := int(cmdFloat)
	if opcode != int(GET_MESSAGES) || cmd != 1 {
		return nil, NewInvalidResponseError("invalid messages response")
	}
	payload, ok := response["payload"].(map[string]interface{})
	if !ok {
		return nil, NewInvalidResponseError("invalid messages response payload")
	}
	messagesData, ok := payload["messages"].([]interface{})
	if !ok {
		return nil, NewInvalidResponseError("missing messages in response")
	}
	messages := make([]Message, len(messagesData))
	for i, msgData := range messagesData {
		if msgMap, ok := msgData.(map[string]interface{}); ok {
			messages[i] = parseMessage(msgMap, chatID)
		}
	}

	return messages, nil
}

func (c *Client) SubscribeToChat(chatID int) error {
	if !c.connection.IsConnected() {
		return NewConnectionError("WebSocket not connected")
	}
	reqData := c.connection.GetRequestBuilder().SubscribeToChat(chatID, true)
	return c.connection.send(reqData)
}

func (c *Client) UnsubscribeFromChat(chatID int) error {
	if !c.connection.IsConnected() {
		return NewConnectionError("WebSocket not connected")
	}
	reqData := c.connection.GetRequestBuilder().SubscribeToChat(chatID, false)
	return c.connection.send(reqData)
}

func (c *Client) GetVideoLink(videoAttachment Attachment, message Message) (string, error) {
	if !c.connection.IsConnected() {
		return "", NewConnectionError("WebSocket not connected")
	}
	reqData := c.connection.GetRequestBuilder().GetVideoLink(videoAttachment.VideoID, message.ChatID, message.ID)
	response, err := c.connection.sendAndReceive(reqData, 10*time.Second)
	if err != nil {
		return "", err
	}
	opcodeFloat, _ := response["opcode"].(float64)
	cmdFloat, _ := response["cmd"].(float64)
	opcode := int(opcodeFloat)
	cmd := int(cmdFloat)
	if opcode != int(GET_VIDEO) || cmd != 1 {
		return "", NewInvalidResponseError("invalid video link response")
	}
	payload, ok := response["payload"].(map[string]interface{})
	if !ok {
		return "", NewInvalidResponseError("invalid video link response payload")
	}
	urls := []string{"MP4_1440", "MP4_1080", "MP4_720", "MP4_480", "MP4_360", "MP4_240", "MP4_144"}
	for _, key := range urls {
		if url, ok := payload[key].(string); ok && url != "" {
			return url, nil
		}
	}
	return "", NewInvalidResponseError("no URL in video link response")
}

func (c *Client) SendMessage(chatID int, text string, replyToMsgID *int) (int, error) {
	if !c.connection.IsConnected() {
		return 0, NewConnectionError("WebSocket not connected")
	}
	reqData := c.connection.GetRequestBuilder().SendMessage(chatID, text, replyToMsgID)
	response, err := c.connection.sendAndReceive(reqData, 10*time.Second)
	if err != nil {
		return 0, err
	}
	opcodeFloat, _ := response["opcode"].(float64)
	cmdFloat, _ := response["cmd"].(float64)
	opcode := int(opcodeFloat)
	cmd := int(cmdFloat)
	if opcode != int(SEND_MESSAGE) || cmd != 1 {
		return 0, NewInvalidResponseError("invalid send message response")
	}
	payload, ok := response["payload"].(map[string]interface{})
	if !ok {
		return 0, NewInvalidResponseError("invalid send message response payload")
	}
	msgData, ok := payload["message"].(map[string]interface{})
	if !ok {
		return 0, NewInvalidResponseError("no message in response")
	}
	msgID := ParseID(msgData["id"])
	if msgID == 0 {
		return 0, NewInvalidResponseError("no message ID in response")
	}
	return msgID, nil
}

func (c *Client) SetReaction(chatID int, messageID int64, emoji string) error {
	if !c.connection.IsConnected() {
		return NewConnectionError("WebSocket not connected")
	}
	reqData := c.connection.GetRequestBuilder().SetReaction(chatID, messageID, emoji)
	response, err := c.connection.sendAndReceive(reqData, 10*time.Second)
	if err != nil {
		return err
	}
	opcodeFloat, _ := response["opcode"].(float64)
	cmdFloat, _ := response["cmd"].(float64)
	opcode := int(opcodeFloat)
	cmd := int(cmdFloat)
	if opcode != int(SET_REACTION) || cmd != 1 {
		return NewInvalidResponseError("invalid set reaction response")
	}
	return nil
}

func (c *Client) GetConfig() *Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config
}

func (c *Client) GetFileLink(fileAttachment Attachment, message Message) (string, error) {
	if !c.connection.IsConnected() {
		return "", NewConnectionError("WebSocket not connected")
	}
	reqData := c.connection.GetRequestBuilder().GetFileLink(fileAttachment.FileID, message.ChatID, message.ID)
	response, err := c.connection.sendAndReceive(reqData, 10*time.Second)
	if err != nil {
		return "", err
	}
	opcodeFloat, _ := response["opcode"].(float64)
	cmdFloat, _ := response["cmd"].(float64)
	opcode := int(opcodeFloat)
	cmd := int(cmdFloat)
	if opcode != int(GET_FILE) || cmd != 1 {
		return "", NewInvalidResponseError("invalid file link response")
	}
	payload, ok := response["payload"].(map[string]interface{})
	if !ok {
		return "", NewInvalidResponseError("invalid file link response payload")
	}
	url, ok := payload["url"].(string)
	if !ok || url == "" {
		return "", NewInvalidResponseError("no URL in file link response")
	}
	return url, nil
}

func (c *Client) GetChatInfo(chatIDs []int) ([]Chat, error) {
	if !c.connection.IsConnected() {
		return nil, NewConnectionError("WebSocket not connected")
	}
	reqData := c.connection.GetRequestBuilder().GetChats(chatIDs)
	response, err := c.connection.sendAndReceive(reqData, 10*time.Second)
	if err != nil {
		return nil, err
	}
	opcodeFloat, _ := response["opcode"].(float64)
	cmdFloat, _ := response["cmd"].(float64)
	opcode := int(opcodeFloat)
	cmd := int(cmdFloat)
	if opcode != int(GET_CHATS) || cmd != 1 {
		return nil, NewInvalidResponseError("invalid chats response")
	}
	payload, ok := response["payload"].(map[string]interface{})
	if !ok {
		return nil, NewInvalidResponseError("invalid chats response payload")
	}
	chatsData, ok := payload["chats"].([]interface{})
	if !ok {
		return nil, NewInvalidResponseError("missing chats in response")
	}
	return parseChats(castToMapArray(chatsData)), nil
}
