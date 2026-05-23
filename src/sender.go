package src

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type TelegramSender struct {
	botToken           string
	routes             []ChatRoute
	defaultGroupChatID int64
	virtualRoutes      map[int]*ChatRoute
	mu                 sync.RWMutex
	db                 *Database
	maxRetries         int
	baseRetryDelay     time.Duration
	config             *Config
	httpClient         *http.Client
}

func NewTelegramSender(botToken string, routes []ChatRoute, cfg *Config, db *Database) *TelegramSender {
	if cfg == nil {
		cfg = DefaultConfig
	}

	tgProxy := GetTelegramProxy(cfg)
	httpClient, err := BuildHTTPClientWithProxy(tgProxy, 30*time.Second)
	if err != nil {
		Logf("Warning: failed to configure Telegram proxy, using direct connection: %v", err)
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	if tgProxy != nil {
		Logf("Telegram HTTP client configured with SOCKS5 proxy %s:%d", tgProxy.Host, tgProxy.Port)
	}

	sender := &TelegramSender{
		botToken:           botToken,
		routes:             routes,
		defaultGroupChatID: cfg.DefaultGroupChatID,
		virtualRoutes:      make(map[int]*ChatRoute),
		db:                 db,
		maxRetries:         cfg.MaxRetries,
		baseRetryDelay:     cfg.BaseRetryDelay,
		config:             cfg,
		httpClient:         httpClient,
	}

	sender.loadVirtualRoutesFromDB()

	return sender
}

type RateLimitError struct {
	RetryAfter int
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited, retry after %d seconds", e.RetryAfter)
}

func isRateLimitError(body string) (*RateLimitError, bool) {
	var resp struct {
		OK          bool   `json:"ok"`
		ErrorCode   int    `json:"error_code"`
		Description string `json:"description"`
		Parameters  struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return nil, false
	}
	if resp.ErrorCode == 429 {
		retryAfter := resp.Parameters.RetryAfter
		return &RateLimitError{RetryAfter: retryAfter + 1}, true
	}
	return nil, false
}

func (s *TelegramSender) FindRoute(maxChatID int) *ChatRoute {
	for i := range s.routes {
		if s.routes[i].MaxChatID == maxChatID {
			return &s.routes[i]
		}
	}

	if s.defaultGroupChatID == 0 {
		return nil
	}

	s.mu.RLock()
	vr, ok := s.virtualRoutes[maxChatID]
	s.mu.RUnlock()
	if ok {
		return vr
	}

	if s.db != nil {
		topicID, err := s.db.GetCachedTopicID(maxChatID)
		if err == nil && topicID > 0 {
			s.mu.Lock()
			vr = &ChatRoute{
				MaxChatID:       maxChatID,
				TelegramChatID:  s.defaultGroupChatID,
				TelegramTopicID: topicID,
			}
			s.virtualRoutes[maxChatID] = vr
			s.mu.Unlock()
			return vr
		}
	}

	vr = &ChatRoute{
		MaxChatID:       maxChatID,
		TelegramChatID:  s.defaultGroupChatID,
		TelegramTopicID: 0,
	}
	s.mu.Lock()
	s.virtualRoutes[maxChatID] = vr
	s.mu.Unlock()
	return vr
}

func (s *TelegramSender) SendMessage(text string, maxChatID int, replyToMessageID *int) (int, error) {
	route := s.FindRoute(maxChatID)
	if route == nil {
		return 0, fmt.Errorf("no route found for MAX chat ID %d", maxChatID)
	}

	processedText, shouldSend := CheckAndHandleMessageLength(text, false, s.config.TruncateLongMessages)
	if !shouldSend {
		return 0, fmt.Errorf("message too long and truncation disabled")
	}
	text = processedText

	var lastErr error

	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			retryDelay := s.baseRetryDelay * time.Duration(1<<uint(attempt-1))
			Logf("Retrying SendMessage (attempt %d/%d) after %v: %v", attempt+1, s.maxRetries, retryDelay, lastErr)
			time.Sleep(retryDelay)
		}

		startTime := time.Now()

		url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", s.botToken)
		payload := map[string]interface{}{
			"chat_id":    route.TelegramChatID,
			"text":       text,
			"parse_mode": "HTML",
		}
		if route.TelegramTopicID > 0 {
			payload["message_thread_id"] = route.TelegramTopicID
		}
		if replyToMessageID != nil {
			payload["reply_to_message_id"] = *replyToMessageID
		}

		jsonData, err := json.Marshal(payload)
		if err != nil {
			return 0, err
		}

		resp, err := s.httpClient.Post(url, "application/json", bytes.NewReader(jsonData))
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusTooManyRequests {
			if rateLimitErr, ok := isRateLimitError(string(body)); ok {
				Logf("Rate limited by Telegram, waiting %d seconds", rateLimitErr.RetryAfter)
				time.Sleep(time.Duration(rateLimitErr.RetryAfter) * time.Second)
				lastErr = rateLimitErr
				continue
			}
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		var result struct {
			OK     bool `json:"ok"`
			Result struct {
				MessageID int `json:"message_id"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = err
			continue
		}

		if !result.OK {
			lastErr = fmt.Errorf("telegram API returned not OK")
			continue
		}

		Logf("Message sent in %v", time.Since(startTime))
		return result.Result.MessageID, nil
	}

	if lastErr != nil && route.TelegramTopicID > 0 && strings.Contains(lastErr.Error(), "message thread not found") {
		Logf("Topic %d not found for chat %d, invalidating cache and retrying without topic", route.TelegramTopicID, maxChatID)
		s.InvalidateTopicCache(maxChatID)
		route.TelegramTopicID = 0

		return s.sendMessageWithoutTopic(text, route, replyToMessageID)
	}

	return 0, fmt.Errorf("failed to send message after %d retries: %w", s.maxRetries, lastErr)
}

func (s *TelegramSender) sendMessageWithoutTopic(text string, route *ChatRoute, replyToMessageID *int) (int, error) {
	var lastErr error

	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			retryDelay := s.baseRetryDelay * time.Duration(1<<uint(attempt-1))
			Logf("Retrying sendMessageWithoutTopic (attempt %d/%d) after %v: %v", attempt+1, s.maxRetries, retryDelay, lastErr)
			time.Sleep(retryDelay)
		}

		url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", s.botToken)
		payload := map[string]interface{}{
			"chat_id":    route.TelegramChatID,
			"text":       text,
			"parse_mode": "HTML",
		}
		if replyToMessageID != nil {
			payload["reply_to_message_id"] = *replyToMessageID
		}

		jsonData, err := json.Marshal(payload)
		if err != nil {
			return 0, err
		}

		resp, err := s.httpClient.Post(url, "application/json", bytes.NewReader(jsonData))
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusTooManyRequests {
			if rateLimitErr, ok := isRateLimitError(string(body)); ok {
				Logf("Rate limited by Telegram, waiting %d seconds", rateLimitErr.RetryAfter)
				time.Sleep(time.Duration(rateLimitErr.RetryAfter) * time.Second)
				lastErr = rateLimitErr
				continue
			}
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		var result struct {
			OK     bool `json:"ok"`
			Result struct {
				MessageID int `json:"message_id"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = err
			continue
		}

		if !result.OK {
			lastErr = fmt.Errorf("telegram API returned not OK")
			continue
		}

		Logf("Message sent (fallback, no topic) in chat %d", route.TelegramChatID)
		return result.Result.MessageID, nil
	}

	return 0, fmt.Errorf("failed to send message without topic after %d retries: %w", s.maxRetries, lastErr)
}

func (s *TelegramSender) SendMediaGroup(files []string, caption string, maxChatID int, replyToMessageID *int) ([]int, error) {
	route := s.FindRoute(maxChatID)
	if route == nil {
		return nil, fmt.Errorf("no route found for MAX chat ID %d", maxChatID)
	}

	if caption != "" {
		processedCaption, shouldSend := CheckAndHandleMessageLength(caption, true, s.config.TruncateLongMessages)
		if !shouldSend {
			return nil, fmt.Errorf("caption too long and truncation disabled")
		}
		caption = processedCaption
	}

	var lastErr error

	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			retryDelay := s.baseRetryDelay * time.Duration(1<<uint(attempt-1))
			Logf("Retrying SendMediaGroup (attempt %d/%d) after %v: %v", attempt+1, s.maxRetries, retryDelay, lastErr)
			time.Sleep(retryDelay)
		}

		startTime := time.Now()

		url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMediaGroup", s.botToken)
		var buf bytes.Buffer
		writer := multipart.NewWriter(&buf)

		writer.WriteField("chat_id", fmt.Sprintf("%d", route.TelegramChatID))
		if route.TelegramTopicID > 0 {
			writer.WriteField("message_thread_id", fmt.Sprintf("%d", route.TelegramTopicID))
		}
		if caption != "" {
			writer.WriteField("caption", caption)
			writer.WriteField("parse_mode", "HTML")
		}
		if replyToMessageID != nil {
			writer.WriteField("reply_to_message_id", fmt.Sprintf("%d", *replyToMessageID))
		}

		media := []map[string]string{}
		for i, file := range files {
			media = append(media, map[string]string{
				"type":  s.getMediaType(file),
				"media": fmt.Sprintf("attach://file%d", i),
			})
		}

		if len(media) > 0 {
			media[0]["caption"] = caption
			media[0]["parse_mode"] = "HTML"
		}

		mediaJSON, _ := json.Marshal(media)
		writer.WriteField("media", string(mediaJSON))

		for i, file := range files {
			part, err := writer.CreateFormFile(fmt.Sprintf("file%d", i), filepath.Base(file))
			if err != nil {
				return nil, err
			}
			f, err := os.Open(file)
			if err != nil {
				return nil, err
			}
			defer f.Close()
			io.Copy(part, f)
		}

		writer.Close()

		resp, err := s.httpClient.Post(url, writer.FormDataContentType(), &buf)
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusTooManyRequests {
			if rateLimitErr, ok := isRateLimitError(string(body)); ok {
				Logf("Rate limited by Telegram, waiting %d seconds", rateLimitErr.RetryAfter)
				time.Sleep(time.Duration(rateLimitErr.RetryAfter) * time.Second)
				lastErr = rateLimitErr
				continue
			}
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		var result struct {
			OK     bool `json:"ok"`
			Result []struct {
				MessageID int `json:"message_id"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = err
			continue
		}

		if !result.OK {
			lastErr = fmt.Errorf("telegram API returned not OK")
			continue
		}

		messageIDs := make([]int, len(result.Result))
		for i, r := range result.Result {
			messageIDs[i] = r.MessageID
		}

		Logf("Media group sent in %v", time.Since(startTime))
		return messageIDs, nil
	}

	if lastErr != nil && route.TelegramTopicID > 0 && strings.Contains(lastErr.Error(), "message thread not found") {
		Logf("Topic %d not found for chat %d, invalidating cache", route.TelegramTopicID, maxChatID)
		s.InvalidateTopicCache(maxChatID)
	}

	return nil, fmt.Errorf("failed to send media group after %d attempts: %w", s.maxRetries, lastErr)
}

func (s *TelegramSender) SendAudio(filePath string, caption string, maxChatID int, replyToMessageID *int) (int, error) {
	route := s.FindRoute(maxChatID)
	if route == nil {
		return 0, fmt.Errorf("no route found for MAX chat ID %d", maxChatID)
	}

	if caption != "" {
		processedCaption, shouldSend := CheckAndHandleMessageLength(caption, true, s.config.TruncateLongMessages)
		if !shouldSend {
			return 0, fmt.Errorf("caption too long and truncation disabled")
		}
		caption = processedCaption
	}

	var lastErr error

	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			retryDelay := s.baseRetryDelay * time.Duration(1<<uint(attempt-1))
			Logf("Retrying SendAudio (attempt %d/%d) after %v: %v", attempt+1, s.maxRetries, retryDelay, lastErr)
			time.Sleep(retryDelay)
		}

		startTime := time.Now()

		endpoint := "sendAudio"
		fieldName := "audio"

		url := fmt.Sprintf("https://api.telegram.org/bot%s/%s", s.botToken, endpoint)
		var buf bytes.Buffer
		writer := multipart.NewWriter(&buf)

		writer.WriteField("chat_id", fmt.Sprintf("%d", route.TelegramChatID))
		if route.TelegramTopicID > 0 {
			writer.WriteField("message_thread_id", fmt.Sprintf("%d", route.TelegramTopicID))
		}
		if caption != "" {
			writer.WriteField("caption", caption)
			writer.WriteField("parse_mode", "HTML")
		}
		if replyToMessageID != nil {
			writer.WriteField("reply_to_message_id", fmt.Sprintf("%d", *replyToMessageID))
		}

		part, err := writer.CreateFormFile(fieldName, filepath.Base(filePath))
		if err != nil {
			return 0, err
		}
		f, err := os.Open(filePath)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		io.Copy(part, f)
		writer.Close()

		resp, err := s.httpClient.Post(url, writer.FormDataContentType(), &buf)
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusTooManyRequests {
			if rateLimitErr, ok := isRateLimitError(string(body)); ok {
				Logf("Rate limited by Telegram, waiting %d seconds", rateLimitErr.RetryAfter)
				time.Sleep(time.Duration(rateLimitErr.RetryAfter) * time.Second)
				lastErr = rateLimitErr
				continue
			}
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		var result struct {
			OK     bool `json:"ok"`
			Result struct {
				MessageID int `json:"message_id"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = err
			continue
		}

		if !result.OK {
			lastErr = fmt.Errorf("telegram API returned not OK")
			continue
		}

		Logf("Audio sent in %v", time.Since(startTime))
		return result.Result.MessageID, nil
	}

	if lastErr != nil && route.TelegramTopicID > 0 && strings.Contains(lastErr.Error(), "message thread not found") {
		Logf("Topic %d not found for chat %d, invalidating cache", route.TelegramTopicID, maxChatID)
		s.InvalidateTopicCache(maxChatID)
	}

	return 0, fmt.Errorf("failed to send audio after %d retries: %w", s.maxRetries, lastErr)
}

func (s *TelegramSender) SendVoice(filePath string, maxChatID int, replyToMessageID *int, duration int) (int, error) {
	route := s.FindRoute(maxChatID)
	if route == nil {
		return 0, fmt.Errorf("no route found for MAX chat ID %d", maxChatID)
	}

	var lastErr error

	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			retryDelay := s.baseRetryDelay * time.Duration(1<<uint(attempt-1))
			Logf("Retrying SendVoice (attempt %d/%d) after %v: %v", attempt+1, s.maxRetries, retryDelay, lastErr)
			time.Sleep(retryDelay)
		}

		startTime := time.Now()

		url := fmt.Sprintf("https://api.telegram.org/bot%s/sendVoice", s.botToken)
		var buf bytes.Buffer
		writer := multipart.NewWriter(&buf)

		writer.WriteField("chat_id", fmt.Sprintf("%d", route.TelegramChatID))
		if route.TelegramTopicID > 0 {
			writer.WriteField("message_thread_id", fmt.Sprintf("%d", route.TelegramTopicID))
		}
		if replyToMessageID != nil {
			writer.WriteField("reply_to_message_id", fmt.Sprintf("%d", *replyToMessageID))
		}
		if duration > 0 {
			writer.WriteField("duration", fmt.Sprintf("%d", duration/1000))
		}

		part, err := writer.CreateFormFile("voice", filepath.Base(filePath))
		if err != nil {
			return 0, err
		}
		f, err := os.Open(filePath)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		io.Copy(part, f)
		writer.Close()

		resp, err := s.httpClient.Post(url, writer.FormDataContentType(), &buf)
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusTooManyRequests {
			if rateLimitErr, ok := isRateLimitError(string(body)); ok {
				Logf("Rate limited by Telegram, waiting %d seconds", rateLimitErr.RetryAfter)
				time.Sleep(time.Duration(rateLimitErr.RetryAfter) * time.Second)
				lastErr = rateLimitErr
				continue
			}
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		var result struct {
			OK     bool `json:"ok"`
			Result struct {
				MessageID int `json:"message_id"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = err
			continue
		}

		if !result.OK {
			lastErr = fmt.Errorf("telegram API returned not OK")
			continue
		}

		Logf("Voice sent in %v", time.Since(startTime))
		return result.Result.MessageID, nil
	}

	return 0, fmt.Errorf("failed to send voice after %d retries: %w", s.maxRetries, lastErr)
}

func (s *TelegramSender) getMediaType(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".gif", ".webp":
		return "photo"
	case ".mp4", ".avi", ".mov", ".mkv":
		return "video"
	default:
		return "document"
	}
}

func (s *TelegramSender) EditMessageText(messageID int, text string, maxChatID int) error {
	route := s.FindRoute(maxChatID)
	if route == nil {
		return fmt.Errorf("no route found for MAX chat ID %d", maxChatID)
	}

	processedText, shouldSend := CheckAndHandleMessageLength(text, false, s.config.TruncateLongMessages)
	if !shouldSend {
		return fmt.Errorf("message too long and truncation disabled")
	}
	text = processedText

	var lastErr error

	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			retryDelay := s.baseRetryDelay * time.Duration(1<<uint(attempt-1))
			Logf("Retrying EditMessageText (attempt %d/%d) after %v: %v", attempt+1, s.maxRetries, retryDelay, lastErr)
			time.Sleep(retryDelay)
		}

		url := fmt.Sprintf("https://api.telegram.org/bot%s/editMessageText", s.botToken)

		payload := map[string]interface{}{
			"chat_id":    route.TelegramChatID,
			"message_id": messageID,
			"text":       text,
			"parse_mode": "HTML",
		}

		jsonData, err := json.Marshal(payload)
		if err != nil {
			return err
		}

		resp, err := s.httpClient.Post(url, "application/json", bytes.NewReader(jsonData))
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusTooManyRequests {
			if rateLimitErr, ok := isRateLimitError(string(body)); ok {
				Logf("Rate limited by Telegram, waiting %d seconds", rateLimitErr.RetryAfter)
				time.Sleep(time.Duration(rateLimitErr.RetryAfter) * time.Second)
				lastErr = rateLimitErr
				continue
			}
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		var result struct {
			OK bool `json:"ok"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = err
			continue
		}

		if !result.OK {
			lastErr = fmt.Errorf("telegram API returned not OK")
			continue
		}

		return nil
	}

	return fmt.Errorf("failed to edit message after %d attempts: %w", s.maxRetries, lastErr)
}

func (s *TelegramSender) EditMessageCaption(messageID int, caption string, maxChatID int) error {
	route := s.FindRoute(maxChatID)
	if route == nil {
		return fmt.Errorf("no route found for MAX chat ID %d", maxChatID)
	}

	if caption != "" {
		processedCaption, shouldSend := CheckAndHandleMessageLength(caption, true, s.config.TruncateLongMessages)
		if !shouldSend {
			return fmt.Errorf("caption too long and truncation disabled")
		}
		caption = processedCaption
	}

	var lastErr error

	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			retryDelay := s.baseRetryDelay * time.Duration(1<<uint(attempt-1))
			Logf("Retrying EditMessageCaption (attempt %d/%d) after %v: %v", attempt+1, s.maxRetries, retryDelay, lastErr)
			time.Sleep(retryDelay)
		}

		url := fmt.Sprintf("https://api.telegram.org/bot%s/editMessageCaption", s.botToken)

		payload := map[string]interface{}{
			"chat_id":    route.TelegramChatID,
			"message_id": messageID,
			"caption":    caption,
			"parse_mode": "HTML",
		}

		jsonData, err := json.Marshal(payload)
		if err != nil {
			return err
		}

		resp, err := s.httpClient.Post(url, "application/json", bytes.NewReader(jsonData))
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusTooManyRequests {
			if rateLimitErr, ok := isRateLimitError(string(body)); ok {
				Logf("Rate limited by Telegram, waiting %d seconds", rateLimitErr.RetryAfter)
				time.Sleep(time.Duration(rateLimitErr.RetryAfter) * time.Second)
				lastErr = rateLimitErr
				continue
			}
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		var result struct {
			OK bool `json:"ok"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = err
			continue
		}

		if !result.OK {
			lastErr = fmt.Errorf("telegram API returned not OK")
			continue
		}

		return nil
	}

	return fmt.Errorf("failed to edit message caption after %d attempts: %w", s.maxRetries, lastErr)
}

func (s *TelegramSender) DeleteMessage(messageID int, maxChatID int) error {
	route := s.FindRoute(maxChatID)
	if route == nil {
		return fmt.Errorf("no route found for MAX chat ID %d", maxChatID)
	}

	var lastErr error

	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			retryDelay := s.baseRetryDelay * time.Duration(1<<uint(attempt-1))
			Logf("Retrying DeleteMessage (attempt %d/%d) after %v: %v", attempt+1, s.maxRetries, retryDelay, lastErr)
			time.Sleep(retryDelay)
		}

		url := fmt.Sprintf("https://api.telegram.org/bot%s/deleteMessage", s.botToken)

		payload := map[string]interface{}{
			"chat_id":    route.TelegramChatID,
			"message_id": messageID,
		}

		jsonData, err := json.Marshal(payload)
		if err != nil {
			return err
		}

		resp, err := s.httpClient.Post(url, "application/json", bytes.NewReader(jsonData))
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusTooManyRequests {
			if rateLimitErr, ok := isRateLimitError(string(body)); ok {
				Logf("Rate limited by Telegram, waiting %d seconds", rateLimitErr.RetryAfter)
				time.Sleep(time.Duration(rateLimitErr.RetryAfter) * time.Second)
				lastErr = rateLimitErr
				continue
			}
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("telegram API error: %s", string(body))
			continue
		}

		var result struct {
			OK bool `json:"ok"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = err
			continue
		}

		if !result.OK {
			lastErr = fmt.Errorf("telegram API returned not OK")
			continue
		}

		return nil
	}

	return fmt.Errorf("failed to delete message after %d attempts: %w", s.maxRetries, lastErr)
}

func (s *TelegramSender) CreateForumTopic(name string) (int, error) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/createForumTopic", s.botToken)

	if len(name) > 128 {
		name = name[:128]
	}

	payload := map[string]interface{}{
		"chat_id": s.defaultGroupChatID,
		"name":    name,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}

	var lastErr error
	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if attempt > 0 {
			retryDelay := s.baseRetryDelay * time.Duration(1<<uint(attempt-1))
			Logf("Retrying CreateForumTopic (attempt %d/%d) after %v: %v", attempt+1, s.maxRetries, retryDelay, lastErr)
			time.Sleep(retryDelay)
		}

		resp, err := s.httpClient.Post(url, "application/json", bytes.NewReader(jsonData))
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusTooManyRequests {
			if rateLimitErr, ok := isRateLimitError(string(body)); ok {
				Logf("Rate limited while creating forum topic, waiting %d seconds", rateLimitErr.RetryAfter)
				time.Sleep(time.Duration(rateLimitErr.RetryAfter) * time.Second)
				lastErr = rateLimitErr
				continue
			}
			lastErr = fmt.Errorf("telegram API error creating forum topic: %s", string(body))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("telegram API error creating forum topic: %s", string(body))
			continue
		}

		var result struct {
			OK     bool `json:"ok"`
			Result struct {
				MessageThreadID int `json:"message_thread_id"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = err
			continue
		}

		if !result.OK {
			lastErr = fmt.Errorf("telegram API returned not OK for createForumTopic")
			continue
		}

		topicID := result.Result.MessageThreadID
		Logf("Created forum topic '%s' with ID %d in chat %d", name, topicID, s.defaultGroupChatID)
		return topicID, nil
	}

	return 0, fmt.Errorf("failed to create forum topic after %d attempts: %w", s.maxRetries, lastErr)
}

func (s *TelegramSender) EnsureTopicExists(maxChatID int, chatTitle string) (int, error) {
	route := s.FindRoute(maxChatID)
	if route == nil {
		return 0, fmt.Errorf("no route for chat %d", maxChatID)
	}

	for i := range s.routes {
		if s.routes[i].MaxChatID == maxChatID {
			return route.TelegramTopicID, nil
		}
	}

	if route.TelegramTopicID > 0 {
		return route.TelegramTopicID, nil
	}

	topicID, err := s.CreateForumTopic(chatTitle)
	if err != nil {
		Logf("Failed to create forum topic for chat %d: %v", maxChatID, err)
		return 0, err
	}

	s.mu.Lock()
	if vr, ok := s.virtualRoutes[maxChatID]; ok {
		vr.TelegramTopicID = topicID
	}
	s.mu.Unlock()

	if s.db != nil {
		if err := s.db.CacheTopicID(maxChatID, topicID); err != nil {
			Logf("Failed to cache topic ID for chat %d: %v", maxChatID, err)
		}
	}

	return topicID, nil
}

func (s *TelegramSender) loadVirtualRoutesFromDB() {
	if s.db == nil || s.defaultGroupChatID == 0 {
		return
	}

	chatIDs, err := s.db.GetAllCachedChatIDs()
	if err != nil {
		Logf("Failed to load cached topic IDs: %v", err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, chatID := range chatIDs {
		topicID, err := s.db.GetCachedTopicID(chatID)
		if err == nil && topicID > 0 {
			s.virtualRoutes[chatID] = &ChatRoute{
				MaxChatID:       chatID,
				TelegramChatID:  s.defaultGroupChatID,
				TelegramTopicID: topicID,
			}
		}
	}

	if len(chatIDs) > 0 {
		Logf("Loaded %d cached topic IDs from database", len(chatIDs))
	}
}

func (s *TelegramSender) InvalidateTopicCache(maxChatID int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.virtualRoutes, maxChatID)
	if s.db != nil {
		s.db.CacheTopicID(maxChatID, 0)
	}
	Logf("Invalidated topic cache for chat %d", maxChatID)
}

func (s *TelegramSender) GetAllMaxChatIDs() []int {
	seen := make(map[int]struct{})

	for _, r := range s.routes {
		seen[r.MaxChatID] = struct{}{}
	}

	s.mu.RLock()
	for id := range s.virtualRoutes {
		seen[id] = struct{}{}
	}
	s.mu.RUnlock()

	result := make([]int, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	return result
}

func (s *TelegramSender) SendDebugMessage(text string, userID int64) error {
	if userID == 0 {
		return nil
	}
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", s.botToken)

	payload := map[string]interface{}{
		"chat_id": userID,
		"text":    text,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := s.httpClient.Post(url, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_ = resp
	return nil
}

type TelegramUpdate struct {
	UpdateID        int                  `json:"update_id"`
	Message         *TelegramMessage     `json:"message,omitempty"`
	MessageReaction *MessageReactionData `json:"message_reaction,omitempty"`
}

type TelegramMessage struct {
	MessageID       int              `json:"message_id"`
	From            *TelegramUser    `json:"from,omitempty"`
	Chat            TelegramChat     `json:"chat"`
	Text            string           `json:"text,omitempty"`
	Date            int64            `json:"date"`
	IsTopicMessage  bool             `json:"is_topic_message,omitempty"`
	MessageThreadID int              `json:"message_thread_id,omitempty"`
	ReplyTo         *TelegramMessage `json:"reply_to_message,omitempty"`
}

type TelegramUser struct {
	ID       int64  `json:"id"`
	Username string `json:"username,omitempty"`
}

type TelegramChat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

type MessageReactionData struct {
	Chat            TelegramChat   `json:"chat"`
	MessageID       int            `json:"message_id"`
	Date            int64          `json:"date"`
	MessageThreadID int            `json:"message_thread_id,omitempty"`
	NewReaction     []ReactionItem `json:"new_reaction"`
}

type ReactionItem struct {
	Type  string `json:"type"`
	Emoji string `json:"emoji"`
}

func (s *TelegramSender) FindReverseRoute(telegramChatID int64, telegramTopicID int) *ChatRoute {
	for i := range s.routes {
		if s.routes[i].TelegramChatID == telegramChatID && s.routes[i].TelegramTopicID == telegramTopicID {
			return &s.routes[i]
		}
	}
	for i := range s.routes {
		if s.routes[i].TelegramChatID == telegramChatID && s.routes[i].TelegramTopicID == 0 && telegramTopicID == 0 {
			return &s.routes[i]
		}
	}
	if s.defaultGroupChatID != 0 && telegramChatID == s.defaultGroupChatID && telegramTopicID > 0 {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, vr := range s.virtualRoutes {
			if vr.TelegramTopicID == telegramTopicID {
				return vr
			}
		}
	}
	return nil
}

func (s *TelegramSender) StartPolling(client *Client, db *Database) {
	tgProxy := GetTelegramProxy(s.config)
	pollClient, err := BuildHTTPClientWithProxy(tgProxy, 90*time.Second)
	if err != nil {
		Logf("Warning: failed to configure polling proxy, using direct: %v", err)
		pollClient = &http.Client{Timeout: 90 * time.Second}
	}
	offset := 0
	Logf("Starting Telegram polling for reverse messages...")

	for {
		updates, err := pollUpdates(pollClient, s.botToken, offset, 25)
		if err != nil {
			Logf("Telegram polling error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, update := range updates {
			if update.Message != nil {
				s.handleTelegramMessage(client, db, *update.Message)
			}
			if update.MessageReaction != nil {
				s.handleTelegramReaction(client, db, *update.MessageReaction)
			}
			offset = update.UpdateID + 1
		}

		if len(updates) == 0 {
			time.Sleep(1 * time.Second)
		}
	}
}

func pollUpdates(httpClient *http.Client, botToken string, offset int, timeout int) ([]TelegramUpdate, error) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/getUpdates", botToken)
	payload := map[string]interface{}{
		"offset":          offset,
		"timeout":         timeout,
		"allowed_updates": []string{"message", "message_reaction"},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("telegram API error: %s", string(body))
	}

	var result struct {
		OK     bool             `json:"ok"`
		Result []TelegramUpdate `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if !result.OK {
		return nil, fmt.Errorf("telegram API returned not OK")
	}

	return result.Result, nil
}

func (s *TelegramSender) handleTelegramMessage(client *Client, db *Database, msg TelegramMessage) {
	tgChatID := msg.Chat.ID
	tgTopicID := msg.MessageThreadID

	if msg.From != nil && msg.From.ID == 777000 {
		return
	}

	if msg.Text == "" {
		return
	}

	existing, _ := db.GetMessageByTgID(int64(msg.MessageID))
	if existing != nil {
		return
	}

	route := s.FindReverseRoute(tgChatID, tgTopicID)
	if route == nil {
		Logf("No reverse route found for Telegram chat %d topic %d", tgChatID, tgTopicID)
		return
	}

	text := msg.Text

	var replyToMaxID *int
	if msg.ReplyTo != nil {
		parentTgID := int64(msg.ReplyTo.MessageID)
		parentRecord, err := db.GetMessageByTgID(parentTgID)
		if err == nil && parentRecord != nil {
			maxID := int(parentRecord["max_message_id"].(int64))
			replyToMaxID = &maxID
		}
	}

	logText := text
	if len(logText) > 100 {
		logText = logText[:100] + "..."
	}
	if replyToMaxID != nil {
		Logf("Forwarding reply to message %d from Telegram chat %d to MAX chat %d: %s", *replyToMaxID, tgChatID, route.MaxChatID, logText)
	} else {
		Logf("Forwarding message from Telegram chat %d to MAX chat %d: %s", tgChatID, route.MaxChatID, logText)
	}

	maxMsgID, err := client.SendMessage(route.MaxChatID, text, replyToMaxID)
	if err != nil {
		Logf("Failed to send message to MAX chat %d: %v", route.MaxChatID, err)
		return
	}

	ts := time.Now().UnixMilli()
	db.AddMessage(int64(maxMsgID), int64(msg.MessageID), 0, ts, 0, route.MaxChatID)
	Logf("Message forwarded to MAX chat %d, MAX msg ID: %d (TG msg ID: %d)", route.MaxChatID, maxMsgID, msg.MessageID)
}

func (s *TelegramSender) handleTelegramReaction(client *Client, db *Database, reac MessageReactionData) {
	Logf("Got reaction update: chat=%d msg=%d topic=%d reactions=%v", reac.Chat.ID, reac.MessageID, reac.MessageThreadID, reac.NewReaction)

	var thumbsUp string
	for _, r := range reac.NewReaction {
		if r.Type == "emoji" && r.Emoji == "\U0001f44d" {
			thumbsUp = r.Emoji
			break
		}
	}
	if thumbsUp == "" {
		Logf("Reaction is not thumbs up, skipping")
		return
	}

	record, err := db.GetMessageByTgID(int64(reac.MessageID))
	if err != nil || record == nil {
		Logf("Message %d not found in database (MAX->TG mapping)", reac.MessageID)
		return
	}

	maxMsgID := record["max_message_id"].(int64)
	maxChatID := int(record["max_chat_id"].(int64))
	if maxChatID == 0 {
		Logf("No max_chat_id for tg message %d, cannot forward reaction", reac.MessageID)
		return
	}

	route := s.FindRoute(maxChatID)
	if route == nil {
		Logf("No route for MAX chat %d", maxChatID)
		return
	}

	Logf("Setting reaction %s on MAX message %d (chat %d)", thumbsUp, maxMsgID, maxChatID)

	if err := client.SetReaction(maxChatID, maxMsgID, thumbsUp); err != nil {
		Logf("Failed to set reaction on MAX message %d: %v", maxMsgID, err)
		return
	}
	Logf("Set reaction %s on MAX message %d (TG msg %d)", thumbsUp, maxMsgID, reac.MessageID)
}
