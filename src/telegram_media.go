package src

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const telegramAlbumWindow = time.Second

type telegramAlbum struct {
	messages []TelegramMessage
	timer    *time.Timer
}

type telegramRemoteFile struct {
	FilePath string `json:"file_path"`
}

type telegramMediaRef struct {
	fileID   string
	name     string
	mimeType string
	kind     MaxMediaKind
	duration int
}

func (s *TelegramSender) enqueueAlbum(client *Client, db *Database, message TelegramMessage) {
	key := fmt.Sprintf("%d:%d:%s", message.Chat.ID, message.MessageThreadID, message.MediaGroupID)
	s.albumMu.Lock()
	batch := s.albums[key]
	if batch == nil {
		batch = &telegramAlbum{}
		s.albums[key] = batch
	}
	batch.messages = append(batch.messages, message)
	if batch.timer != nil {
		batch.timer.Stop()
	}
	batch.timer = time.AfterFunc(telegramAlbumWindow, func() {
		s.albumMu.Lock()
		current := s.albums[key]
		delete(s.albums, key)
		s.albumMu.Unlock()
		if current != nil {
			s.forwardTelegramMessages(client, db, current.messages)
		}
	})
	s.albumMu.Unlock()
}

func mediaReference(message TelegramMessage) (telegramMediaRef, bool) {
	if len(message.Photo) > 0 {
		photo := message.Photo[len(message.Photo)-1]
		return telegramMediaRef{fileID: photo.FileID, name: fmt.Sprintf("photo-%d.jpg", message.MessageID), mimeType: "image/jpeg", kind: MaxMediaPhoto}, photo.FileID != ""
	}
	choices := []struct {
		ref      *TelegramFileRef
		kind     MaxMediaKind
		fallback string
	}{
		{message.Video, MaxMediaVideo, fmt.Sprintf("video-%d.mp4", message.MessageID)},
		{message.Document, MaxMediaFile, fmt.Sprintf("document-%d", message.MessageID)},
		{message.Audio, MaxMediaFile, fmt.Sprintf("audio-%d", message.MessageID)},
		{message.Voice, MaxMediaVoice, fmt.Sprintf("voice-%d.ogg", message.MessageID)},
		{message.Animation, MaxMediaFile, fmt.Sprintf("animation-%d", message.MessageID)},
	}
	for _, choice := range choices {
		if choice.ref == nil || choice.ref.FileID == "" {
			continue
		}
		name := choice.ref.FileName
		if name == "" {
			name = choice.fallback
		}
		return telegramMediaRef{fileID: choice.ref.FileID, name: name, mimeType: choice.ref.MimeType, kind: choice.kind, duration: choice.ref.Duration}, true
	}
	return telegramMediaRef{}, false
}

func (s *TelegramSender) getTelegramFile(fileID string) (string, error) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/getFile", s.botToken)
	payload, _ := json.Marshal(map[string]string{"file_id": fileID})
	resp, err := s.httpClient.Post(url, "application/json", strings.NewReader(string(payload)))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Telegram getFile: %s", string(body))
	}
	var result struct {
		OK     bool               `json:"ok"`
		Result telegramRemoteFile `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}
	if !result.OK || result.Result.FilePath == "" {
		return "", fmt.Errorf("Telegram getFile returned no file path")
	}
	return result.Result.FilePath, nil
}

func (s *TelegramSender) downloadTelegramMedia(messages []TelegramMessage) ([]MediaUpload, func(), error) {
	var files []string
	cleanup := func() {
		for _, file := range files {
			_ = os.Remove(file)
		}
	}
	var result []MediaUpload
	for _, message := range messages {
		ref, ok := mediaReference(message)
		if !ok {
			continue
		}
		remotePath, err := s.getTelegramFile(ref.fileID)
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
		name := SanitizeFilename(ref.name)
		if name == "" {
			name = fmt.Sprintf("telegram-%d", message.MessageID)
		}
		temp, err := os.CreateTemp(s.config.DownloadPath, "tg-upload-*")
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
		path := temp.Name()
		files = append(files, path)
		response, err := s.httpClient.Get(fmt.Sprintf("https://api.telegram.org/file/bot%s/%s", s.botToken, remotePath))
		if err != nil {
			temp.Close()
			cleanup()
			return nil, func() {}, err
		}
		_, copyErr := io.Copy(temp, response.Body)
		response.Body.Close()
		closeErr := temp.Close()
		if copyErr != nil {
			cleanup()
			return nil, func() {}, copyErr
		}
		if closeErr != nil {
			cleanup()
			return nil, func() {}, closeErr
		}
		if info, err := os.Stat(path); err != nil || info.Size() == 0 {
			cleanup()
			return nil, func() {}, fmt.Errorf("downloaded Telegram file %s is empty", name)
		}
		result = append(result, MediaUpload{Path: path, Name: filepath.Base(name), Kind: ref.kind, DurationMS: ref.duration * 1000})
	}
	return result, cleanup, nil
}
