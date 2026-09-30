package src

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	FileSize int64  `json:"file_size"`
}

type telegramMediaRef struct {
	fileID   string
	name     string
	mimeType string
	kind     MaxMediaKind
	duration int
	size     int64
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
		return telegramMediaRef{fileID: photo.FileID, name: fmt.Sprintf("photo-%d.jpg", message.MessageID), mimeType: "image/jpeg", kind: MaxMediaPhoto, size: photo.FileSize}, photo.FileID != ""
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
		return telegramMediaRef{fileID: choice.ref.FileID, name: name, mimeType: choice.ref.MimeType, kind: choice.kind, duration: choice.ref.Duration, size: choice.ref.FileSize}, true
	}
	return telegramMediaRef{}, false
}

func (s *TelegramSender) getTelegramFile(fileID string) (telegramRemoteFile, error) {
	url := s.apiURL("getFile")
	payload, _ := json.Marshal(map[string]string{"file_id": fileID})
	resp, err := s.httpClient.Post(url, "application/json", strings.NewReader(string(payload)))
	if err != nil {
		return telegramRemoteFile{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return telegramRemoteFile{}, fmt.Errorf("Telegram getFile: %s", string(body))
	}
	var result struct {
		OK     bool               `json:"ok"`
		Result telegramRemoteFile `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return telegramRemoteFile{}, err
	}
	if !result.OK || result.Result.FilePath == "" {
		return telegramRemoteFile{}, fmt.Errorf("Telegram getFile returned no file path")
	}
	return result.Result, nil
}

func (s *TelegramSender) downloadTelegramMedia(messages []TelegramMessage) ([]MediaUpload, error) {
	var result []MediaUpload
	for _, message := range messages {
		ref, ok := mediaReference(message)
		if !ok {
			continue
		}
		remote, err := s.getTelegramFile(ref.fileID)
		if err != nil {
			return nil, err
		}
		name := SanitizeFilename(ref.name)
		if name == "" {
			name = fmt.Sprintf("telegram-%d", message.MessageID)
		}
		size := ref.size
		if remote.FileSize > 0 {
			size = remote.FileSize
		}
		if size <= 0 {
			return nil, fmt.Errorf("Telegram file %s has no size", name)
		}
		remotePath := remote.FilePath
		result = append(result, MediaUpload{
			Name:       name,
			Size:       size,
			Kind:       ref.kind,
			DurationMS: ref.duration * 1000,
			OpenStream: func() (io.ReadCloser, error) {
				response, err := s.httpClient.Get(s.fileURL(remotePath))
				if err != nil {
					return nil, err
				}
				if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
					response.Body.Close()
					return nil, fmt.Errorf("Telegram file download returned HTTP %d", response.StatusCode)
				}
				return response.Body, nil
			},
		})
	}
	return result, nil
}
