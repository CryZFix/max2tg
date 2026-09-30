package src

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

const attachReadyTimeout = 30 * time.Second

type MaxMediaKind string

const (
	MaxMediaFile  MaxMediaKind = "file"
	MaxMediaPhoto MaxMediaKind = "photo"
	MaxMediaVideo MaxMediaKind = "video"
	MaxMediaVoice MaxMediaKind = "voice"
)

type MediaUpload struct {
	Name       string
	Size       int64
	Kind       MaxMediaKind
	DurationMS int
	OpenStream func() (io.ReadCloser, error)
}

func (m MediaUpload) Open() (io.ReadCloser, error) {
	if m.OpenStream == nil {
		return nil, errors.New("media source is unavailable")
	}
	return m.OpenStream()
}

type attachNotification struct {
	fileID  int64
	videoID int64
}

func (c *Client) initAttachNotifications() {
	c.OnFromWebSocket(func(raw string) {
		var frame WebSocketPayload
		if json.Unmarshal([]byte(raw), &frame) != nil {
			return
		}
		if opcode, ok := parseInt(frame["opcode"]); ok && opcode == int(NOTIF_ATTACH) {
			payload, _ := frame["payload"].(map[string]interface{})
			fileID, _ := parseInt64(payload["fileId"])
			videoID, _ := parseInt64(payload["videoId"])
			if fileID == 0 && videoID == 0 {
				return
			}
			select {
			case c.attachReady <- attachNotification{fileID: fileID, videoID: videoID}:
			default:
			}
		}
	})
}

func (c *Client) drainAttachNotifications() {
	for {
		select {
		case <-c.attachReady:
		default:
			return
		}
	}
}

func (c *Client) waitForAttach(id int64, attachmentType string) error {
	timer := time.NewTimer(attachReadyTimeout)
	defer timer.Stop()
	for {
		select {
		case notification := <-c.attachReady:
			matched := notification.fileID == id
			if attachmentType == "video" {
				matched = notification.videoID == id
			}
			if matched {
				return nil
			}
			Logf("Ignoring NOTIF_ATTACH for an unrelated %s upload", attachmentType)
		case <-timer.C:
			return NewTimeoutError("timed out waiting for MAX attachment confirmation")
		}
	}
}

func responsePayload(response WebSocketPayload, opcode Opcode) (map[string]interface{}, error) {
	gotOpcode, _ := parseInt(response["opcode"])
	cmd, _ := parseInt(response["cmd"])
	if gotOpcode != int(opcode) || cmd != 1 {
		return nil, NewInvalidResponseError("invalid media upload response")
	}
	payload, ok := response["payload"].(map[string]interface{})
	if !ok {
		return nil, NewInvalidResponseError("missing media upload payload")
	}
	return payload, nil
}

func firstUploadInfo(payload map[string]interface{}) (map[string]interface{}, error) {
	info, ok := payload["info"].([]interface{})
	if !ok || len(info) == 0 {
		return nil, NewInvalidResponseError("missing upload info")
	}
	result, ok := info[0].(map[string]interface{})
	if !ok {
		return nil, NewInvalidResponseError("invalid upload info")
	}
	return result, nil
}

func stringValue(values map[string]interface{}, key string) string {
	v, ok := values[key]
	if !ok {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func int64Value(values map[string]interface{}, key string) (int64, bool) {
	return parseInt64(values[key])
}

func jsonShape(value interface{}, depth int) interface{} {
	if depth == 0 {
		return "…"
	}
	switch typed := value.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			result[key] = jsonShape(nested, depth-1)
		}
		return result
	case []interface{}:
		if len(typed) == 0 {
			return []interface{}{}
		}
		return []interface{}{jsonShape(typed[0], depth-1)}
	default:
		return fmt.Sprintf("%T", value)
	}
}

func findPhotoAttachment(value interface{}) (interface{}, string, bool) {
	if object, ok := value.(map[string]interface{}); ok {
		photoID, hasPhotoID := object["photoId"]
		if !hasPhotoID {
			photoID, hasPhotoID = object["id"]
		}
		token := stringValue(object, "photoToken")
		if token == "" {
			token = stringValue(object, "token")
		}
		if hasPhotoID && token != "" {
			return photoID, token, true
		}
		if photos, ok := object["photos"].(map[string]interface{}); ok {
			for photoID, rawPhoto := range photos {
				if photo, ok := rawPhoto.(map[string]interface{}); ok {
					if token := stringValue(photo, "token"); token != "" {
						return photoID, token, true
					}
				}
			}
		}
		for _, nested := range object {
			if photoID, token, ok := findPhotoAttachment(nested); ok {
				return photoID, token, true
			}
		}
	}
	if values, ok := value.([]interface{}); ok {
		for _, nested := range values {
			if photoID, token, ok := findPhotoAttachment(nested); ok {
				return photoID, token, true
			}
		}
	}
	return nil, "", false
}

type byteCounter int64

func (c *byteCounter) Write(p []byte) (int, error) {
	*c += byteCounter(len(p))
	return len(p), nil
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func multipartUploadSize(boundary, name string, size int64) (int64, error) {
	if size <= 0 {
		return 0, errors.New("media size must be positive")
	}
	var counter byteCounter
	writer := multipart.NewWriter(&counter)
	if err := writer.SetBoundary(boundary); err != nil {
		return 0, err
	}
	part, err := writer.CreateFormFile("file", name)
	if err != nil {
		return 0, err
	}
	if _, err := io.Copy(part, io.LimitReader(zeroReader{}, size)); err != nil {
		return 0, err
	}
	if err := writer.Close(); err != nil {
		return 0, err
	}
	return int64(counter), nil
}

func (c *Client) postFile(uploadURL string, media MediaUpload) (map[string]interface{}, error) {
	boundary := fmt.Sprintf("max2tg-%d", time.Now().UnixNano())
	contentLength, err := multipartUploadSize(boundary, media.Name, media.Size)
	if err != nil {
		return nil, err
	}
	file, err := media.Open()
	if err != nil {
		return nil, err
	}

	reader, writer := io.Pipe()
	go func() {
		defer file.Close()
		multipartWriter := multipart.NewWriter(writer)
		if err := multipartWriter.SetBoundary(boundary); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		part, err := multipartWriter.CreateFormFile("file", media.Name)
		if err == nil {
			_, err = io.Copy(part, file)
		}
		if closeErr := multipartWriter.Close(); err == nil {
			err = closeErr
		}
		_ = writer.CloseWithError(err)
	}()

	httpClient, err := BuildHTTPClientWithProxy(GetMaxProxy(c.config), 60*time.Second)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, uploadURL, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	req.ContentLength = contentLength
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("MAX upload returned HTTP %d", resp.StatusCode)
	}
	result := map[string]interface{}{}
	_ = json.Unmarshal(responseBody, &result) // Some MAX upload endpoints reply with an empty body.
	return result, nil
}

func (c *Client) uploadFileAttachment(media MediaUpload) (map[string]interface{}, error) {
	if media.Size <= 0 {
		return nil, errors.New("media size must be positive")
	}
	request := c.connection.GetRequestBuilder().FileUpload(media.Name, media.Size)
	response, err := c.connection.sendAndReceive(request, 10*time.Second)
	if err != nil {
		return nil, err
	}
	payload, err := responsePayload(response, FILE_UPLOAD)
	if err != nil {
		return nil, err
	}
	upload, err := firstUploadInfo(payload)
	if err != nil {
		return nil, err
	}
	uploadURL := stringValue(upload, "url")
	if uploadURL == "" {
		return nil, NewInvalidResponseError("MAX file upload URL is empty")
	}
	c.drainAttachNotifications()
	if _, err := c.postFile(uploadURL, media); err != nil {
		return nil, err
	}
	fileID, ok := int64Value(upload, "fileId")
	if !ok {
		return nil, NewInvalidResponseError("MAX file upload did not return file ID")
	}
	if err := c.waitForAttach(fileID, "file"); err != nil {
		return nil, err
	}
	return map[string]interface{}{"_type": "FILE", "fileId": upload["fileId"], "token": stringValue(upload, "token"), "name": media.Name, "size": media.Size}, nil
}

// uploadNativeAttachment uses the documented URL opcodes. If a provider response
// does not expose the identifiers required by MSG_SEND, callers fall back to FILE.
func (c *Client) uploadNativeAttachment(chatID int, media MediaUpload) (map[string]interface{}, error) {
	var request WebSocketPayload
	var opcode Opcode
	if media.Kind == MaxMediaPhoto {
		request, opcode = c.connection.GetRequestBuilder().ImageUpload(1), IMAGE_UPLOAD
	} else {
		request, opcode = c.connection.GetRequestBuilder().VideoUpload(chatID, 1), VIDEO_UPLOAD
	}
	response, err := c.connection.sendAndReceive(request, 10*time.Second)
	if err != nil {
		return nil, err
	}
	payload, err := responsePayload(response, opcode)
	if err != nil {
		return nil, err
	}
	upload := payload
	uploadURL := stringValue(payload, "url")
	if media.Kind == MaxMediaVideo {
		upload, err = firstUploadInfo(payload)
		if err != nil {
			return nil, err
		}
		uploadURL = stringValue(upload, "url")
	}
	if uploadURL == "" {
		return nil, NewInvalidResponseError("MAX native upload URL is empty")
	}
	if media.Kind == MaxMediaVideo {
		c.drainAttachNotifications()
	}
	httpResult, err := c.postFile(uploadURL, media)
	if err != nil {
		return nil, err
	}
	if media.Kind == MaxMediaVideo {
		fileID, ok := int64Value(upload, "videoId")
		if !ok {
			return nil, NewInvalidResponseError("MAX video upload did not return video ID")
		}
		if err := c.waitForAttach(fileID, "video"); err != nil {
			return nil, err
		}
		videoID, hasVideoID := upload["videoId"]
		token := stringValue(upload, "token")
		if !hasVideoID || token == "" {
			return nil, NewInvalidResponseError("MAX video upload did not return attachment identifiers")
		}
		return map[string]interface{}{"_type": "VIDEO", "videoId": videoID, "token": token}, nil
	}
	Logf("MAX image upload response schema: %v", jsonShape(httpResult, 3))
	photoID, photoToken, found := findPhotoAttachment(httpResult)
	if !found {
		return nil, NewInvalidResponseError("MAX photo upload did not return attachment identifiers")
	}
	return map[string]interface{}{"_type": "PHOTO", "photoId": photoID, "photoToken": photoToken}, nil
}

func (c *Client) SendMediaMessage(chatID int, text string, media []MediaUpload, replyToMsgID *int) (int, error) {
	if !c.connection.IsConnected() {
		return 0, NewConnectionError("WebSocket not connected")
	}
	c.mediaMu.Lock()
	defer c.mediaMu.Unlock()
	attaches := make([]map[string]interface{}, 0, len(media))
	for _, item := range media {
		item.Name = SanitizeFilename(item.Name)
		if item.Name == "" {
			item.Name = "file"
		}
		var attach map[string]interface{}
		var err error
		if item.Kind == MaxMediaVoice {
			// AUDIO tokens and MSG_SEND must be issued through the same binary
			// web connection. A voice message cannot be combined with an album.
			if len(media) != 1 {
				err = errors.New("MAX voice cannot be combined with other media")
			} else {
				return c.sendWebVoice(chatID, text, item, replyToMsgID)
			}
		} else if item.Kind == MaxMediaPhoto || item.Kind == MaxMediaVideo {
			attach, err = c.uploadNativeAttachment(chatID, item)
			if err != nil {
				Logf("Native %s upload failed for %s; falling back to FILE: %v", item.Kind, item.Name, err)
			}
		}
		if attach == nil {
			attach, err = c.uploadFileAttachment(item)
		}
		if err != nil {
			return 0, fmt.Errorf("upload %s: %w", item.Name, err)
		}
		attaches = append(attaches, attach)
	}
	request := c.connection.GetRequestBuilder().SendMessageWithAttachments(chatID, text, attaches, replyToMsgID)
	response, err := c.connection.sendAndReceive(request, 10*time.Second)
	if err != nil {
		return 0, err
	}
	payload, err := responsePayload(response, SEND_MESSAGE)
	if err != nil {
		return 0, err
	}
	message, ok := payload["message"].(map[string]interface{})
	if !ok {
		return 0, NewInvalidResponseError("MAX media send returned no message")
	}
	return ParseID(message["id"]), nil
}

func mediaKindFromMIME(mimeType string) MaxMediaKind {
	if strings.HasPrefix(strings.ToLower(mimeType), "image/") {
		return MaxMediaPhoto
	}
	if strings.HasPrefix(strings.ToLower(mimeType), "video/") {
		return MaxMediaVideo
	}
	return MaxMediaFile
}
