package src

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const nativeVoiceRetryCount = 5

// sendWebVoice uses the same authenticated binary WEB session as the MAX web
// client. Unlike a regular FILE upload, this produces a playable voice message.
func (c *Client) sendWebVoice(chatID int, text string, media MediaUpload, replyToMsgID *int) (int, error) {
	transport, err := dialMobile(c.config, nil)
	if err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, err)
	}
	defer transport.Close()
	if _, err := transport.webHello(c.config); err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, fmt.Errorf("web voice HELLO: %w", err))
	}
	if _, err := transport.webBinaryLogin(c.config.Token); err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, fmt.Errorf("web voice login: %w", err))
	}
	slot, err := transport.request(uint16(VIDEO_UPLOAD), map[string]interface{}{
		"count": 1, "type": 2, "uploaderType": 1,
	}, 0, nil)
	if err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, fmt.Errorf("web audio upload URL: %w", err))
	}
	info, err := firstUploadInfo(slot)
	if err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, err)
	}
	url, token := stringValue(info, "url"), stringValue(info, "token")
	audioID, hasAudioID := info["videoId"]
	if url == "" || token == "" || !hasAudioID {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, NewInvalidResponseError("web audio upload slot is incomplete"))
	}
	userAgent := ""
	if c.config.UserAgent != nil {
		userAgent = c.config.UserAgent.UserAgent
	}
	if err := c.postAudio(url, media.Path, userAgent); err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, fmt.Errorf("web audio upload: %w", err))
	}
	duration := media.DurationMS
	if measured, err := audioDurationMS(media.Path); err == nil && measured > 0 {
		duration = measured
	}
	attach := map[string]interface{}{"_type": "AUDIO", "audioId": audioID, "duration": duration, "wave": audioWaveform(media.Path), "token": token}
	cid := -time.Now().UnixMilli()
	c.markLocalCID(int(cid))
	message := map[string]interface{}{"cid": cid, "attaches": []interface{}{attach}}
	if text != "" {
		message["text"] = text
	}
	if replyToMsgID != nil {
		message["link"] = map[string]interface{}{"type": "REPLY", "messageId": strconv.Itoa(*replyToMsgID)}
	}
	response, err := transport.request(uint16(SEND_MESSAGE), map[string]interface{}{"chatId": chatID, "message": message, "notify": true}, 2, nil)
	if err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, fmt.Errorf("web voice send: %w", err))
	}
	messageData, ok := response["message"].(map[string]interface{})
	if !ok {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, NewInvalidResponseError("web voice send returned no message"))
	}
	return ParseID(messageData["id"]), nil
}

func (c *Client) sendNativeVoice(chatID int, text string, media MediaUpload, replyToMsgID *int) (int, error) {
	session, err := LoadMobileSession(c.config.MobileSessionPath())
	if err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, err)
	}
	transport, err := dialMobile(c.config, nil)
	if err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, err)
	}
	defer transport.Close()
	if _, err := transport.hello(session); err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, fmt.Errorf("mobile HELLO: %w", err))
	}
	if _, err := transport.mobileLogin(session.LoginToken); err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, fmt.Errorf("mobile login: %w", err))
	}

	slot, err := transport.request(uint16(VIDEO_UPLOAD), map[string]interface{}{
		"uploaderType": 1, "type": 2, "count": 1,
	}, 0, nil)
	if err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, fmt.Errorf("audio upload URL: %w", err))
	}
	info, err := firstUploadInfo(slot)
	if err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, err)
	}
	url, token := stringValue(info, "url"), stringValue(info, "token")
	if url == "" || token == "" {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, NewInvalidResponseError("audio upload slot is incomplete"))
	}
	if err := c.postNativeAudio(url, media.Path); err != nil {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, err)
	}

	duration := media.DurationMS
	if measured, err := audioDurationMS(media.Path); err == nil && measured > 0 {
		duration = measured
	}
	wave := audioWaveform(media.Path)
	attach := map[string]interface{}{"_type": "AUDIO", "token": token, "duration": duration, "wave": wave}
	cid := -(time.Now().UnixMilli())
	message := map[string]interface{}{"cid": cid, "text": text, "elements": []interface{}{}, "attaches": []interface{}{attach}}
	if replyToMsgID != nil {
		message["link"] = map[string]interface{}{"type": "REPLY", "messageId": strconv.Itoa(*replyToMsgID)}
	}
	var response map[string]interface{}
	for attempt := 0; attempt < nativeVoiceRetryCount; attempt++ {
		response, err = transport.request(uint16(SEND_MESSAGE), map[string]interface{}{"chatId": chatID, "message": message, "notify": true}, 0, nil)
		if err == nil {
			break
		}
		if !strings.Contains(strings.ToLower(err.Error()), "not.ready") || attempt == nativeVoiceRetryCount-1 {
			return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, err)
		}
		time.Sleep(time.Duration(1<<attempt) * 500 * time.Millisecond)
	}
	messageData, ok := response["message"].(map[string]interface{})
	if !ok {
		return c.sendVoiceAsFile(chatID, text, media, replyToMsgID, NewInvalidResponseError("native voice send returned no message"))
	}
	return ParseID(messageData["id"]), nil
}

func (c *Client) sendVoiceAsFile(chatID int, text string, media MediaUpload, replyToMsgID *int, nativeErr error) (int, error) {
	Logf("MAX voice upload failed for %s; falling back to FILE: %v", media.Name, nativeErr)
	attach, err := c.uploadFileAttachment(media)
	if err != nil {
		return 0, fmt.Errorf("native voice failed (%v), file fallback failed: %w", nativeErr, err)
	}
	request := c.connection.GetRequestBuilder().SendMessageWithAttachments(chatID, text, []map[string]interface{}{attach}, replyToMsgID)
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
		return 0, NewInvalidResponseError("MAX voice fallback returned no message")
	}
	return ParseID(message["id"]), nil
}

func (c *Client) postNativeAudio(uploadURL, path string) error {
	return c.postAudio(uploadURL, path, "OKMessages/"+mobileAppVersion+" (Android 14)")
}

func (c *Client) postAudio(uploadURL, path, userAgent string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return fmt.Errorf("audio file is empty")
	}
	httpClient, err := BuildHTTPClientWithProxy(GetMaxProxy(c.config), 60*time.Second)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, uploadURL, file)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%d", time.Now().UnixNano()))
	req.Header.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", info.Size()-1, info.Size()))
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("MAX audio upload returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func audioDurationMS(path string) (int, error) {
	output, err := exec.Command("ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", path).Output()
	if err != nil {
		return 0, err
	}
	seconds, err := strconv.ParseFloat(strings.TrimSpace(string(output)), 64)
	if err != nil {
		return 0, err
	}
	return int(seconds*1000 + .5), nil
}

func audioWaveform(path string) []byte {
	// ffmpeg's s16le stream is portable and lets us generate MAX's 80 amplitude
	// bytes without retaining the original audio in memory.
	cmd := exec.Command("ffmpeg", "-v", "error", "-i", path, "-ac", "1", "-ar", "8000", "-f", "s16le", "pipe:1")
	pcm, err := cmd.Output()
	if err != nil || len(pcm) < 2 {
		return make([]byte, 80)
	}
	samples := len(pcm) / 2
	wave := make([]byte, 80)
	for i := range wave {
		start, end := i*samples/len(wave), (i+1)*samples/len(wave)
		var peak int
		for sample := start; sample < end; sample++ {
			v := int(int16(binary.LittleEndian.Uint16(pcm[sample*2:])))
			if v < 0 {
				v = -v
			}
			if v > peak {
				peak = v
			}
		}
		wave[i] = byte(peak * 255 / 32767)
	}
	return wave
}
