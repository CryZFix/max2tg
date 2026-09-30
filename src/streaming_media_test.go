package src

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPostFileStreamsMultipartWithExactLength(t *testing.T) {
	const content = "streamed MAX payload"
	var opens atomic.Int32
	serverErr := make(chan error, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength <= 0 {
			serverErr <- fmt.Errorf("missing Content-Length")
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			serverErr <- err
			return
		}
		if int64(len(body)) != r.ContentLength {
			serverErr <- fmt.Errorf("body length %d does not match Content-Length %d", len(body), r.ContentLength)
			return
		}
		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "multipart/form-data" {
			serverErr <- fmt.Errorf("invalid content type %q: %w", r.Header.Get("Content-Type"), err)
			return
		}
		reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		part, err := reader.NextPart()
		if err != nil {
			serverErr <- err
			return
		}
		got, err := io.ReadAll(part)
		if err != nil {
			serverErr <- err
			return
		}
		if string(got) != content {
			serverErr <- fmt.Errorf("unexpected uploaded content %q", got)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	media := MediaUpload{
		Name: "payload.txt",
		Size: int64(len(content)),
		OpenStream: func() (io.ReadCloser, error) {
			opens.Add(1)
			return io.NopCloser(strings.NewReader(content)), nil
		},
	}
	client := &Client{config: &Config{}}
	if _, err := client.postFile(server.URL, media); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-serverErr:
		t.Fatal(err)
	default:
	}
	if opens.Load() != 1 {
		t.Fatalf("stream opened %d times, want 1", opens.Load())
	}
}

func TestPrepareTelegramMediaDoesNotWriteToDisk(t *testing.T) {
	const content = "telegram file"
	var fileDownloads atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/botTOKEN/getFile":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"ok":     true,
				"result": map[string]interface{}{"file_path": "documents/file.txt", "file_size": len(content)},
			})
		case "/file/botTOKEN/documents/file.txt":
			fileDownloads.Add(1)
			_, _ = w.Write([]byte(content))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloadPath := t.TempDir()
	sender := &TelegramSender{
		botToken:   "TOKEN",
		config:     &Config{TelegramAPIURL: server.URL, DownloadPath: downloadPath},
		httpClient: server.Client(),
	}
	media, err := sender.downloadTelegramMedia([]TelegramMessage{{
		MessageID: 1,
		Document:  &TelegramFileRef{FileID: "file-id", FileName: "file.txt", FileSize: int64(len(content))},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(media) != 1 {
		t.Fatalf("prepared %d media items, want 1", len(media))
	}
	if media[0].Size != int64(len(content)) {
		t.Fatalf("size %d, want %d", media[0].Size, len(content))
	}
	entries, err := os.ReadDir(downloadPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("download path is not empty: %v", entries)
	}
	reader, err := media[0].Open()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Fatalf("unexpected Telegram content %q", got)
	}
	if fileDownloads.Load() != 1 {
		t.Fatalf("file downloaded %d times, want 1", fileDownloads.Load())
	}
}
