package src

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type GitHubRelease struct {
	TagName    string `json:"tag_name"`
	Draft      bool   `json:"draft"`
	PreRelease bool   `json:"prerelease"`
}

func CheckForUpdates(ctx context.Context) (latestVersion string, updateAvailable bool, err error) {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	releaseAPIURL := strings.Replace(RepositoryURL, "https://github.com/", "https://api.github.com/repos/", 1) + "/releases/latest"
	req, err := http.NewRequestWithContext(ctx, "GET", releaseAPIURL, nil)
	if err != nil {
		return "", false, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", false, fmt.Errorf("failed to check for updates: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", false, fmt.Errorf("GitHub API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, fmt.Errorf("failed to read response: %w", err)
	}

	var release GitHubRelease
	if err := json.Unmarshal(body, &release); err != nil {
		return "", false, fmt.Errorf("failed to parse response: %w", err)
	}

	latestVersion = strings.TrimPrefix(release.TagName, "v")
	updateAvailable = CompareVersions(AppVersion, latestVersion) < 0

	return latestVersion, updateAvailable, nil
}

func CompareVersions(v1, v2 string) int {
	parts1 := strings.Split(v1, ".")
	parts2 := strings.Split(v2, ".")

	maxLen := len(parts1)
	if len(parts2) > maxLen {
		maxLen = len(parts2)
	}

	for i := 0; i < maxLen; i++ {
		p1 := 0
		p2 := 0

		if i < len(parts1) {
			fmt.Sscanf(parts1[i], "%d", &p1)
		}
		if i < len(parts2) {
			fmt.Sscanf(parts2[i], "%d", &p2)
		}

		if p1 < p2 {
			return -1
		}
		if p1 > p2 {
			return 1
		}
	}

	return 0
}

func GetVersionInfo() string {
	return fmt.Sprintf("%s %s", AppName, AppVersion)
}
