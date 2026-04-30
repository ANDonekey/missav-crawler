package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/RomainMichau/CycleTLS/cycletls"
	"github.com/RomainMichau/cloudscraper_go/cloudscraper"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
)

const startURL = "https://missav.ws/dm194/cn"
const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:150.0) Gecko/20100101 Firefox/150.0"
const defaultDetailLimit = 10
const defaultJobLimit = 100
const defaultMaxRetries = 3
const maxM3U8Concurrency = 6

var sharedScraper *cloudscraper.CloudScrapper
var scraperMu sync.Mutex

var (
	startTime = time.Now()
	statusMu  sync.Mutex
	crawlStat = CrawlStatus{}
)

type CrawlStatus struct {
	ListJobsProcessed   int
	DetailJobsProcessed int
	ListJobsPending     int
	DetailJobsPending   int
	VideosFound         int
	VideosDone          int
	CurrentJob          string
	Errors              int
}

func startProfilingServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/status", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		statusMu.Lock()
		s := crawlStat
		statusMu.Unlock()

		goroutines := runtime.NumGoroutine()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		elapsed := time.Since(startTime).Round(time.Second)
		fmt.Fprintf(w, `Status: running
Elapsed: %s
Goroutines: %d
Memory: %.1f MB

%s
`, elapsed, goroutines, float64(m.Alloc)/1024/1024, s.String())
	})
	mux.HandleFunc("/debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
		http.DefaultServeMux.ServeHTTP(w, r)
	})

	go func() {
		port := os.Getenv("PORT")
		if port == "" {
			port = "6060"
		}
		addr := ":" + port
		log.Printf("profiling server started on %s", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("profiling server error: %v", err)
		}
	}()
}

func (c *CrawlStatus) String() string {
	return fmt.Sprintf(
		`List jobs:  %d processed, %d pending
Detail jobs: %d processed, %d pending
Videos:      %d total, %d with details
Errors:      %d
Current:     %s`,
		c.ListJobsProcessed, c.ListJobsPending,
		c.DetailJobsProcessed, c.DetailJobsPending,
		c.VideosFound, c.VideosDone,
		c.Errors, c.CurrentJob,
	)
}

type Video struct {
	Code     string
	URL      string
	Title    string
	Duration string
	Section  string
}

type VideoDetail struct {
	Code            string
	URL             string
	Title           string
	Description     string
	CoverURL        string
	ReleaseDate     string
	DurationSeconds string
	Actors          string
	Genres          string
	Maker           string
	Director        string
	Tags            string
	M3U8URLs        []string
}

type M3U8Playlist struct {
	URL     string
	Content string
}

type VideoListResult struct {
	Videos      []Video
	ListURLs    []string
	NextPageURL string
}

type CrawlJob struct {
	ID          int
	JobType     string
	URL         string
	VideoCode   string
	Priority    int
	RetryCount  int
	MaxRetries  int
	ScheduledAt string
}

func ConnectDB() *sql.DB {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/missav?sslmode=disable"
		log.Printf("DATABASE_URL not set, using local Postgres: %s", dsn)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("open postgres failed: %v", err)
	}
	if err := db.Ping(); err != nil {
		log.Fatalf("connect postgres failed: %v", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if shouldRebuildTables() {
		mustExec(db, `DROP TABLE IF EXISTS streams;`)
		mustExec(db, `DROP TABLE IF EXISTS crawl_jobs;`)
		mustExec(db, `DROP TABLE IF EXISTS videos;`)
		if err := os.RemoveAll("m3u8"); err != nil {
			log.Printf("remove m3u8 dir failed: %v", err)
		}
	}
	mustExec(db, `DROP TABLE IF EXISTS video_details;`)

	mustExec(db, `
		CREATE TABLE IF NOT EXISTS videos (
			id BIGSERIAL PRIMARY KEY,
			code TEXT NOT NULL UNIQUE,
			url TEXT NOT NULL UNIQUE,
			title TEXT,
			duration TEXT,
			section TEXT,
			description TEXT,
			cover_url TEXT,
			release_date TEXT,
			duration_seconds TEXT,
			actors TEXT,
			genres TEXT,
			maker TEXT,
			director TEXT,
			tags TEXT,
			detail_status TEXT NOT NULL DEFAULT 'pending',
			stream_status TEXT NOT NULL DEFAULT 'pending',
			first_seen_at TEXT NOT NULL,
			last_seen_at TEXT NOT NULL,
			last_crawled_at TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
	`)
	ensureVideoColumns(db)

	mustExec(db, `
		CREATE TABLE IF NOT EXISTS streams (
			id BIGSERIAL PRIMARY KEY,
			video_code TEXT NOT NULL,
			video_url TEXT NOT NULL,
			stream_type TEXT,
			m3u8_url TEXT NOT NULL UNIQUE,
			m3u8_path TEXT NOT NULL,
			fetched_at TEXT NOT NULL,
			created_at TEXT NOT NULL,
			FOREIGN KEY (video_code) REFERENCES videos (code)
		);
	`)

	mustExec(db, `
		CREATE TABLE IF NOT EXISTS crawl_jobs (
			id BIGSERIAL PRIMARY KEY,
			job_type TEXT NOT NULL,
			url TEXT NOT NULL,
			video_code TEXT,
			status TEXT NOT NULL,
			priority INTEGER NOT NULL DEFAULT 0,
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 3,
			last_error TEXT,
			scheduled_at TEXT NOT NULL,
			started_at TEXT,
			finished_at TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE (job_type, url)
		);
	`)
	ensureJobColumns(db)
	mustExec(db, `CREATE INDEX IF NOT EXISTS idx_videos_code ON videos (code);`)
	mustExec(db, `CREATE INDEX IF NOT EXISTS idx_crawl_jobs_pending ON crawl_jobs (job_type, status, scheduled_at, priority DESC, id);`)

	return db
}

func shouldRebuildTables() bool {
	return os.Getenv("REBUILD_TABLES") == "1"
}

func crawlMode() string {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("CRAWL_MODE")))
	if mode == "init" {
		return "init"
	}
	return "incremental"
}

func detailLimit() int {
	return envInt("DETAIL_LIMIT", defaultDetailLimit)
}

func jobLimit() int {
	return envInt("JOB_LIMIT", defaultJobLimit)
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func ensureVideoColumns(db *sql.DB) {
	columns := []string{
		`ALTER TABLE videos ADD COLUMN IF NOT EXISTS detail_status TEXT NOT NULL DEFAULT 'pending';`,
		`ALTER TABLE videos ADD COLUMN IF NOT EXISTS stream_status TEXT NOT NULL DEFAULT 'pending';`,
		`ALTER TABLE videos ADD COLUMN IF NOT EXISTS first_seen_at TEXT NOT NULL DEFAULT '';`,
		`ALTER TABLE videos ADD COLUMN IF NOT EXISTS last_seen_at TEXT NOT NULL DEFAULT '';`,
		`ALTER TABLE videos ADD COLUMN IF NOT EXISTS last_crawled_at TEXT;`,
	}

	for _, sql := range columns {
		mustExec(db, sql)
	}
}

func ensureJobColumns(db *sql.DB) {
	columns := []string{
		`ALTER TABLE crawl_jobs ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0;`,
		`ALTER TABLE crawl_jobs ADD COLUMN IF NOT EXISTS max_retries INTEGER NOT NULL DEFAULT 3;`,
		`ALTER TABLE crawl_jobs ADD COLUMN IF NOT EXISTS scheduled_at TEXT NOT NULL DEFAULT '';`,
		`ALTER TABLE crawl_jobs ADD COLUMN IF NOT EXISTS started_at TEXT;`,
		`ALTER TABLE crawl_jobs ADD COLUMN IF NOT EXISTS finished_at TEXT;`,
	}

	for _, sql := range columns {
		mustExec(db, sql)
	}
}

func mustExec(db *sql.DB, query string, args ...any) {
	if _, err := db.Exec(query, args...); err != nil {
		log.Fatalf("sql exec failed: %v\n%s", err, query)
	}
}

func NewHeaders(referer string) map[string]string {
	headers := map[string]string{
		"Accept":                    "*/*",
		"Accept-Language":           "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5",
		"Referer":                   referer,
		"Upgrade-Insecure-Requests": "1",
		"Sec-Fetch-Dest":            "document",
		"Sec-Fetch-Mode":            "navigate",
		"Sec-Fetch-Site":            "same-origin",
		"Sec-Fetch-User":            "?1",
		"Priority":                  "u=0, i",
	}

	if cookie := os.Getenv("MISSAV_COOKIE"); cookie != "" {
		headers["Cookie"] = cookie
	}

	return headers
}

func FetchHTML(targetURL string, referer string) (string, error) {
	return FetchHTMLWithOptions(targetURL, referer, 30, 3)
}

func FetchDetailHTML(targetURL string, referer string) (string, error) {
	return FetchHTMLWithOptions(targetURL, referer, 15, 3)
}

func FetchHTMLWithOptions(targetURL string, referer string, timeout int, attempts int) (string, error) {
	headers := NewHeaders(referer)
	headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
	return FetchText(targetURL, headers, timeout, attempts)
}

func FetchM3U8(targetURL string, referer string) (string, error) {
	var lastErr error
	for i := 1; i <= 3; i++ {
		req, err := http.NewRequest(http.MethodGet, targetURL, nil)
		if err != nil {
			return "", err
		}

		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", "application/vnd.apple.mpegurl,application/x-mpegURL,text/plain,*/*")
		req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5")
		req.Header.Set("Referer", referer)

		client := http.Client{Timeout: 8 * time.Second}
		res, err := client.Do(req)
		if err != nil {
			lastErr = err
			log.Printf("m3u8 request error, retry %d: %s: %v", i, targetURL, err)
			time.Sleep(time.Duration(i) * time.Second)
			continue
		}

		if res.StatusCode >= 200 && res.StatusCode < 300 {
			body, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil {
				lastErr = err
				log.Printf("m3u8 read error, retry %d: %s: %v", i, targetURL, err)
				time.Sleep(time.Duration(i) * time.Second)
				continue
			}
			return string(body), nil
		}
		res.Body.Close()

		lastErr = fmt.Errorf("unexpected status %d", res.StatusCode)
		log.Printf("m3u8 status, retry %d: %s: %v", i, targetURL, lastErr)
		ResetScraper()
		time.Sleep(30 * time.Second)
		}

	return "", lastErr
}

func FetchText(targetURL string, headers map[string]string, timeout int, attempts int) (string, error) {
	var lastErr error
	for i := 1; i <= attempts; i++ {
		client, err := GetScraper()
		if err != nil {
			return "", err
		}

		options := cycletls.Options{
			Headers:   headers,
			UserAgent: userAgent,
			Timeout:   timeout,
		}
		res, err := client.Do(targetURL, options, "GET")
		if err != nil {
			lastErr = err
			log.Printf("request error, retry %d: %s: %v", i, targetURL, err)
			time.Sleep(time.Duration(i) * time.Second)
			continue
		}

		if res.Status >= 200 && res.Status < 300 {
			return res.Body, nil
		}

		lastErr = fmt.Errorf("unexpected status %d", res.Status)
		log.Printf("request status, retry %d: %s: %v", i, targetURL, lastErr)
		ResetScraper()
		time.Sleep(30 * time.Second)
	}

	return "", lastErr
}

func GetScraper() (*cloudscraper.CloudScrapper, error) {
	scraperMu.Lock()
	defer scraperMu.Unlock()

	if sharedScraper != nil {
		return sharedScraper, nil
	}

	client, err := cloudscraper.Init(false, false)
	if err != nil {
		return nil, err
	}
	sharedScraper = client
	return sharedScraper, nil
}

func ResetScraper() {
	scraperMu.Lock()
	defer scraperMu.Unlock()

	sharedScraper = nil
}

func CrawlVideoListPage(db *sql.DB, targetURL string) (VideoListResult, error) {
	body, err := FetchHTML(targetURL, startURL)
	if err != nil {
		return VideoListResult{}, err
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(body))
	if err != nil {
		return VideoListResult{}, err
	}

	videos := map[string]Video{}
	order := []string{}
	listURLs := map[string]bool{}
	listOrder := []string{}
	section := deriveListSection(targetURL)

	doc.Find("body *").Each(func(_ int, s *goquery.Selection) {
		if goquery.NodeName(s) == "h2" {
			if nextSection := normalizeVideoSection(s.Text()); nextSection != "" {
				section = nextSection
			}
			return
		}

		if goquery.NodeName(s) != "a" {
			return
		}

		href, exists := s.Attr("href")
		if !exists || strings.HasPrefix(strings.TrimSpace(href), "#") {
			return
		}

		videoURL := absoluteURL(targetURL, href)
		if isListURL(videoURL) {
			if !listURLs[videoURL] {
				listURLs[videoURL] = true
				listOrder = append(listOrder, videoURL)
			}
			return
		}

		if !isVideoURL(videoURL) {
			return
		}

		video := Video{
			Code:     extractCodeFromURL(videoURL),
			URL:      videoURL,
			Title:    extractTitle(s),
			Duration: extractDuration(s),
			Section:  section,
		}

		current, exists := videos[videoURL]
		if !exists {
			videos[videoURL] = video
			order = append(order, videoURL)
			return
		}

		if shouldReplaceTitle(current.Title, video.Title) {
			current.Title = video.Title
		}
		if current.Duration == "" && video.Duration != "" {
			current.Duration = video.Duration
		}
		if current.Section == "" && video.Section != "" {
			current.Section = video.Section
		}
		videos[videoURL] = current
	})

		newCount := 0
		log.Printf("videos found on page: %d", len(order))
		result := make([]Video, 0, len(order))
		for _, videoURL := range order {
			video := videos[videoURL]
			if saveVideo(db, video) {
				newCount++
			}
			result = append(result, video)
		}

		var nextPageURL string
		if len(order) > 0 && newCount == 0 {
			log.Printf("page has no new videos, stopping pagination: %s", targetURL)
		} else if next := nextPageURLWithPage(targetURL); next != "" {
			nextPageURL = next
		}

		log.Printf("list urls found on page: %d", len(listOrder))
		return VideoListResult{Videos: result, ListURLs: listOrder, NextPageURL: nextPageURL}, nil
}

func CrawlVideoDetailPage(db *sql.DB, video Video) error {
	body, err := FetchDetailHTML(video.URL, startURL)
	if err != nil {
		return err
	}

	detail, err := ParseVideoDetail(video.URL, body)
	if err != nil {
		return err
	}
	if detail.Title == "" {
		detail.Title = video.Title
	}
	if detail.Code == "" {
		detail.Code = video.Code
	}
	if detail.Code == "" {
		detail.Code = extractCodeFromURL(video.URL)
	}

	saveVideoDetail(db, detail, video)
	playlists := FetchM3U8Playlists(detail.URL, detail.M3U8URLs)
	for _, playlist := range playlists {
		saveStream(db, detail.Code, detail.URL, "hls", playlist.URL, playlist.Content)
	}

	if len(playlists) == 0 {
		updateVideoStreamStatus(db, detail.Code, "failed")
		log.Printf("detail saved, no m3u8 found: %s", detail.URL)
	} else {
		updateVideoStreamStatus(db, detail.Code, "done")
		log.Printf("detail saved, m3u8 saved: %d %s", len(playlists), detail.URL)
	}

	return nil
}

func ParseVideoDetail(videoURL string, body string) (VideoDetail, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(body))
	if err != nil {
		return VideoDetail{}, err
	}

	detail := VideoDetail{
		Code:     extractCodeFromURL(videoURL),
		URL:      videoURL,
		Title:    normalizeText(doc.Find("h1").First().Text()),
		M3U8URLs: extractM3U8URLs(body),
	}
	actors := []string{}
	genres := []string{}
	tags := []string{}

	doc.Find("meta").Each(func(_ int, s *goquery.Selection) {
		key, _ := s.Attr("name")
		if key == "" {
			key, _ = s.Attr("property")
		}
		content, _ := s.Attr("content")
		content = normalizeText(content)
		if content == "" {
			return
		}

		switch key {
		case "description", "og:description":
			if detail.Description == "" {
				detail.Description = content
			}
		case "og:title":
			if detail.Title == "" {
				detail.Title = content
			}
		case "og:image", "twitter:image":
			if detail.CoverURL == "" {
				detail.CoverURL = content
			}
		case "og:video:release_date":
			detail.ReleaseDate = content
		case "og:video:duration":
			detail.DurationSeconds = content
		case "og:video:actor":
			actors = append(actors, content)
		}
	})

	doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		text := normalizeText(s.Text())
		if text == "" {
			return
		}

		switch {
		case strings.Contains(href, "/actresses/"):
			actors = append(actors, text)
		case strings.Contains(href, "/genres/"):
			genres = append(genres, text)
		case strings.Contains(href, "/makers/"):
			if detail.Maker == "" {
				detail.Maker = text
			}
		case strings.Contains(href, "/directors/"):
			if detail.Director == "" {
				detail.Director = text
			}
		case strings.Contains(href, "/tags/"):
			tags = append(tags, text)
		}
	})

	detail.Actors = strings.Join(uniqueStrings(actors), ", ")
	detail.Genres = strings.Join(uniqueStrings(genres), ", ")
	detail.Tags = strings.Join(uniqueStrings(tags), ", ")
	return detail, nil
}

func absoluteURL(baseURL string, href string) string {
	parsedBaseURL, err := url.Parse(baseURL)
	if err != nil {
		return href
	}

	parsedHref, err := url.Parse(href)
	if err != nil {
		return href
	}

	return canonicalCrawlURL(parsedBaseURL.ResolveReference(parsedHref).String())
}

func canonicalCrawlURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	u.Fragment = ""
	return u.String()
}


func nextPageURLWithPage(currentURL string) string {
	u, err := url.Parse(currentURL)
	if err != nil {
		return ""
	}
	q := u.Query()
	page := 1
	if p := q.Get("page"); p != "" {
		page, _ = strconv.Atoi(p)
		page++
	} else {
		page = 2
	}
	q.Set("page", strconv.Itoa(page))
	u.RawQuery = q.Encode()
	return u.String()
}

func extractCodeFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}

	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) == 0 {
		return ""
	}

	return strings.ToUpper(parts[len(parts)-1])
}

func isVideoURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	if u.Host != "missav.ws" {
		return false
	}

	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 {
		return false
	}

	var slug string
	switch {
	case len(parts) == 2 && parts[0] == "cn":
		slug = parts[1]
	case len(parts) >= 3 && strings.HasPrefix(parts[0], "dm") && parts[1] == "cn":
		slug = parts[2]
	default:
		return false
	}

	if slug == "" {
		return false
	}

	blocked := map[string]bool{
		"actresses":        true,
		"chinese-subtitle": true,
		"genres":           true,
		"makers":           true,
		"monthly-hot":      true,
		"new":              true,
		"release":          true,
		"search":           true,
		"tags":             true,
		"today-hot":        true,
		"users":            true,
		"vip":              true,
		"vr":               true,
		"weekly-hot":       true,
	}

	if blocked[slug] {
		return false
	}

	return strings.Contains(slug, "-") && containsDigit(slug)
}

func isListURL(rawURL string) bool {
	if rawURL == "" || strings.Contains(rawURL, "#") || isVideoURL(rawURL) {
		return false
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	if u.RawQuery != "" {
		for _, key := range strings.Split(u.RawQuery, "&") {
			pair := strings.SplitN(key, "=", 2)
			if len(pair) == 2 && (pair[0] == "sort" || pair[0] == "filters") {
				return false
			}
		}
	}
	if err != nil || u.Host != "missav.ws" {
		return false
	}

	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) == 0 {
		return false
	}

	if len(parts) == 1 && parts[0] == "cn" {
		return true
	}

	if parts[0] == "cn" {
		return isKnownListPath(parts[1:])
	}

	if strings.HasPrefix(parts[0], "dm") && len(parts) >= 2 && parts[1] == "cn" {
		return true
	}

	return false
}

func isKnownListPath(parts []string) bool {
	if len(parts) == 0 {
		return true
	}

	switch parts[0] {
	case "new", "release", "chinese-subtitle", "uncensored-leak",
		"today-hot", "weekly-hot", "monthly-hot",
		"genres", "makers", "actresses", "tags",
		"fc2", "heyzo", "tokyohot", "siro", "luxu", "gana",
		"maan", "scute", "ara", "madou", "twav", "furuke":
		return true
	default:
		return false
	}
}

func deriveListSection(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}

	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) == 0 {
		return ""
	}

	slug := parts[len(parts)-1]
	switch slug {
	case "cn":
		return "首页"
	case "new":
		return "最近更新"
	case "release":
		return "新作上市"
	case "chinese-subtitle":
		return "中文字幕"
	case "uncensored-leak":
		return "无码流出"
	case "today-hot":
		return "今日热门"
	case "weekly-hot":
		return "本週热门"
	case "monthly-hot":
		return "本月热门"
	default:
		return ""
	}
}

func normalizeVideoSection(value string) string {
	text := normalizeText(value)
	switch text {
	case "首页", "推荐给你", "新作上市", "最近更新", "无码影片", "随机",
		"中文字幕", "无码流出", "今日热门", "本週热门", "本月热门":
		return text
	case "本周热门":
		return "本週热门"
	default:
		return ""
	}
}

func containsDigit(value string) bool {
	for _, r := range value {
		if r >= '0' && r <= '9' {
			return true
		}
	}

	return false
}

func extractTitle(a *goquery.Selection) string {
	title := normalizeText(a.Text())
	if title != "" && !looksLikeDuration(title) {
		return title
	}

	for _, attr := range []string{"title", "alt"} {
		value, exists := a.Attr(attr)
		if exists && normalizeText(value) != "" {
			return normalizeText(value)
		}
	}

	img := a.Find("img").First()
	for _, attr := range []string{"alt", "title"} {
		value, exists := img.Attr(attr)
		if exists && normalizeText(value) != "" {
			return normalizeText(value)
		}
	}

	return ""
}

func shouldReplaceTitle(current string, next string) bool {
	if next == "" || looksLikeDuration(next) {
		return false
	}

	if current == "" || looksLikeDuration(current) {
		return true
	}

	return len([]rune(next)) > len([]rune(current))
}

func extractDuration(a *goquery.Selection) string {
	text := normalizeText(a.Text())
	fields := strings.Fields(text)
	for _, field := range fields {
		if looksLikeDuration(field) {
			return field
		}
	}

	cardText := normalizeText(a.Parent().Text())
	for _, field := range strings.Fields(cardText) {
		if looksLikeDuration(field) {
			return field
		}
	}

	return ""
}

func looksLikeDuration(value string) bool {
	parts := strings.Split(value, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return false
	}

	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, r := range part {
			if r < '0' || r > '9' {
				return false
			}
		}
	}

	return true
}

func normalizeText(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func extractM3U8URLs(body string) []string {
	candidates := []string{body}
	candidates = append(candidates, unpackJavascriptStrings(body)...)

	re := regexp.MustCompile(`https?://[^'"\\\s<>]+\.m3u8`)
	urls := []string{}
	for _, candidate := range candidates {
		for _, match := range re.FindAllString(candidate, -1) {
			urls = append(urls, strings.ReplaceAll(match, `\/`, `/`))
		}
	}

	return uniqueStrings(urls)
}

func FetchM3U8Playlists(referer string, urls []string) []M3U8Playlist {
	playlists := []M3U8Playlist{}
	seen := map[string]bool{}
	sem := make(chan struct{}, maxM3U8Concurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup

	var enqueue func(string)
	enqueue = func(m3u8URL string) {
		m3u8URL = strings.TrimSpace(m3u8URL)
		if m3u8URL == "" {
			return
		}

		mu.Lock()
		if seen[m3u8URL] {
			mu.Unlock()
			return
		}
		seen[m3u8URL] = true
		mu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			content, err := FetchM3U8(m3u8URL, referer)
			if err != nil {
				log.Printf("m3u8 fetch failed: %s: %v", m3u8URL, err)
				return
			}
			content = strings.TrimSpace(content)
			if content == "" {
				log.Printf("m3u8 fetch empty: %s", m3u8URL)
				return
			}

			mu.Lock()
			playlists = append(playlists, M3U8Playlist{
				URL:     m3u8URL,
				Content: content,
			})
			mu.Unlock()

			for _, nestedURL := range extractNestedM3U8URLs(m3u8URL, content) {
				enqueue(nestedURL)
			}
		}()
	}

	for _, m3u8URL := range uniqueStrings(urls) {
		enqueue(m3u8URL)
	}
	wg.Wait()

	return playlists
}

func extractNestedM3U8URLs(baseURL string, content string) []string {
	urls := []string{}
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, ".m3u8") {
			continue
		}
		urls = append(urls, absoluteURL(baseURL, line))
	}

	return uniqueStrings(urls)
}

func unpackJavascriptStrings(body string) []string {
	re := regexp.MustCompile(`(?s)eval\(function\(p,a,c,k,e,d\).*?\('((?:\\'|[^'])*)',(\d+),(\d+),'([^']*)'\.split\('\|'\)`)
	matches := re.FindAllStringSubmatch(body, -1)
	result := []string{}

	for _, match := range matches {
		if len(match) != 5 {
			continue
		}

		payload := strings.ReplaceAll(match[1], `\'`, `'`)
		radix, err := strconv.Atoi(match[2])
		if err != nil || radix < 2 || radix > 36 {
			continue
		}
		words := strings.Split(match[4], "|")

		tokenRe := regexp.MustCompile(`\b[0-9a-zA-Z]+\b`)
		unpacked := tokenRe.ReplaceAllStringFunc(payload, func(token string) string {
			index, err := strconv.ParseInt(token, radix, 64)
			if err != nil || index < 0 || int(index) >= len(words) || words[index] == "" {
				return token
			}
			return words[index]
		})

		result = append(result, unpacked)
	}

	return result
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	result := []string{}
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}

	return result
}

func writeM3U8File(videoCode string, m3u8URL string, content string) (string, error) {
	if videoCode == "" {
		videoCode = "unknown"
	}

	dir := filepath.Join("m3u8", sanitizeFilename(videoCode))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}

	parsedURL, err := url.Parse(m3u8URL)
	if err != nil {
		return "", err
	}
	filename := sanitizeFilename(parsedURL.Host + "_" + strings.Trim(parsedURL.Path, "/"))
	if filename == "" || filename == "." || filename == "/" {
		filename = "playlist.m3u8"
	}
	if !strings.HasSuffix(strings.ToLower(filename), ".m3u8") {
		filename += ".m3u8"
	}

	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, []byte(content+"\n"), 0644); err != nil {
		return "", err
	}

	return path, nil
}

func sanitizeFilename(value string) string {
	value = strings.TrimSpace(value)
	re := regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
	value = re.ReplaceAllString(value, "_")
	return strings.Trim(value, "._-")
}

func saveVideo(db *sql.DB, video Video) bool {
	if video.Code == "" {
		video.Code = extractCodeFromURL(video.URL)
	}
	now := time.Now().Format(time.RFC3339)
	var current Video
	err := db.QueryRow(
		"SELECT COALESCE(title, ''), COALESCE(duration, ''), COALESCE(section, '') FROM videos WHERE code = $1 LIMIT 1",
		video.Code,
	).Scan(&current.Title, &current.Duration, &current.Section)

	if err == nil {
		current.URL = video.URL
		if !shouldUpdateVideo(current, video) {
			updateVideoLastSeen(db, video.Code, now)
			log.Printf("skip duplicate video: %s", video.URL)
			return false
		}

		if !shouldReplaceTitle(current.Title, video.Title) {
			video.Title = current.Title
		}
		if video.Duration == "" {
			video.Duration = current.Duration
		}
		if video.Section == "" {
			video.Section = current.Section
		}

		mustExec(db, `
			UPDATE videos
			SET url = $1, title = $2, duration = $3, section = $4,
				last_seen_at = $5, updated_at = $6
			WHERE code = $7
		`, video.URL, video.Title, video.Duration, video.Section, now, now, video.Code)

		log.Printf("video updated: [%s] %s %s", video.Section, video.Duration, video.Title)
		return false
	}
	if err != sql.ErrNoRows {
		log.Fatalf("query video failed: %v", err)
	}

	mustExec(db, `
		INSERT INTO
			videos (
				code, url, title, duration, section, detail_status, stream_status,
				first_seen_at, last_seen_at, created_at, updated_at
			)
		VALUES
			($1, $2, $3, $4, $5, 'pending', 'pending', $6, $7, $8, $9)
	`, video.Code, video.URL, video.Title, video.Duration, video.Section, now, now, now, now)

	log.Printf("video saved: [%s] %s %s", video.Section, video.Duration, video.Title)
	return true
}

func shouldUpdateVideo(current Video, next Video) bool {
	return shouldReplaceTitle(current.Title, next.Title) ||
		(current.Duration == "" && next.Duration != "") ||
		(current.Section == "" && next.Section != "")
}

func updateVideoLastSeen(db *sql.DB, code string, seenAt string) {
	if code == "" {
		return
	}

	mustExec(db, `
		UPDATE videos
		SET last_seen_at = $1, updated_at = $2
		WHERE code = $3
	`, seenAt, seenAt, code)
}

func saveVideoDetail(db *sql.DB, detail VideoDetail, listVideo Video) {
	if detail.Code == "" {
		detail.Code = listVideo.Code
	}
	if detail.Code == "" {
		detail.Code = extractCodeFromURL(detail.URL)
	}
	if detail.Title == "" {
		detail.Title = listVideo.Title
	}
	duration := listVideo.Duration
	section := listVideo.Section
	now := time.Now().Format(time.RFC3339)
	var exists int
	err := db.QueryRow("SELECT 1 FROM videos WHERE code = $1 LIMIT 1", detail.Code).Scan(&exists)

	if err == nil {
		mustExec(db, `
			UPDATE videos
			SET url = $1, title = $2, duration = $3, section = $4, description = $5,
				cover_url = $6, release_date = $7, duration_seconds = $8, actors = $9,
				genres = $10, maker = $11, director = $12, tags = $13,
				detail_status = 'done', last_crawled_at = $14, updated_at = $15
			WHERE code = $16
		`, detail.URL, detail.Title, duration, section, detail.Description,
			detail.CoverURL, detail.ReleaseDate, detail.DurationSeconds, detail.Actors,
			detail.Genres, detail.Maker, detail.Director, detail.Tags, now, now, detail.Code)
		return
	}
	if err != sql.ErrNoRows {
		log.Fatalf("query video detail target failed: %v", err)
	}

	mustExec(db, `
		INSERT INTO videos (
			code, url, title, duration, section, description, cover_url,
			release_date, duration_seconds, actors, genres, maker, director,
			tags, detail_status, stream_status, first_seen_at, last_seen_at,
			last_crawled_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
			'done', 'pending', $15, $16, $17, $18, $19)
	`, detail.Code, detail.URL, detail.Title, duration, section, detail.Description,
		detail.CoverURL, detail.ReleaseDate, detail.DurationSeconds, detail.Actors,
		detail.Genres, detail.Maker, detail.Director, detail.Tags, now, now, now, now, now)
}

func saveStream(db *sql.DB, videoCode string, videoURL string, streamType string, m3u8URL string, content string) {
	path, err := writeM3U8File(videoCode, m3u8URL, content)
	if err != nil {
		log.Printf("m3u8 file write failed: %s: %v", m3u8URL, err)
		return
	}

	var exists int
	err = db.QueryRow("SELECT 1 FROM streams WHERE m3u8_url = $1 LIMIT 1", m3u8URL).Scan(&exists)
	if err == nil {
		return
	}
	if err != sql.ErrNoRows {
		log.Fatalf("query stream failed: %v", err)
	}

	now := time.Now().Format(time.RFC3339)
	mustExec(db, `
		INSERT INTO streams (
			video_code, video_url, stream_type, m3u8_url, m3u8_path, fetched_at, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, videoCode, videoURL, streamType, m3u8URL, path, now, now)
}

func updateVideoDetailStatus(db *sql.DB, code string, status string) {
	if code == "" {
		return
	}

	mustExec(db, `
		UPDATE videos
		SET detail_status = $1, updated_at = $2
		WHERE code = $3
	`, status, time.Now().Format(time.RFC3339), code)
}

func updateVideoStreamStatus(db *sql.DB, code string, status string) {
	if code == "" {
		return
	}

	mustExec(db, `
		UPDATE videos
		SET stream_status = $1, updated_at = $2
		WHERE code = $3
	`, status, time.Now().Format(time.RFC3339), code)
}

func enqueueJob(db *sql.DB, jobType string, targetURL string, videoCode string, priority int, resetDone bool) {
	targetURL = canonicalCrawlURL(targetURL)
	now := time.Now().Format(time.RFC3339)
	scheduledAt := now
	var id int
	var status string
	err := db.QueryRow(
		"SELECT id, status FROM crawl_jobs WHERE job_type = $1 AND url = $2 LIMIT 1",
		jobType, targetURL,
	).Scan(&id, &status)
	if err == nil {
		if resetDone {
			mustExec(db, `
				UPDATE crawl_jobs
				SET video_code = $1, status = 'pending', priority = $2, retry_count = 0,
					max_retries = $3, last_error = '', scheduled_at = $4,
					started_at = NULL, finished_at = NULL, updated_at = $5
				WHERE id = $6
			`, videoCode, priority, defaultMaxRetries, scheduledAt, now, id)
		}
		return
	}
	if err != sql.ErrNoRows {
		log.Fatalf("query crawl job failed: %v", err)
	}

	mustExec(db, `
		INSERT INTO crawl_jobs (
			job_type, url, video_code, status, priority, retry_count, max_retries,
			scheduled_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, 'pending', $4, 0, $5, $6, $7, $8)
	`, jobType, targetURL, videoCode, priority, defaultMaxRetries, scheduledAt, now, now)
}

func loadPendingJobs(db *sql.DB, jobType string, limit int) []CrawlJob {
	now := time.Now().Format(time.RFC3339)
	rows, err := db.Query(
		`
		SELECT id, job_type, url, COALESCE(video_code, ''), priority, retry_count, max_retries, scheduled_at
		FROM crawl_jobs
		WHERE job_type = $1
			AND status IN ('pending', 'failed')
			AND retry_count < max_retries
			AND (scheduled_at = '' OR scheduled_at <= $2)
		ORDER BY priority DESC, id ASC
		LIMIT $3
		`,
		jobType, now, limit,
	)
	if err != nil {
		log.Fatalf("load pending jobs failed: %v", err)
	}
	defer rows.Close()

	jobs := []CrawlJob{}
	for rows.Next() {
		var job CrawlJob
		if err := rows.Scan(
			&job.ID, &job.JobType, &job.URL, &job.VideoCode,
			&job.Priority, &job.RetryCount, &job.MaxRetries, &job.ScheduledAt,
		); err != nil {
			log.Fatalf("scan pending job failed: %v", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("read pending jobs failed: %v", err)
	}

	return jobs
}

func markJobRunning(db *sql.DB, job CrawlJob) {
	now := time.Now().Format(time.RFC3339)
	mustExec(db, `
		UPDATE crawl_jobs
		SET status = 'running', started_at = $1, updated_at = $2
		WHERE id = $3
	`, now, now, job.ID)
}

func markJobDone(db *sql.DB, job CrawlJob) {
	now := time.Now().Format(time.RFC3339)
	mustExec(db, `
		UPDATE crawl_jobs
		SET status = 'done', last_error = '', finished_at = $1, updated_at = $2
		WHERE id = $3
	`, now, now, job.ID)
}

func markJobFailed(db *sql.DB, job CrawlJob, err error) {
	now := time.Now().Format(time.RFC3339)
	mustExec(db, `
		UPDATE crawl_jobs
		SET status = 'failed', retry_count = retry_count + 1,
			last_error = $1, finished_at = $2, updated_at = $3
		WHERE id = $4
	`, err.Error(), now, now, job.ID)
}

func loadVideoByCode(db *sql.DB, code string) (Video, bool) {
	var video Video
	err := db.QueryRow(
		"SELECT code, url, COALESCE(title, ''), COALESCE(duration, ''), COALESCE(section, '') FROM videos WHERE code = $1 LIMIT 1",
		code,
	).Scan(&video.Code, &video.URL, &video.Title, &video.Duration, &video.Section)
	if err == sql.ErrNoRows {
		return Video{}, false
	}
	if err != nil {
		log.Fatalf("load video failed: %v", err)
	}

	return video, true
}

func runVideoListJobs(db *sql.DB, limit int) int {
	processed := 0
	unlimited := limit <= 0
	batchSize := 10
	for {
		if !unlimited && processed >= limit {
			break
		}
		n := batchSize
		if !unlimited && limit-processed < n {
			n = limit - processed
		}
		jobs := loadPendingJobs(db, "list", n)
		if len(jobs) == 0 {
			break
		}

		for _, job := range jobs {
			if !unlimited && processed >= limit {
				break
			}

			markJobRunning(db, job)
			result, err := CrawlVideoListPage(db, job.URL)
			processed++
				statusMu.Lock()
				crawlStat.ListJobsProcessed = processed
				crawlStat.CurrentJob = job.URL
				statusMu.Unlock()
			if err != nil {
				markJobFailed(db, job, err)
				statusMu.Lock()
				crawlStat.Errors++
				statusMu.Unlock()
				log.Printf("video list job failed: %s: %v", job.URL, err)
				continue
			}

			for _, listURL := range result.ListURLs {
				enqueueJob(db, "list", listURL, "", 20, false)
			}
			if result.NextPageURL != "" {
				enqueueJob(db, "list", result.NextPageURL, "", 20, false)
			}
			for _, video := range result.Videos {
				enqueueJob(db, "detail", video.URL, video.Code, 10, false)
			}
			markJobDone(db, job)
			log.Printf(
				"video list job done: %s, list jobs discovered: %d, video detail jobs queued: %d",
				job.URL,
				len(result.ListURLs),
				len(result.Videos),
			)
		}
	}
	log.Printf("video list jobs processed: %d", processed)
	return processed
}

func runVideoDetailJobs(db *sql.DB, limit int) {
	unlimited := limit <= 0
	batchSize := 50
	processed := 0
	for {
		if !unlimited && processed >= limit {
			break
		}
		n := batchSize
		if !unlimited && limit-processed < n {
			n = limit - processed
		}
		jobs := loadPendingJobs(db, "detail", n)
		if len(jobs) == 0 {
			break
		}

		for _, job := range jobs {
			if !unlimited && processed >= limit {
				statusMu.Lock()
				crawlStat.DetailJobsProcessed = processed
				crawlStat.CurrentJob = job.URL
				statusMu.Unlock()
				break
			}
			processed++

			video, ok := loadVideoByCode(db, job.VideoCode)
			if !ok {
				video = Video{
					Code: job.VideoCode,
					URL:  job.URL,
				}
			}
			if video.URL == "" {
				video.URL = job.URL
			}

			markJobRunning(db, job)
			if err := CrawlVideoDetailPage(db, video); err != nil {
				statusMu.Lock()
				crawlStat.Errors++
				statusMu.Unlock()
				updateVideoDetailStatus(db, video.Code, "failed")
				updateVideoStreamStatus(db, video.Code, "failed")
				markJobFailed(db, job, err)
				log.Printf("video detail job failed: %s: %v", job.URL, err)
				continue
			}

			markJobDone(db, job)
		}
	}
	log.Printf("video detail jobs processed: %d", processed)
}

func RunCrawlerJobs(db *sql.DB) {
	resetRunningJobs(db)
	seedCrawlerJobs(db)
	totalLimit := jobLimit()
	requestedDetailLimit := detailLimit()

	if totalLimit > 0 {
		listLimit := totalLimit
		detailLimit := 0
		if requestedDetailLimit == 0 {
			listLimit = totalLimit / 2
			detailLimit = totalLimit
		} else if requestedDetailLimit < totalLimit {
			listLimit = totalLimit - requestedDetailLimit
			detailLimit = requestedDetailLimit
		} else {
			listLimit = 0
			detailLimit = requestedDetailLimit
		}

		listJobs := runVideoListJobs(db, listLimit)
		remaining := totalLimit - listJobs
		if detailLimit > remaining {
			detailLimit = remaining
		}
		runVideoDetailJobs(db, detailLimit)
	} else {
		// Unlimited mode: interleave list and detail processing
		for {
			listDone := runVideoListJobs(db, 10)
			var pendingDetail int
			db.QueryRow("SELECT count(*) FROM crawl_jobs WHERE job_type='detail' AND status IN ('pending','failed') AND retry_count < max_retries").Scan(&pendingDetail)
			if pendingDetail > 0 {
				batch := pendingDetail
				if batch > 30 {
					batch = 30
				}
				runVideoDetailJobs(db, batch)
			}

			if listDone == 0 && pendingDetail == 0 {
				var anyPending int
				db.QueryRow("SELECT count(*) FROM crawl_jobs WHERE status IN ('pending','failed') AND retry_count < max_retries").Scan(&anyPending)
				if anyPending == 0 {
					break
				}
			}
		}
	}
}

func seedCrawlerJobs(db *sql.DB) {
	switch crawlMode() {
	case "init":
		seedInitialJobs(db)
	default:
		seedIncrementalJobs(db)
	}
}

func seedInitialJobs(db *sql.DB) {
	for _, seedURL := range initialSeedURLs() {
		enqueueJob(db, "list", seedURL, "", 100, false)
	}
}

func seedIncrementalJobs(db *sql.DB) {
	for _, seedURL := range incrementalSeedURLs() {
		enqueueJob(db, "list", seedURL, "", 100, true)
	}
}

func initialSeedURLs() []string {
	return []string{
		startURL,
		"https://missav.ws/cn/new",
		"https://missav.ws/cn/release",
		"https://missav.ws/cn/chinese-subtitle",
		"https://missav.ws/cn/uncensored-leak",
		"https://missav.ws/cn/today-hot",
		"https://missav.ws/cn/weekly-hot",
		"https://missav.ws/cn/monthly-hot",
		"https://missav.ws/cn/genres",
		"https://missav.ws/cn/makers",
		"https://missav.ws/cn/actresses",
	}
}

func incrementalSeedURLs() []string {
	return []string{
		startURL,
		"https://missav.ws/cn/new",
		"https://missav.ws/cn/release",
		"https://missav.ws/cn/chinese-subtitle",
	}
}

func resetRunningJobs(db *sql.DB) {
	mustExec(db, `
		UPDATE crawl_jobs
		SET status = 'pending', updated_at = $1
		WHERE status = 'running'
	`, time.Now().Format(time.RFC3339))
}

func updateCrawlStatus(currentURL string) {
	statusMu.Lock()
	defer statusMu.Unlock()
	crawlStat.CurrentJob = currentURL
}

func crawlInterval() time.Duration {
	val := strings.TrimSpace(os.Getenv("CRAWL_INTERVAL"))
	if val == "" {
		return 6 * time.Hour
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return 6 * time.Hour
	}
	return d
}

func main() {
	godotenv.Load()

	db := ConnectDB()
	defer db.Close()

	startProfilingServer()

	go func() {
		for range time.NewTicker(30 * time.Second).C {
			var listPending, detailPending, totalVideos, doneVideos int
			db.QueryRow("SELECT count(*) FROM crawl_jobs WHERE job_type='list' AND status='pending'").Scan(&listPending)
			db.QueryRow("SELECT count(*) FROM crawl_jobs WHERE job_type='detail' AND status='pending'").Scan(&detailPending)
			db.QueryRow("SELECT count(*) FROM videos").Scan(&totalVideos)
			db.QueryRow("SELECT count(*) FROM videos WHERE detail_status='done'").Scan(&doneVideos)
			statusMu.Lock()
			crawlStat.ListJobsPending = listPending
			crawlStat.DetailJobsPending = detailPending
			crawlStat.VideosFound = totalVideos
			crawlStat.VideosDone = doneVideos
			s := crawlStat
			statusMu.Unlock()
			log.Printf("\n=== Status ===\n%s", s.String())
		}
	}()

	interval := crawlInterval()
	log.Printf("start missav crawler, interval: %v", interval)
	for {
		RunCrawlerJobs(db)
		log.Printf("crawl pass done, next in %v", interval)
		time.Sleep(interval)
	}
}
