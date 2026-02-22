package glance

import (
	"context"
	"fmt"
	"html"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mmcdole/gofeed"
	"github.com/mmcdole/gofeed/atom"
	gofeedext "github.com/mmcdole/gofeed/extensions"
	"github.com/mmcdole/gofeed/rss"
)

var (
	rssWidgetTemplate                 = mustParseTemplate("rss-list.html", "widget-base.html")
	rssWidgetDetailedListTemplate     = mustParseTemplate("rss-detailed-list.html", "widget-base.html")
	rssWidgetHorizontalCardsTemplate  = mustParseTemplate("rss-horizontal-cards.html", "widget-base.html")
	rssWidgetHorizontalCards2Template = mustParseTemplate("rss-horizontal-cards-2.html", "widget-base.html")
)

var feedParser = gofeed.NewParser()

type rssWidget struct {
	widgetBase       `yaml:",inline"`
	FeedRequests     []rssFeedRequest `yaml:"feeds"`
	Style            string           `yaml:"style"`
	ThumbnailHeight  float64          `yaml:"thumbnail-height"`
	CardHeight       float64          `yaml:"card-height"`
	Limit            int              `yaml:"limit"`
	CollapseAfter    int              `yaml:"collapse-after"`
	SingleLineTitles bool             `yaml:"single-line-titles"`
	PreserveOrder    bool             `yaml:"preserve-order"`

	Items          rssFeedItemList `yaml:"-"`
	NoItemsMessage string          `yaml:"-"`

	cachedFeedsMutex sync.Mutex
	cachedFeeds      map[string]*cachedRSSFeed `yaml:"-"`
}

func (widget *rssWidget) initialize() error {
	widget.withTitle("RSS Feed").withCacheDuration(2 * time.Hour)

	if widget.Limit <= 0 {
		widget.Limit = 25
	}

	if widget.CollapseAfter == 0 || widget.CollapseAfter < -1 {
		widget.CollapseAfter = 5
	}

	if widget.ThumbnailHeight < 0 {
		widget.ThumbnailHeight = 0
	}

	if widget.CardHeight < 0 {
		widget.CardHeight = 0
	}

	if widget.Style == "detailed-list" {
		for i := range widget.FeedRequests {
			widget.FeedRequests[i].IsDetailed = true
		}
	}

	widget.NoItemsMessage = "No items were returned from the feeds."
	widget.cachedFeeds = make(map[string]*cachedRSSFeed)

	return nil
}

func (widget *rssWidget) update(ctx context.Context) {
	items, err := widget.fetchItemsFromFeeds()

	if !widget.canContinueUpdateAfterHandlingErr(err) {
		return
	}

	if !widget.PreserveOrder {
		items.sortByNewest()
	}

	if len(items) > widget.Limit {
		items = items[:widget.Limit]
	}

	widget.Items = items
}

func (widget *rssWidget) Render() template.HTML {
	if widget.Style == "horizontal-cards" {
		return widget.renderTemplate(widget, rssWidgetHorizontalCardsTemplate)
	}

	if widget.Style == "horizontal-cards-2" {
		return widget.renderTemplate(widget, rssWidgetHorizontalCards2Template)
	}

	if widget.Style == "detailed-list" {
		return widget.renderTemplate(widget, rssWidgetDetailedListTemplate)
	}

	return widget.renderTemplate(widget, rssWidgetTemplate)
}

type cachedRSSFeed struct {
	etag         string
	lastModified string
	items        []rssFeedItem
}

type rssFeedItem struct {
	ChannelName string
	ChannelURL  string
	Title       string
	Link        string
	ImageURL    string
	Categories  []string
	Description string
	PublishedAt time.Time
}

type rssFeedRequest struct {
	URL             string              `yaml:"url"`
	Title           string              `yaml:"title"`
	HideCategories  bool                `yaml:"hide-categories"`
	HideDescription bool                `yaml:"hide-description"`
	Limit           int                 `yaml:"limit"`
	ItemLinkPrefix  string              `yaml:"item-link-prefix"`
	Headers         map[string]string   `yaml:"headers"`
	IsDetailed      bool                `yaml:"-"`
	TitleKey        itemChannelPrefList `yaml:"item-channel"`
}

type itemChannelPref struct {
	Key   string   `yaml:"key"`
	Match string   `yaml:"match"`
	Sub   []string `yaml:"sub"`
}

type itemChannelPrefList []itemChannelPref

type rssFeedItemList []rssFeedItem

func (f rssFeedItemList) sortByNewest() rssFeedItemList {
	sort.Slice(f, func(i, j int) bool {
		return f[i].PublishedAt.After(f[j].PublishedAt)
	})

	return f
}

func (widget *rssWidget) fetchItemsFromFeeds() (rssFeedItemList, error) {
	requests := widget.FeedRequests

	job := newJob(widget.fetchItemsFromFeedTask, requests).withWorkers(30)
	feeds, errs, err := workerPoolDo(job)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errNoContent, err)
	}

	failed := 0
	entries := make(rssFeedItemList, 0, len(feeds)*10)
	seen := make(map[string]struct{})

	for i := range feeds {
		if errs[i] != nil {
			failed++
			slog.Error("Failed to get RSS feed", "url", requests[i].URL, "error", errs[i])
			continue
		}

		for _, item := range feeds[i] {
			if _, exists := seen[item.Link]; exists {
				continue
			}
			entries = append(entries, item)
			seen[item.Link] = struct{}{}
		}
	}

	if failed == len(requests) {
		return nil, errNoContent
	}

	if failed > 0 {
		return entries, fmt.Errorf("%w: missing %d RSS feeds", errPartialContent, failed)
	}

	return entries, nil
}

func (widget *rssWidget) fetchItemsFromFeedTask(request rssFeedRequest) ([]rssFeedItem, error) {
	req, err := http.NewRequest("GET", request.URL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("User-Agent", glanceUserAgentString)

	widget.cachedFeedsMutex.Lock()
	cache, isCached := widget.cachedFeeds[request.URL]
	if isCached {
		if cache.etag != "" {
			req.Header.Add("If-None-Match", cache.etag)
		}
		if cache.lastModified != "" {
			req.Header.Add("If-Modified-Since", cache.lastModified)
		}
	}
	widget.cachedFeedsMutex.Unlock()

	for key, value := range request.Headers {
		req.Header.Set(key, value)
	}

	resp, err := defaultHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified && isCached {
		return cache.items, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, request.URL)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if request.TitleKey.contains("source") {
		feedParser.RSSTranslator = newRssTranslator()
		feedParser.AtomTranslator = newAtomTranslator()
	}

	feed, err := feedParser.ParseString(string(body))
	if err != nil {
		return nil, err
	}

	if request.Limit > 0 && len(feed.Items) > request.Limit {
		feed.Items = feed.Items[:request.Limit]
	}

	items := make(rssFeedItemList, 0, len(feed.Items))

	for i := range feed.Items {
		item := feed.Items[i]

		rssItem := rssFeedItem{
			ChannelURL: feed.Link,
		}

		if request.ItemLinkPrefix != "" {
			rssItem.Link = request.ItemLinkPrefix + item.Link
		} else if strings.HasPrefix(item.Link, "http://") || strings.HasPrefix(item.Link, "https://") {
			rssItem.Link = item.Link
		} else {
			parsedUrl, err := url.Parse(feed.Link)
			if err != nil {
				parsedUrl, err = url.Parse(request.URL)
			}

			if err == nil {
				var link string

				if len(item.Link) > 0 && item.Link[0] == '/' {
					link = item.Link
				} else {
					link = "/" + item.Link
				}

				rssItem.Link = parsedUrl.Scheme + "://" + parsedUrl.Host + link
			}
		}

		if item.Title != "" {
			rssItem.Title = html.UnescapeString(item.Title)
		} else {
			rssItem.Title = shortenFeedDescriptionLen(item.Description, 100)
		}

		if request.IsDetailed {
			if !request.HideDescription && item.Description != "" && item.Title != "" {
				rssItem.Description = shortenFeedDescriptionLen(item.Description, 200)
			}

			if !request.HideCategories {
				var categories = make([]string, 0, 6)

				for _, category := range item.Categories {
					if len(categories) == 6 {
						break
					}

					if len(category) == 0 || len(category) > 30 {
						continue
					}

					categories = append(categories, category)
				}

				rssItem.Categories = categories
			}
		}

		if request.TitleKey != nil {
			if title := request.TitleKey.itemChannel(item); title != "" {
				rssItem.ChannelName = title
			}
		}

		if request.Title != "" && rssItem.ChannelName == "" {
			rssItem.ChannelName = request.Title
		} else if rssItem.ChannelName == "" {
			rssItem.ChannelName = feed.Title
		}

		if item.Image != nil {
			rssItem.ImageURL = item.Image.URL
		} else if url := findThumbnailInItemExtensions(item); url != "" {
			rssItem.ImageURL = url
		} else if feed.Image != nil {
			if len(feed.Image.URL) > 0 && feed.Image.URL[0] == '/' {
				rssItem.ImageURL = strings.TrimRight(feed.Link, "/") + feed.Image.URL
			} else {
				rssItem.ImageURL = feed.Image.URL
			}
		}

		if item.PublishedParsed != nil {
			rssItem.PublishedAt = *item.PublishedParsed
		} else {
			rssItem.PublishedAt = time.Now()
		}

		items = append(items, rssItem)
	}

	if resp.Header.Get("ETag") != "" || resp.Header.Get("Last-Modified") != "" {
		widget.cachedFeedsMutex.Lock()
		widget.cachedFeeds[request.URL] = &cachedRSSFeed{
			etag:         resp.Header.Get("ETag"),
			lastModified: resp.Header.Get("Last-Modified"),
			items:        items,
		}
		widget.cachedFeedsMutex.Unlock()
	}

	return items, nil
}

// Custom translators to preserve source tag that is not included in the default gofeed.Item
type rssTranslator struct {
	defaultTranslator *gofeed.DefaultRSSTranslator
}

func newRssTranslator() *rssTranslator {
	t := &rssTranslator{}
	t.defaultTranslator = &gofeed.DefaultRSSTranslator{}
	return t
}

func (ct *rssTranslator) Translate(feed interface{}) (*gofeed.Feed, error) {
	rss, found := feed.(*rss.Feed)
	if !found {
		return nil, fmt.Errorf("Feed did not match expected type of *rss.Feed")
	}

	f, err := ct.defaultTranslator.Translate(rss)
	if err != nil {
		return nil, err
	}

	// keep source info
	for i := range f.Items {
		if f.Items[i].Custom == nil {
			f.Items[i].Custom = make(map[string]string)
		}

		if rss.Items[i].Source != nil && rss.Items[i].Source.Title != "" {
			f.Items[i].Custom["source_title"] = rss.Items[i].Source.Title
		}
		if rss.Items[i].Source != nil && rss.Items[i].Source.URL != "" {
			f.Items[i].Custom["source_url"] = rss.Items[i].Source.URL
		}
	}

	return f, nil
}

type atomTranslator struct {
	defaultTranslator *gofeed.DefaultAtomTranslator
}

func newAtomTranslator() *atomTranslator {
	t := &atomTranslator{}
	t.defaultTranslator = &gofeed.DefaultAtomTranslator{}
	return t
}

func (ct *atomTranslator) Translate(feed interface{}) (*gofeed.Feed, error) {
	atom, found := feed.(*atom.Feed)
	if !found {
		return nil, fmt.Errorf("Feed did not match expected type of *atom.Feed")
	}

	f, err := ct.defaultTranslator.Translate(atom)
	if err != nil {
		return nil, err
	}

	// keep source info
	for i := range f.Items {
		if f.Items[i].Custom == nil {
			f.Items[i].Custom = make(map[string]string)
		}

		if atom.Entries[i].Source != nil && atom.Entries[i].Source.Title != "" {
			f.Items[i].Custom["source_title"] = atom.Entries[i].Source.Title
		}
		if atom.Entries[i].Source != nil && atom.Entries[i].Source.ID != "" {
			f.Items[i].Custom["source_url"] = atom.Entries[i].Source.ID
		}
	}

	return f, nil
}

func (t itemChannelPrefList) contains(key string) bool {
	for _, pref := range t {
		if pref.Key == key {
			return true
		}
	}

	return false
}

func (t itemChannelPrefList) itemChannel(item *gofeed.Item) string {
	channelName := ""

	for _, pref := range t {
		switch pref.Key {
		case "source":
			if sourceTitle, ok := item.Custom["source_channelName"]; ok {
				channelName = sourceTitle
			} else if sourceURL, ok := item.Custom["source_url"]; ok {
				channelName = sourceURL
			}
		case "link":
			if parsedUrl, err := url.Parse(item.Link); err == nil {
				channelName = parsedUrl.Host
			}
		case "author-name":
			if item.Author != nil && item.Author.Name != "" {
				channelName = item.Author.Name
			}
		case "author-email":
			if item.Author != nil && item.Author.Email != "" {
				channelName = item.Author.Email
			}
		case "author-email-username":
			if item.Author != nil && item.Author.Email != "" {
				if at := strings.LastIndex(item.Author.Email, "@"); at != -1 {
					channelName = item.Author.Email[:at]
				}
			}
		case "author-email-domain":
			if item.Author != nil && item.Author.Email != "" {
				if at := strings.LastIndex(item.Author.Email, "@"); at != -1 {
					channelName = item.Author.Email[at+1:]
				}
			}
		}

		if channelName != "" {
			if m, err := regexp.MatchString(pref.Match, channelName); !m || err != nil {
				channelName = ""
				continue
			}

			if len(pref.Sub) > 0 {
				channelName = pref.subItemChannel(channelName)
			}

			break
		}
	}

	return channelName
}

func (t itemChannelPref) subItemChannel(name string) string {
	for _, s := range t.Sub {
		sep := s[0]
		elements := strings.Split(s, string(sep))

		if len(elements) != 3 {
			continue
		}

		if m, err := regexp.MatchString(elements[1], name); m && err == nil {
			name = elements[2]
			break
		}
	}

	return strings.TrimSpace(name)
}

func findThumbnailInItemExtensions(item *gofeed.Item) string {
	media, ok := item.Extensions["media"]

	if !ok {
		return ""
	}

	return recursiveFindThumbnailInExtensions(media)
}

func recursiveFindThumbnailInExtensions(extensions map[string][]gofeedext.Extension) string {
	for _, exts := range extensions {
		for _, ext := range exts {
			if ext.Name == "thumbnail" || ext.Name == "image" {
				if url, ok := ext.Attrs["url"]; ok {
					return url
				}
			}

			if ext.Children != nil {
				if url := recursiveFindThumbnailInExtensions(ext.Children); url != "" {
					return url
				}
			}
		}
	}

	return ""
}

var htmlTagsWithAttributesPattern = regexp.MustCompile(`<\/?[a-zA-Z0-9-]+ *(?:[a-zA-Z-]+=(?:"|').*?(?:"|') ?)* *\/?>`)

func sanitizeFeedDescription(description string) string {
	if description == "" {
		return ""
	}

	description = strings.ReplaceAll(description, "\n", " ")
	description = htmlTagsWithAttributesPattern.ReplaceAllString(description, "")
	description = sequentialWhitespacePattern.ReplaceAllString(description, " ")
	description = strings.TrimSpace(description)
	description = html.UnescapeString(description)

	return description
}

func shortenFeedDescriptionLen(description string, maxLen int) string {
	description, _ = limitStringLength(description, 1000)
	description = sanitizeFeedDescription(description)
	description, limited := limitStringLength(description, maxLen)

	if limited {
		description += "…"
	}

	return description
}
