package hierarchy

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/ONSdigital/dp-dd-csv-transformer/config"
)

type Hierarchy struct {
	ID       string                     `json:"id"`
	Name     string                     `json:"name"`
	Type     string                     `json:"type"`
	Options  []*HierarchyEntry          `json:"options,omitempty"`
	EntryMap map[string]*HierarchyEntry `json:"-"`
}

type HierarchyEntry struct {
	Code      string              `json:"code"`
	Name      string              `json:"name"`
	LevelType *HierarchyLevelType `json:"levelType,omitempty"`
	HasData   bool                `json:"hasData,omitempty"` // used only for sparsely populated hierarchy
	Options   []*HierarchyEntry   `json:"options,omitempty"`
}

type HierarchyLevelType struct {
	Code  string `json:"code"`
	Name  string `json:"name"`
	Level int    `json:"level"`
}

// HierarchyClient defines the HierarchyClient interface.
type HierarchyClient interface {
	GetHierarchy(hierarchyId string) (*Hierarchy, error)
	GetHierarchyValue(hierarchyId string, entryCode string) (string, error)
}

type hierarchyClient struct {
	endpoint string
	cache    map[string]*Hierarchy
}

// NewHierarchyClient Create a new HierarchyClient
func NewHierarchyClient() HierarchyClient {
	var client hierarchyClient
	client.endpoint = config.HierarchyEndpoint
	client.cache = make(map[string]*Hierarchy)
	return client
}

// GetHierarchy calls the hierarchy endpoint to get the requested hierarchy, constructing a map of all entries by code.
// A HierarchyClient stores the downloaded hierarchies in memory, so should be disposed of when no longer needed.
func (hc hierarchyClient) GetHierarchy(hierarchyId string) (*Hierarchy, error) {
	// do we have this hierarchy in the cache?
	if cached, ok := hc.cache[hierarchyId]; ok {
		return cached, nil
	}
	// get the hierarchy:
	endpoint := strings.Replace(hc.endpoint, config.HIERACHY_ID_PLACEHOLDER, hierarchyId, -1)
	res, err := http.Get(endpoint)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	var h Hierarchy
	err = json.Unmarshal(body, &h)
	if err != nil {
		return nil, err
	}
	h.EntryMap = make(map[string]*HierarchyEntry)
	// map it
	mapHierarchyEntries(h.EntryMap, h.Options)
	// cache it for future requests
	hc.cache[hierarchyId] = &h
	return &h, nil
}

func mapHierarchyEntries(entryMap map[string]*HierarchyEntry, entries []*HierarchyEntry) {
	for _, entry := range entries {
		entryMap[entry.Code] = entry
		mapHierarchyEntries(entryMap, entry.Options)
	}
}

// getHierarchyValue
func (hc hierarchyClient) GetHierarchyValue(hierarchyId string, entryCode string) (string, error) {
	h, err := hc.GetHierarchy(hierarchyId)
	if err != nil {
		return "", err
	}
	entry := h.EntryMap[entryCode]
	if entry == nil {
		return "", errors.New("No entry found with code " + entryCode + " in hierarchy " + hierarchyId)
	}
	return entry.Name, nil
}
