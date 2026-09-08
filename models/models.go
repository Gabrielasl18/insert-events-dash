package models

import "encoding/xml"

type Evento struct {
	SegStart int
	SegEnd   int
	Name     string
	Desc     string
	Color    []int
}

type Action struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Color       []int  `json:"color"`
}

type EventJSON struct {
	SegStart int    `json:"seg_start"`
	SegEnd   int    `json:"seg_end"`
	Action   Action `json:"action"`
}

type EventsFile struct {
	LastSeg int         `json:"last_seg"`
	Actions []EventJSON `json:"actions"`
}

type Event struct {
	XMLName          xml.Name `xml:"Event"`
	PresentationTime int64    `xml:"presentationTime,attr"`
	Duration         int64    `xml:"duration,attr"`
	ID               string   `xml:"id,attr"`
	Data             string   `xml:",chardata"`
}

type EventStream struct {
	XMLName    xml.Name `xml:"EventStream"`
	SchemeID   string   `xml:"schemeIdUri,attr"`
	Value      string   `xml:"value,attr"`
	Timescale  int64    `xml:"timescale,attr"`
	EventItems []Event  `xml:"Event"`
}
