package models

import "encoding/xml"

type Evento struct {
	SegStart int
	SegEnd   int
	Name     string
	Desc     string
	Color    []int
}

type ActionDetail struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Color       []int  `json:"color"`
}

type MPDEventPayload struct {
	SegStart int          `json:"seg_start"`
	SegEnd   int          `json:"seg_end"`
	Action   ActionDetail `json:"action"`
}

type EventPayload struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Color       []int  `json:"color"`
	SegStart    int    `json:"seg_start"`
	SegEnd      int    `json:"seg_end"`
}

type MPD struct {
	XMLName xml.Name `xml:"MPD"`

	Type        string `xml:"type,attr"`
	PublishTime string `xml:"publishTime,attr"`

	Period Period `xml:"Period"`
}

type Period struct {
	EventStream    EventStream     `xml:"EventStream"`
	AdaptationSets []AdaptationSet `xml:"AdaptationSet"`
}

type EventStream struct {
	SchemeID    string     `xml:"schemeIdUri,attr"`
	Timescale   int        `xml:"timescale,attr"`
	MaxSegment  int        `xml:"maxSegment,attr"`
	LastSegment int        `xml:"lastSegment,attr"`
	EventItems  []MPDEvent `xml:"Event"`
}

type MPDEvent struct {
	PresentationTime int64  `xml:"presentationTime,attr"`
	Duration         int64  `xml:"duration,attr"`
	ID               string `xml:"id,attr"`
	SegStart         int    `xml:"segStart,attr"`
	SegEnd           int    `xml:"segEnd,attr"`
	Data             string `xml:",chardata"`
}

type AdaptationSet struct {
	ContentType     string           `xml:"contentType,attr"`
	Representations []Representation `xml:"Representation"`
	SegmentTemplate SegmentTemplate  `xml:"SegmentTemplate"`
}

type Representation struct {
	ID              string          `xml:"id,attr"`
	SegmentTemplate SegmentTemplate `xml:"SegmentTemplate"`
}

type SegmentTemplate struct {
	StartNumber     int             `xml:"startNumber,attr"`
	Timescale       int64           `xml:"timescale,attr"`
	Duration        int64           `xml:"duration,attr"`
	SegmentTimeline SegmentTimeline `xml:"SegmentTimeline"`
}

type SegmentTimeline struct {
	Segments []Segment `xml:"S"`
}

type Segment struct {
	Time     int64 `xml:"t,attr"`
	Duration int64 `xml:"d,attr"`
	Repeat   int   `xml:"r,attr"`
}
