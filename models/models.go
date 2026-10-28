package models

import "encoding/xml"

type Evento struct {
	SegStart int
	SegEnd   int
	Name     string
	Desc     string
	Color    []int
}

// Struct para desserializar a ação contida dentro do JSON
type ActionDetail struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Color       []int  `json:"color"`
}

// Struct para desserializar o payload completo do EventStream
type MPDEventPayload struct {
	SegStart int          `json:"seg_start"`
	SegEnd   int          `json:"seg_end"`
	Action   ActionDetail `json:"action"`
}

// Struct para o envio padronizado no Broker MQTT
type EventPayload struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Color       []int  `json:"color"`
	SegStart    int    `json:"seg_start"`
	SegEnd      int    `json:"seg_end"`
}

// Estruturas XML para o parser do MPD
type MPD struct {
	XMLName xml.Name `xml:"MPD"`
	Period  Period   `xml:"Period"`
}

type Period struct {
	EventStream EventStream `xml:"EventStream"`
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
