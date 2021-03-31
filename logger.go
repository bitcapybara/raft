package raft

import "io"

type LogLevel uint8

const (
	Trace LogLevel = iota
	DEBUG
	INFO
	WARN
	ERROR
	FATAL
)

type Logger interface {
	Output() io.Writer
	SetOutput(w io.Writer)
	Prefix() string
	SetPrefix(p string)
	Level() LogLevel
	SetLevel(v LogLevel)
	Trace(msg string)
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}
