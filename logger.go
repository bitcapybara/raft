package raft

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
	Trace(msg string)
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}
