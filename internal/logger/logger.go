package logger

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Logger struct {
	mu  sync.Mutex
	out io.Writer
	f   *os.File
}

func New(path string) (*Logger, error) {
	if path == "" || path == "-" {
		return &Logger{out: os.Stdout}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Logger{out: f, f: f}, nil
}

func (l *Logger) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	return l.f.Close()
}

func (l *Logger) Event(event string, fields map[string]any) {
	if l == nil {
		return
	}
	if fields == nil {
		fields = map[string]any{}
	}
	fields["time"] = time.Now().UTC().Format(time.RFC3339Nano)
	fields["event"] = event
	b, err := json.Marshal(fields)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.out.Write(append(b, '\n'))
}
