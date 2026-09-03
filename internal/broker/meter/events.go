package meter

import (
	"bytes"
)

type eventParser interface {
	feed([]byte, *accumulator, Provider) error
	finish(*accumulator, Provider) error
	release()
}

type lineEvents struct {
	limits     Limits
	sse        bool
	pending    []byte
	data       []byte
	eventBytes int
	events     int
	sawField   bool
	skipLF     bool
}

func newSSEParser(limits Limits) eventParser {
	return &lineEvents{limits: limits, sse: true}
}

func newNDJSONParser(limits Limits) eventParser {
	return &lineEvents{limits: limits}
}

func (p *lineEvents) feed(src []byte, usage *accumulator, provider Provider) error {
	for len(src) > 0 {
		if p.skipLF {
			p.skipLF = false
			if src[0] == '\n' {
				src = src[1:]
				continue
			}
		}
		lineLimit := p.limits.MaxEventBytes
		if p.sse {
			lineLimit -= p.eventBytes
		}
		i := bytes.IndexAny(src, "\r\n")
		if i < 0 {
			if len(src) > lineLimit-len(p.pending) {
				return ErrLimit
			}
			p.pending = append(p.pending, src...)
			return nil
		}
		if i > lineLimit-len(p.pending) {
			return ErrLimit
		}
		line := src[:i]
		if len(p.pending) > 0 {
			p.pending = append(p.pending, line...)
			line = p.pending
		}
		if err := p.line(line, usage, provider); err != nil {
			return err
		}
		p.pending = p.pending[:0]
		terminator := src[i]
		src = src[i+1:]
		if terminator == '\r' {
			if len(src) == 0 {
				p.skipLF = true
			} else if src[0] == '\n' {
				src = src[1:]
			}
		}
	}
	return nil
}

func (p *lineEvents) line(line []byte, usage *accumulator, provider Provider) error {
	if !p.sse {
		if len(bytes.TrimSpace(line)) == 0 {
			return nil
		}
		if err := p.countEvent(len(line)); err != nil {
			return err
		}
		return consumeJSON(line, usage, provider)
	}
	if len(line) == 0 {
		if err := p.countEvent(0); err != nil {
			return err
		}
		return p.dispatch(usage, provider)
	}
	if len(line)+1 > p.limits.MaxEventBytes-p.eventBytes {
		return ErrLimit
	}
	p.eventBytes += len(line) + 1
	p.sawField = true
	if line[0] == ':' {
		return nil
	}
	field, value, found := bytes.Cut(line, []byte{':'})
	if !found {
		value = nil
	} else if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}
	if !bytes.Equal(field, []byte("data")) {
		return nil
	}
	if len(p.data) > 0 {
		p.data = append(p.data, '\n')
	}
	p.data = append(p.data, value...)
	if len(p.data) > p.limits.MaxEventBytes {
		return ErrLimit
	}
	return nil
}

func (p *lineEvents) countEvent(extra int) error {
	if extra > p.limits.MaxEventBytes {
		return ErrLimit
	}
	if p.events >= p.limits.MaxEvents {
		return ErrLimit
	}
	p.events++
	return nil
}

func (p *lineEvents) dispatch(usage *accumulator, provider Provider) error {
	data := p.data
	sawField := p.sawField
	p.data = nil
	p.eventBytes = 0
	p.sawField = false
	if !sawField || len(bytes.TrimSpace(data)) == 0 || bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
		return nil
	}
	return consumeJSON(data, usage, provider)
}

func (p *lineEvents) finish(usage *accumulator, provider Provider) error {
	if len(p.pending) > 0 {
		line := p.pending
		if err := p.line(line, usage, provider); err != nil {
			return err
		}
		p.pending = nil
	}
	if p.sse && p.sawField {
		if err := p.countEvent(0); err != nil {
			return err
		}
		return p.dispatch(usage, provider)
	}
	return nil
}

func (p *lineEvents) release() {
	clear(p.pending)
	clear(p.data)
	p.pending = nil
	p.data = nil
}
