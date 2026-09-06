package computer

import (
	"context"

	"remount.dev/remount/internal/proto"
)

// keyDef is one entry of the named-key table. CDP wants the logical key, the
// physical code and the legacy virtual key code; sending only one of them
// makes some pages see nothing at all.
type keyDef struct {
	key  string
	code string
	vk   int
	text string
}

// namedKeys is the set of keys computer.input can name. Anything not here is
// rejected rather than guessed: a silently mistyped key is worse than an error.
var namedKeys = map[string]keyDef{
	"Enter":      {key: "Enter", code: "Enter", vk: 13, text: "\r"},
	"Tab":        {key: "Tab", code: "Tab", vk: 9, text: "\t"},
	"Backspace":  {key: "Backspace", code: "Backspace", vk: 8},
	"Delete":     {key: "Delete", code: "Delete", vk: 46},
	"Escape":     {key: "Escape", code: "Escape", vk: 27},
	"Space":      {key: " ", code: "Space", vk: 32, text: " "},
	"ArrowUp":    {key: "ArrowUp", code: "ArrowUp", vk: 38},
	"ArrowDown":  {key: "ArrowDown", code: "ArrowDown", vk: 40},
	"ArrowLeft":  {key: "ArrowLeft", code: "ArrowLeft", vk: 37},
	"ArrowRight": {key: "ArrowRight", code: "ArrowRight", vk: 39},
	"Home":       {key: "Home", code: "Home", vk: 36},
	"End":        {key: "End", code: "End", vk: 35},
	"PageUp":     {key: "PageUp", code: "PageUp", vk: 33},
	"PageDown":   {key: "PageDown", code: "PageDown", vk: 34},
}

// KeyNames lists every key computer.input accepts, for documentation and for
// an SDK that wants to validate before a round trip.
func KeyNames() []string {
	names := make([]string, 0, len(namedKeys))
	for name := range namedKeys {
		names = append(names, name)
	}
	return names
}

const (
	buttonLeft   = "left"
	buttonRight  = "right"
	buttonMiddle = "middle"
)

func mouseButton(name string) (string, error) {
	switch name {
	case "", buttonLeft:
		return buttonLeft, nil
	case buttonRight, buttonMiddle:
		return name, nil
	}
	return "", proto.ErrReason(proto.CodeBadRequest, proto.ReasonInputRejected,
		"unknown mouse button %q", name)
}

// Validate checks one action against the viewport before any of a batch is
// applied, so a malformed action cannot leave a batch half-applied.
func Validate(a proto.ComputerAction, v proto.ComputerViewport) error {
	inside := func(label string, x, y int) error {
		if x < 0 || y < 0 || x > v.Width || y > v.Height {
			return proto.ErrReason(proto.CodeBadRequest, proto.ReasonInputRejected,
				"%s (%d,%d) is outside the %dx%d viewport", label, x, y, v.Width, v.Height)
		}
		return nil
	}
	switch a.Kind {
	case proto.ComputerActionClick, proto.ComputerActionMove, proto.ComputerActionScroll:
		if _, err := mouseButton(a.Button); err != nil {
			return err
		}
		return inside("point", a.X, a.Y)
	case proto.ComputerActionDrag:
		if _, err := mouseButton(a.Button); err != nil {
			return err
		}
		if err := inside("origin", a.X, a.Y); err != nil {
			return err
		}
		return inside("destination", a.ToX, a.ToY)
	case proto.ComputerActionType:
		if a.Text == "" {
			return proto.ErrReason(proto.CodeBadRequest, proto.ReasonInputRejected,
				"type action has no text")
		}
		return nil
	case proto.ComputerActionKey:
		if _, ok := namedKeys[a.Key]; !ok {
			return proto.ErrReason(proto.CodeBadRequest, proto.ReasonInputRejected,
				"unknown key %q", a.Key)
		}
		return nil
	}
	return proto.ErrReason(proto.CodeBadRequest, proto.ReasonInputRejected,
		"unknown action kind %q", a.Kind)
}

// Apply dispatches actions in order. Every action is validated first: a batch
// either starts applying valid input or is rejected whole.
func (c *Client) Apply(ctx context.Context, actions []proto.ComputerAction) error {
	for _, a := range actions {
		if err := Validate(a, c.opts.Viewport); err != nil {
			return err
		}
	}
	for _, a := range actions {
		if err := c.apply(ctx, a); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) apply(ctx context.Context, a proto.ComputerAction) error {
	call, cancel := context.WithTimeout(ctx, c.opts.CallTimeout)
	defer cancel()
	switch a.Kind {
	case proto.ComputerActionMove:
		return c.mouse(call, "mouseMoved", a.X, a.Y, "", 0, a.Modifiers)
	case proto.ComputerActionClick:
		button, err := mouseButton(a.Button)
		if err != nil {
			return err
		}
		if err := c.mouse(call, "mouseMoved", a.X, a.Y, "", 0, a.Modifiers); err != nil {
			return err
		}
		if err := c.mouse(call, "mousePressed", a.X, a.Y, button, 1, a.Modifiers); err != nil {
			return err
		}
		return c.mouse(call, "mouseReleased", a.X, a.Y, button, 1, a.Modifiers)
	case proto.ComputerActionDrag:
		button, err := mouseButton(a.Button)
		if err != nil {
			return err
		}
		if err := c.mouse(call, "mouseMoved", a.X, a.Y, "", 0, a.Modifiers); err != nil {
			return err
		}
		if err := c.mouse(call, "mousePressed", a.X, a.Y, button, 1, a.Modifiers); err != nil {
			return err
		}
		if err := c.mouse(call, "mouseMoved", a.ToX, a.ToY, button, 0, a.Modifiers); err != nil {
			return err
		}
		return c.mouse(call, "mouseReleased", a.ToX, a.ToY, button, 1, a.Modifiers)
	case proto.ComputerActionScroll:
		return c.conn.call(call, c.page, "Input.dispatchMouseEvent", map[string]any{
			"type": "mouseWheel", "x": a.X, "y": a.Y,
			"deltaX": a.DX, "deltaY": a.DY, "modifiers": a.Modifiers,
		}, nil)
	case proto.ComputerActionType:
		// insertText, not synthesized keystrokes: it is the only reliable path
		// into ordinary inputs and contenteditable regions alike, and it does
		// not depend on a keyboard layout the workspace may not have.
		return c.conn.call(call, c.page, "Input.insertText", map[string]any{"text": a.Text}, nil)
	case proto.ComputerActionKey:
		def := namedKeys[a.Key]
		down := map[string]any{
			"type": "keyDown", "key": def.key, "code": def.code,
			"windowsVirtualKeyCode": def.vk, "nativeVirtualKeyCode": def.vk,
			"modifiers": a.Modifiers,
		}
		if def.text != "" {
			down["text"] = def.text
		} else {
			down["type"] = "rawKeyDown"
		}
		if err := c.conn.call(call, c.page, "Input.dispatchKeyEvent", down, nil); err != nil {
			return err
		}
		return c.conn.call(call, c.page, "Input.dispatchKeyEvent", map[string]any{
			"type": "keyUp", "key": def.key, "code": def.code,
			"windowsVirtualKeyCode": def.vk, "nativeVirtualKeyCode": def.vk,
			"modifiers": a.Modifiers,
		}, nil)
	}
	return proto.ErrReason(proto.CodeBadRequest, proto.ReasonInputRejected,
		"unknown action kind %q", a.Kind)
}

func (c *Client) mouse(ctx context.Context, kind string, x, y int, button string, clicks, modifiers int) error {
	params := map[string]any{
		"type": kind, "x": x, "y": y, "modifiers": modifiers,
	}
	if button != "" {
		params["button"] = button
	}
	if clicks > 0 {
		params["clickCount"] = clicks
	}
	return c.conn.call(ctx, c.page, "Input.dispatchMouseEvent", params, nil)
}
