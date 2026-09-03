package proto

import "testing"

func FuzzDecodeFrame(f *testing.F) {
	valid, err := EncodeFrame(NewReq(1, PeerControl, OpWSGet, WSGetReq{ID: "ws_seed"}))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte{0xff, 0x00})
	f.Add(MustMarshal(map[string]any{"v": Version + 1, "t": KindReq, "body": []byte{0xff}}))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4<<20 {
			t.Skip()
		}
		frame, err := DecodeFrame(data)
		if err != nil {
			return
		}
		var body map[string]any
		_ = frame.Decode(&body)
	})
}
