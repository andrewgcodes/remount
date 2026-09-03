module example.com/remount-public-consumer

go 1.27.1

require remount.dev/remount v0.0.0

require (
	github.com/coder/websocket v1.8.15 // indirect
	github.com/fxamacker/cbor/v2 v2.9.3 // indirect
	github.com/x448/float16 v0.8.4 // indirect
)

replace remount.dev/remount => ../..
