module task6oracle

go 1.27.1

require (
	connectrpc.com/connect v1.20.0
	golang.org/x/net v0.57.0
	google.golang.org/protobuf v1.36.11
	local/devinproto v0.0.0
)

require golang.org/x/text v0.40.0 // indirect

replace local/devinproto => ../../../../../devin2api/outputs/devin-proto-go
