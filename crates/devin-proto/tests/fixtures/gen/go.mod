module devinprotofixtures

go 1.27.1

require (
	google.golang.org/protobuf v1.36.11
	local/devinproto v0.0.0
)

replace local/devinproto => ../../../../../../devin2api/outputs/devin-proto-go
