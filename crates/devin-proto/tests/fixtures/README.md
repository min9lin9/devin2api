# Wire-compat fixtures

Go-produced protobuf fixtures proving the generated Rust bindings are
wire-compatible with the Go reference's protobuf stack.

- `<name>.bin` — `proto.Marshal` output from the Go generated module
  (`devin2api/outputs/devin-proto-go`, protoc-gen-go v1.36.11).
- `<name>.json` — `protojson.Marshal` output for the same message.
- `manifest.json` — fixture list with fully-qualified type names and a
  `deterministic_bytes` flag (false when the message tree contains map
  fields, whose wire order is unspecified).
- `response_unknown_*` — hand-crafted wire payloads (unknown tag 90 varint;
  `stop_reason` = unknown enum 999) decoded by Go to produce the oracle JSON.

## Regenerating

`gen/` is a Go oracle tool (development only, never shipped). It resolves
`local/devinproto` via a relative `replace` to the Go reference checkout:

    cd gen && go run . gen ..

## Verifying Rust output against Go

    scripts/verify-wire-fixtures.sh

emits Rust re-encodings + serde proto-JSON into a temp dir and runs
`gen verify` to decode them with the Go stack and compare protojson.
