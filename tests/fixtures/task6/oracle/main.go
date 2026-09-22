// Task-6 transport interop oracle: a real Connect server for
// exa.api_server_pb.ApiServerService built from the same generated Go
// bindings the reference daemon uses (local/devinproto via replace).
//
// It exists to validate the Rust reqwest-backed ClientTransport against a
// genuine connect-go server: unary + server-streaming, HTTP/1.1 and
// HTTP/2-over-TLS, header echo, gzip compression, end-stream metadata
// (trailers), semantic Connect errors, mid-stream truncation, cancellation
// observation and the Seat JSON Connect endpoint.
//
// Modes:
//   -listen 127.0.0.1:PORT   bind address (required)
//   -mode h1|tls|h2c         cleartext HTTP/1.1, TLS (ALPN h2+h1), or h2c
//   -cert-out PATH           (tls mode) write the self-signed PEM cert here
//   -seat-token TOKEN        expected Bearer token on the Seat endpoint
//
// Every Connect handler echoes the request headers it saw into response
// headers prefixed x-seen-* so the Rust side can assert wire parity.
// /stats returns JSON counters (status_calls, chat_calls, cancels).
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	devinproto "local/devinproto"
	"local/devinproto/devinprotoconnect"

	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/proto"
)

var (
	statusCalls atomic.Int64
	chatCalls   atomic.Int64
	cancels     atomic.Int64
	conns       atomic.Int64
	seatToken   = flag.String("seat-token", "qa-seat-token", "expected Bearer token")
)

// echoHeaders copies the wire-visible request headers into response headers
// so the Rust test can assert exactly what the server received.
func echoHeaders(dst, src http.Header) {
	set := func(name, val string) { dst.Set("x-seen-"+name, val) }
	set("authorization", src.Get("Authorization"))
	set("content-type", src.Get("Content-Type"))
	set("connect-protocol-version", src.Get("Connect-Protocol-Version"))
	set("connect-content-encoding", src.Get("Connect-Content-Encoding"))
	set("connect-accept-encoding", src.Get("Connect-Accept-Encoding"))
	set("content-encoding", src.Get("Content-Encoding"))
	set("accept-encoding", src.Get("Accept-Encoding"))
	set("stub-mode", src.Get("X-Stub-Mode"))
	set("connect-timeout-ms", src.Get("Connect-Timeout-Ms"))
	// Distinguish "header absent" from "present but empty": Go's
	// BasicAuthTransport sets User-Agent to "" which net/http omits on the
	// wire entirely.
	_, uaPresent := src["User-Agent"]
	set("user-agent-present", fmt.Sprintf("%v", uaPresent))
	set("user-agent", src.Get("User-Agent"))
}

type svc struct {
	devinprotoconnect.UnimplementedApiServerServiceHandler
}

func (s *svc) GetStatus(
	ctx context.Context,
	req *connect.Request[devinproto.GetStatusRequest],
) (*connect.Response[devinproto.GetStatusResponse], error) {
	statusCalls.Add(1)
	if req.Header().Get("X-Stub-Mode") == "error" {
		return nil, connect.NewError(connect.CodeResourceExhausted,
			fmt.Errorf("stub: rate limited"))
	}
	resp := connect.NewResponse(&devinproto.GetStatusResponse{
		Status: &devinproto.ExaCodeiumCommonPb_IdeStatus{
			Level:   devinproto.ExaCodeiumCommonPb_StatusLevel_ExaCodeiumCommonPb_StatusLevel_STATUS_LEVEL_INFO.Enum(),
			Message: proto.String("stub ok"),
		},
		ShowReviewPrompt: proto.Bool(true),
	})
	echoHeaders(resp.Header(), req.Header())
	return resp, nil
}

func (s *svc) GetChatMessage(
	ctx context.Context,
	req *connect.Request[devinproto.GetChatMessageRequest],
	stream *connect.ServerStream[devinproto.GetChatMessageResponse],
) error {
	chatCalls.Add(1)
	echoHeaders(stream.ResponseHeader(), req.Header())
	stream.ResponseTrailer().Set("x-stub-trailer", "t1")

	switch req.Header().Get("X-Stub-Mode") {
	case "error":
		return connect.NewError(connect.CodeResourceExhausted,
			fmt.Errorf("stub: rate limited"))
	case "hang":
		// Send one frame then hold until the client cancels/drops.
		if err := stream.Send(metaFrame()); err != nil {
			return err
		}
		<-ctx.Done()
		cancels.Add(1)
		return ctx.Err()
	}

	if err := stream.Send(metaFrame()); err != nil {
		return err
	}
	if err := stream.Send(deltaText("stub: hello ")); err != nil {
		return err
	}
	if err := stream.Send(deltaText("world")); err != nil {
		return err
	}
	if err := stream.Send(stopFrame()); err != nil {
		return err
	}
	return nil
}

func metaFrame() *devinproto.GetChatMessageResponse {
	return &devinproto.GetChatMessageResponse{
		MessageId: proto.String("bot-stub"),
		RequestId: proto.String("stub-req"),
		Timestamp: &devinproto.GoogleProtobuf_Timestamp{Seconds: proto.Int64(time.Now().Unix())},
		Usage: &devinproto.ExaCodeiumCommonPb_ModelUsageStats{
			ModelUid: proto.String("swe-2-max"),
		},
	}
}

func deltaText(text string) *devinproto.GetChatMessageResponse {
	return &devinproto.GetChatMessageResponse{DeltaText: proto.String(text)}
}

func stopFrame() *devinproto.GetChatMessageResponse {
	return &devinproto.GetChatMessageResponse{
		StopReason: devinproto.ExaCodeiumCommonPb_StopReason_ExaCodeiumCommonPb_StopReason_STOP_REASON_STOP_PATTERN.Enum(),
	}
}

// envelope encodes one Connect streaming envelope: flag byte + 4-byte
// big-endian length + payload.
func envelope(flag byte, payload []byte) []byte {
	out := make([]byte, 5, 5+len(payload))
	out[0] = flag
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload)))
	return append(out, payload...)
}

// truncateWrapper answers GetChatMessage requests carrying
// X-Stub-Mode: truncate with a syntactically valid stream prefix that ends
// mid-envelope — the client's frame reader must surface a transport-level
// truncation, not a semantic Connect error.
func truncateWrapper(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/GetChatMessage") &&
			r.Header.Get("X-Stub-Mode") == "truncate" {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "application/connect+proto")
			w.WriteHeader(http.StatusOK)
			meta, err := proto.Marshal(metaFrame())
			if err != nil {
				return
			}
			// One complete meta envelope, then an envelope header that
			// declares 100 bytes with only 2 payload bytes before EOF.
			body := append(envelope(0x00, meta), 0x00, 0x00, 0x00, 0x00, 0x64, 0x01, 0x02)
			_, _ = w.Write(body)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// versionWriter injects x-seen-http-version into the first response header
// block so tests can assert which HTTP version served the RPC.
type versionWriter struct {
	http.ResponseWriter
	proto string
	wrote bool
}

func (w *versionWriter) WriteHeader(code int) {
	if !w.wrote {
		w.wrote = true
		w.Header().Set("x-seen-http-version", w.proto)
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *versionWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *versionWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func versionInjector(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&versionWriter{ResponseWriter: w, proto: r.Proto}, r)
	})
}

// seatHandler mirrors the Windsurf SeatManagement GetUserStatus JSON
// Connect endpoint: POST + JSON body + Connect-Protocol-Version: 1 +
// Bearer auth. It echoes the auth header back for assertions.
func seatHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	auth := r.Header.Get("Authorization")
	protoVersion := r.Header.Get("Connect-Protocol-Version")
	if auth != "Bearer "+*seatToken {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code":    "unauthenticated",
			"message": "bad token",
		})
		return
	}
	var meta map[string]any
	_ = json.Unmarshal(body, &meta)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-seen-authorization", auth)
	w.Header().Set("x-seen-connect-protocol-version", protoVersion)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"userStatus": map[string]any{
			"name":   "QA Stub",
			"email":  "qa@example.invalid",
			"pro":    true,
			"userId": "u-stub",
			"planStatus": map[string]any{
				"availablePromptCredits": 42,
			},
		},
		"seenMetadata": meta["metadata"],
	})
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status_calls": statusCalls.Load(),
		"chat_calls":   chatCalls.Load(),
		"cancels":      cancels.Load(),
		"conns":        conns.Load(),
	})
}

func selfSignedCert() (tls.Certificate, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "task6-oracle.invalid"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost", "test.invalid"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	return pair, certPEM, nil
}

func main() {
	listen := flag.String("listen", "", "bind address, e.g. 127.0.0.1:8080")
	mode := flag.String("mode", "h1", "h1 | tls | h2c")
	certOut := flag.String("cert-out", "", "tls mode: write PEM cert here")
	flag.Parse()
	if *listen == "" {
		log.Fatal("-listen is required")
	}

	path, apiHandler := devinprotoconnect.NewApiServerServiceHandler(&svc{})
	mux := http.NewServeMux()
	mux.Handle(path, truncateWrapper(apiHandler))
	mux.HandleFunc("/exa.seat_management_pb.SeatManagementService/GetUserStatus", seatHandler)
	mux.HandleFunc("/stats", statsHandler)
	handler := versionInjector(mux)

	server := &http.Server{
		Addr:    *listen,
		Handler: handler,
		// Count accepted connections so the Rust side can assert HTTP/1.1
		// keep-alive reuse (sequential calls share one conn).
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateNew {
				conns.Add(1)
			}
		},
	}
	switch *mode {
	case "h1":
		log.Printf("oracle listening (h1) on %s", *listen)
		log.Fatal(server.ListenAndServe())
	case "h2c":
		server.Handler = h2c.NewHandler(handler, nil)
		log.Printf("oracle listening (h2c) on %s", *listen)
		log.Fatal(server.ListenAndServe())
	case "tls":
		cert, pem, err := selfSignedCert()
		if err != nil {
			log.Fatalf("self-signed cert: %v", err)
		}
		if *certOut != "" {
			if err := os.WriteFile(*certOut, pem, 0o600); err != nil {
				log.Fatalf("write cert: %v", err)
			}
		}
		server.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2", "http/1.1"},
		}
		ln, err := net.Listen("tcp", *listen)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("oracle listening (tls) on %s", *listen)
		log.Fatal(server.ServeTLS(ln, "", ""))
	default:
		log.Fatalf("unknown -mode %q", *mode)
	}
}
