//! Environment proxy resolution — a port of Go's
//! `net/http.ProxyFromEnvironment` (`x/net/http/httpproxy`), which is what
//! G's `http.DefaultTransport` consults when `devin.proxy` is empty.
//!
//! reqwest's own system matcher is deliberately NOT used: it honors
//! `ALL_PROXY` (Go does not), has no localhost/loopback bypass (Go always
//! bypasses), and treats `socks5` as local-DNS (Go's transport treats env
//! `socks5` as `socks5h`, remote DNS). This module reproduces the Go
//! semantics exactly:
//!
//! - Only `HTTP_PROXY`/`http_proxy`, `HTTPS_PROXY`/`https_proxy` and
//!   `NO_PROXY`/`no_proxy` are read; `ALL_PROXY` is ignored.
//! - Values may be a full URL or bare `host[:port]` (http assumed).
//! - `localhost` and any loopback IP bypass the proxy unconditionally.
//! - `NO_PROXY` supports `*`, CIDRs, IPs (with optional `:port`), domains
//!   (matching subdomains), and leading-dot/`*.` subdomain-only forms.
//! - `REQUEST_METHOD` set (CGI) makes an applicable `HTTP_PROXY` a request
//!   error, not a silent bypass.
//! - Env `socks5`/`socks5h` both mean remote DNS (Go's transport maps
//!   `socks5` → `socks5h`); other non-http(s) schemes are request errors.
//!
//! The environment is snapshotted once per client build, matching Go's
//! `envProxyFunc` once-ness.

use std::net::IpAddr;

/// A proxy URL that can never connect, used to surface Go's request-time
/// errors (CGI `HTTP_PROXY` refusal, unsupported env scheme) through
/// `reqwest::Proxy::custom`, which cannot return errors — `Some(Err)` is
/// swallowed into "direct". A guaranteed-dial-failure proxy preserves the
/// observable contract: the request fails as a transport error.
const BLACKHOLE_PROXY: &str = "http://0.0.0.0:0/";

/// Snapshot of the process proxy environment (Go `httpproxy.Config`).
#[derive(Debug, Clone, Default)]
pub struct EnvProxyConfig {
    /// Parsed `HTTP_PROXY`/`http_proxy`, if valid.
    http_proxy: Option<reqwest::Url>,
    /// Parsed `HTTPS_PROXY`/`https_proxy`, if valid.
    https_proxy: Option<reqwest::Url>,
    /// Whether `REQUEST_METHOD` is set (CGI): an applicable `HTTP_PROXY` is
    /// then a request error (Go's cgihttpproxy guard).
    cgi: bool,
    /// `NO_PROXY` IP/CIDR matchers.
    ip_matchers: Vec<IpMatcher>,
    /// `NO_PROXY` domain matchers.
    domain_matchers: Vec<DomainMatcher>,
    /// `NO_PROXY` contained `*` — Go installs `allMatch` in both matcher
    /// lists, so every host bypasses (including non-IP hosts, which skip
    /// the ipMatchers loop).
    bypass_all: bool,
}

#[derive(Debug, Clone)]
enum IpMatcher {
    /// `*` — everything bypasses.
    All,
    /// CIDR prefix.
    Cidr { network: IpAddr, prefix: u8 },
    /// Single IP with optional port.
    Ip { ip: IpAddr, port: Option<String> },
}

#[derive(Debug, Clone)]
struct DomainMatcher {
    /// Stored with a leading dot, like Go's `domainMatch.host`.
    host: String,
    port: Option<String>,
    /// Whether the bare host (without the leading dot) also matches.
    match_host: bool,
}

impl EnvProxyConfig {
    /// Read the environment (Go `FromEnvironment` + `config.init`).
    // The http_/https_ name pair intentionally mirrors the env vars.
    #[allow(clippy::similar_names)]
    #[must_use]
    pub fn from_env() -> Self {
        let http_var = env_any(&["HTTP_PROXY", "http_proxy"]);
        let https_var = env_any(&["HTTPS_PROXY", "https_proxy"]);
        let no_proxy = env_any(&["NO_PROXY", "no_proxy"]);
        let cgi = std::env::var("REQUEST_METHOD").is_ok_and(|v| !v.is_empty());

        let mut cfg = Self {
            http_proxy: parse_proxy(&http_var),
            https_proxy: parse_proxy(&https_var),
            cgi,
            ..Default::default()
        };
        cfg.init_no_proxy(&no_proxy);
        cfg
    }

    /// Go `config.init`'s `NoProxy` loop.
    fn init_no_proxy(&mut self, no_proxy: &str) {
        for raw in no_proxy.split(',') {
            let p = raw.trim().to_ascii_lowercase();
            if p.is_empty() {
                continue;
            }
            if p == "*" {
                self.ip_matchers = vec![IpMatcher::All];
                self.domain_matchers = Vec::new();
                self.bypass_all = true;
                return;
            }
            // IPv4/CIDR, IPv6/CIDR.
            if let Some((network, prefix)) = parse_cidr(&p) {
                self.ip_matchers.push(IpMatcher::Cidr { network, prefix });
                continue;
            }
            // IPv4:port, [IPv6]:port — Go uses net.SplitHostPort; on error
            // the whole entry is treated as the host part.
            let (phost, pport) = match split_host_port(&p) {
                Some((h, pt)) => {
                    if h.is_empty() {
                        continue; // malformed entry, ignored like Go
                    }
                    (strip_brackets(&h).to_string(), Some(pt))
                }
                None => (p.clone(), None),
            };
            // IPv4, IPv6.
            if let Ok(ip) = phost.parse::<IpAddr>() {
                self.ip_matchers.push(IpMatcher::Ip { ip, port: pport });
                continue;
            }
            if phost.is_empty() {
                continue;
            }
            // domain.com / .domain.com / *.domain.com, optional :port.
            let mut host = phost;
            if let Some(rest) = host.strip_prefix('*') {
                host = rest.to_string();
            }
            let match_host = !host.starts_with('.');
            if match_host {
                host = format!(".{host}");
            }
            self.domain_matchers.push(DomainMatcher {
                host,
                port: pport,
                match_host,
            });
        }
    }

    /// Go `proxyForURL`: pick the proxy for a request URL. Returns `None`
    /// for a direct connection, `Some(url)` for the proxy to use. The
    /// `BLACKHOLE_PROXY` result marks Go's request-time error cases.
    fn resolve(&self, req: &reqwest::Url) -> Option<reqwest::Url> {
        let proxy = match req.scheme() {
            "https" => self.https_proxy.as_ref(),
            "http" => {
                if self.cgi && self.http_proxy.is_some() {
                    // Go: "refusing to use HTTP_PROXY value in CGI
                    // environment" — a request error, not a bypass.
                    return Some(blackhole());
                }
                self.http_proxy.as_ref()
            }
            _ => None,
        };
        let proxy = proxy?;
        if !self.use_proxy(&canonical_addr(req)) {
            return None;
        }
        Some(remap_scheme_for_reqwest(proxy))
    }

    /// Go `useProxy(addr)` where addr is `host:port` (canonicalAddr output).
    fn use_proxy(&self, addr: &str) -> bool {
        if addr.is_empty() {
            return true;
        }
        let Some((host, port)) = split_host_port(addr) else {
            return false;
        };
        if host == "localhost" {
            return false;
        }
        // netip.ParseAddr + IsLoopback: IPv4-mapped IPv6 counts as loopback.
        let ip = host.parse::<IpAddr>().ok().map(normalize_ip);
        if ip.is_some_and(|i| i.is_loopback()) {
            return false;
        }
        let host = host.trim().to_ascii_lowercase();
        if self.bypass_all {
            return false;
        }
        if let Some(ip) = ip {
            for m in &self.ip_matchers {
                if m.matches(&host, &port, Some(ip)) {
                    return false;
                }
            }
        }
        for m in &self.domain_matchers {
            if m.matches(&host, &port, ip) {
                return false;
            }
        }
        true
    }
}

impl IpMatcher {
    fn matches(&self, _host: &str, port: &str, ip: Option<IpAddr>) -> bool {
        match self {
            Self::All => true,
            Self::Cidr { network, prefix } => {
                ip.is_some_and(|i| cidr_contains(*network, *prefix, i))
            }
            Self::Ip {
                ip: want,
                port: want_port,
            } => {
                ip.is_some_and(|i| ip_eq(*want, i))
                    && want_port.as_deref().is_none_or(|p| p == port)
            }
        }
    }
}

impl DomainMatcher {
    fn matches(&self, host: &str, port: &str, ip: Option<IpAddr>) -> bool {
        if ip.is_some() {
            return false;
        }
        if host.ends_with(&self.host) || (self.match_host && host == &self.host[1..]) {
            return self.port.as_deref().is_none_or(|p| p == port);
        }
        false
    }
}

/// Build the `reqwest::Proxy` that applies Go's environment semantics per
/// request. Always returns a proxy object (custom intercept); requests
/// that resolve to "direct" return `None` from the closure. Go's
/// request-time error cases (CGI refusal, unsupported env scheme) cannot
/// be returned through this API, so they resolve to [`BLACKHOLE_PROXY`],
/// which fails the request as a transport error — the same observable
/// outcome Go produces.
#[must_use]
pub fn env_proxy() -> reqwest::Proxy {
    let cfg = EnvProxyConfig::from_env();
    reqwest::Proxy::custom(move |url| cfg.resolve(url))
}

fn blackhole() -> reqwest::Url {
    reqwest::Url::parse(BLACKHOLE_PROXY).expect("static URL")
}

/// First non-empty value among `names` (Go `getEnvAny`).
fn env_any(names: &[&str]) -> String {
    for name in names {
        if let Ok(val) = std::env::var(name)
            && !val.is_empty()
        {
            return val;
        }
    }
    String::new()
}

/// Go `parseProxy`: empty → None; a full URL parses as-is; a bogus or
/// scheme/host-less value retries with `http://` prepended; unparseable
/// values are ignored (Go's `init` swallows the error → no proxy).
fn parse_proxy(raw: &str) -> Option<reqwest::Url> {
    if raw.is_empty() {
        return None;
    }
    match reqwest::Url::parse(raw) {
        Ok(url) if !url.scheme().is_empty() && url.host().is_some() => Some(url),
        _ => reqwest::Url::parse(&format!("http://{raw}")).ok(),
    }
}

/// Go's transport maps env `socks5` to `socks5h` (remote DNS); reqwest
/// keeps them distinct, so rewrite the scheme. Everything else passes
/// through — including schemes reqwest will reject at dial time, which is
/// the same request-error outcome Go produces for them.
fn remap_scheme_for_reqwest(url: &reqwest::Url) -> reqwest::Url {
    if url.scheme() == "socks5" {
        let rewritten = url.as_str().replacen("socks5://", "socks5h://", 1);
        return reqwest::Url::parse(&rewritten).unwrap_or_else(|_| url.clone());
    }
    url.clone()
}

/// Go `canonicalAddr`: `net.JoinHostPort(hostname, port)` with the port
/// defaulted from `portMap` (http→80, https→443, socks5→1080, else "").
fn canonical_addr(url: &reqwest::Url) -> String {
    let host = url.host_str().unwrap_or("");
    let port = url.port().map_or_else(
        || match url.scheme() {
            "http" => "80".to_string(),
            "https" => "443".to_string(),
            "socks5" => "1080".to_string(),
            _ => String::new(),
        },
        |p| p.to_string(),
    );
    join_host_port(host, &port)
}

/// `net.JoinHostPort`: bracket IPv6 literals.
fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// `net.SplitHostPort` for the shapes `canonicalAddr` and `NO_PROXY` entries
/// produce: `host:port`, `[v6]:port`, bare `host`, bare `[v6]`.
/// Returns `None` where Go errors (missing port, unbracketed multi-colon).
fn split_host_port(s: &str) -> Option<(String, String)> {
    if let Some(rest) = s.strip_prefix('[') {
        let end = rest.find(']')?;
        let host = &rest[..end];
        let after = &rest[end + 1..];
        return after
            .strip_prefix(':')
            .map(|port| (format!("[{host}]"), port.to_string()));
    }
    match s.rsplit_once(':') {
        // Multiple colons without brackets → Go's "too many colons" error.
        Some((host, port)) if !host.contains(':') => Some((host.to_string(), port.to_string())),
        _ => None,
    }
}

/// Strip the brackets Go removes after `SplitHostPort` on `[v6]:port`.
fn strip_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// `net.ParseCIDR`: `ip/prefix` → (network address, prefix bits).
fn parse_cidr(s: &str) -> Option<(IpAddr, u8)> {
    let (ip, bits) = s.rsplit_once('/')?;
    let ip: IpAddr = ip.parse().ok()?;
    let prefix: u8 = bits.parse().ok()?;
    let max = match ip {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    if prefix > max {
        return None;
    }
    Some((ip, prefix))
}

/// `IPNet.Contains` with Go's IPv4/IPv6-mapped equivalence.
fn cidr_contains(network: IpAddr, prefix: u8, ip: IpAddr) -> bool {
    match (normalize_ip(network), normalize_ip(ip)) {
        (IpAddr::V4(net), IpAddr::V4(ip)) => {
            let mask = prefix_mask32(prefix);
            (u32::from(net) & mask) == (u32::from(ip) & mask)
        }
        (IpAddr::V6(net), IpAddr::V6(ip)) => {
            let mask = prefix_mask128(prefix);
            (u128::from(net) & mask) == (u128::from(ip) & mask)
        }
        _ => false,
    }
}

fn prefix_mask32(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}

fn prefix_mask128(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    }
}

/// Go's `netip.ParseAddr`/`net.IP.Equal` semantics: an IPv4-mapped IPv6
/// address equals its IPv4 form.
fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        v4 @ IpAddr::V4(_) => v4,
    }
}

fn ip_eq(a: IpAddr, b: IpAddr) -> bool {
    normalize_ip(a) == normalize_ip(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(http: &str, https: &str, no_proxy: &str) -> EnvProxyConfig {
        let mut c = EnvProxyConfig {
            http_proxy: parse_proxy(http),
            https_proxy: parse_proxy(https),
            cgi: false,
            ..Default::default()
        };
        c.init_no_proxy(no_proxy);
        c
    }

    fn url(s: &str) -> reqwest::Url {
        reqwest::Url::parse(s).unwrap()
    }

    #[test]
    fn localhost_and_loopback_bypass() {
        let c = cfg("http://proxy:1", "http://proxy:1", "");
        assert!(c.resolve(&url("http://localhost:9/x")).is_none());
        assert!(c.resolve(&url("http://127.0.0.1:9/x")).is_none());
        assert!(c.resolve(&url("http://[::1]:9/x")).is_none());
        assert!(c.resolve(&url("http://[::ffff:127.0.0.1]:9/x")).is_none());
        // Non-loopback still proxied.
        assert!(c.resolve(&url("http://example.com/x")).is_some());
    }

    #[test]
    fn no_proxy_forms() {
        let c = cfg(
            "http://proxy:1",
            "",
            "foo.com,.bar.com,*.baz.com,10.0.0.0/8,192.168.1.1:8080",
        );
        // foo.com matches itself and subdomains.
        assert!(c.resolve(&url("http://foo.com/")).is_none());
        assert!(c.resolve(&url("http://a.foo.com/")).is_none());
        assert!(c.resolve(&url("http://notfoo.com/")).is_some());
        // .bar.com matches subdomains only.
        assert!(c.resolve(&url("http://x.bar.com/")).is_none());
        assert!(c.resolve(&url("http://bar.com/")).is_some());
        // *.baz.com behaves like .baz.com.
        assert!(c.resolve(&url("http://x.baz.com/")).is_none());
        assert!(c.resolve(&url("http://baz.com/")).is_some());
        // CIDR.
        assert!(c.resolve(&url("http://10.1.2.3/")).is_none());
        assert!(c.resolve(&url("http://11.0.0.1/")).is_some());
        // IP with port: only that port bypasses.
        assert!(c.resolve(&url("http://192.168.1.1:8080/")).is_none());
        assert!(c.resolve(&url("http://192.168.1.1:9090/")).is_some());
    }

    #[test]
    fn star_bypasses_everything() {
        let c = cfg("http://proxy:1", "http://proxy:1", "*");
        assert!(c.resolve(&url("http://example.com/")).is_none());
        assert!(c.resolve(&url("https://example.com/")).is_none());
    }

    #[test]
    fn bare_host_port_gets_http_scheme() {
        let c = cfg("proxy.local:3128", "", "");
        let got = c.resolve(&url("http://example.com/")).unwrap();
        assert_eq!(got.scheme(), "http");
        assert_eq!(got.host_str(), Some("proxy.local"));
        assert_eq!(got.port(), Some(3128));
    }

    #[test]
    fn env_socks5_maps_to_remote_dns() {
        let c = cfg("socks5://proxy:1080", "", "");
        let got = c.resolve(&url("http://example.com/")).unwrap();
        assert_eq!(got.scheme(), "socks5h");
    }

    #[test]
    fn https_uses_https_proxy_only() {
        let c = cfg("http://p1:1", "http://p2:2", "");
        assert_eq!(
            c.resolve(&url("https://example.com/")).unwrap().port(),
            Some(2)
        );
        assert_eq!(
            c.resolve(&url("http://example.com/")).unwrap().port(),
            Some(1)
        );
    }

    #[test]
    fn split_host_port_shapes() {
        assert_eq!(
            split_host_port("a.b:80"),
            Some(("a.b".to_string(), "80".to_string()))
        );
        assert_eq!(
            split_host_port("[::1]:80"),
            Some(("[::1]".to_string(), "80".to_string()))
        );
        assert_eq!(split_host_port("::1"), None);
        assert_eq!(split_host_port("host"), None);
        assert_eq!(
            split_host_port(":80"),
            Some((String::new(), "80".to_string()))
        );
    }
}
