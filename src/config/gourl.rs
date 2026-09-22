//! The Go `net/url` subset that `redactConfigSecrets` relies on: `url.Parse`
//! (with `viaRequest == false`) and `URL.String` after `User = nil`.
//!
//! This is a byte-faithful port of `net/url/url.go` (Go 1.27.1) restricted to
//! what the proxy-redaction path can observe: parsing succeeds or fails, the
//! parsed URL carries userinfo or not, and re-serialization reproduces Go's
//! escaping decisions exactly (scheme lowercasing, host/path/fragment
//! re-encoding, `ForceQuery`, `OmitHost`, the `//`-path guard and the
//! leading-`./` guard). `netip.ParseAddr` is ported for the bracketed
//! IP-literal host rule.
//!
//! Everything operates on bytes like Go strings do; inputs are `&str` so all
//! verbatim slices stay valid UTF-8 and every escaped output byte is ASCII.

/// Go `encoding` mode bits (`encoding_table.go`).
const ENCODE_PATH: u8 = 1 << 0;
const ENCODE_HOST: u8 = 1 << 2;
const ENCODE_ZONE: u8 = 1 << 3;
const ENCODE_USER_PASSWORD: u8 = 1 << 4;
const ENCODE_FRAGMENT: u8 = 1 << 6;
const HEX_CHAR: u8 = 1 << 7;

/// Port of the generated `table[256]encoding`: the modes in which byte `c`
/// may appear unescaped. Bytes not listed (control bytes, space, `%`, `?`
/// for most modes, non-ASCII) default to 0 — escaped everywhere.
const fn table(c: u8) -> u8 {
    match c {
        b'!' | b'(' | b')' | b'*' => ENCODE_FRAGMENT | ENCODE_ZONE | ENCODE_HOST,
        b'"' | b'\'' | b'[' | b']' | b'<' | b'>' => ENCODE_ZONE | ENCODE_HOST,
        b'$'
        | b'&'
        | b'+'
        | b','
        | b'-'
        | b'.'
        | b'_'
        | b'~'
        | b';'
        | b'='
        | b'G'..=b'Z'
        | b'g'..=b'z' => {
            ENCODE_FRAGMENT | ENCODE_USER_PASSWORD | ENCODE_ZONE | ENCODE_HOST | ENCODE_PATH
        }
        b'/' | b'@' => ENCODE_FRAGMENT | ENCODE_PATH,
        b'0'..=b'9' | b'A'..=b'F' | b'a'..=b'f' => {
            HEX_CHAR
                | ENCODE_FRAGMENT
                | ENCODE_USER_PASSWORD
                | ENCODE_ZONE
                | ENCODE_HOST
                | ENCODE_PATH
        }
        b':' => ENCODE_FRAGMENT | ENCODE_ZONE | ENCODE_HOST | ENCODE_PATH,
        b'?' => ENCODE_FRAGMENT,
        _ => 0,
    }
}

const UPPERHEX: &[u8; 16] = b"0123456789ABCDEF";

fn ishex(c: u8) -> bool {
    table(c) & HEX_CHAR != 0
}

/// Precondition: `ishex(c)`.
fn unhex(c: u8) -> u8 {
    9 * (c >> 6) + (c & 15)
}

fn should_escape(c: u8, mode: u8) -> bool {
    table(c) & mode == 0
}

/// Port of `unescape` for the modes this path uses (never
/// `encodeQueryComponent`, so `+` is always literal). Errors are collapsed
/// to `None`: `redactConfigSecrets` only observes parse success/failure.
fn unescape(s: &[u8], mode: u8) -> Option<Vec<u8>> {
    // Count %, check that they're well-formed.
    let mut n = 0usize;
    let mut i = 0usize;
    while i < s.len() {
        if s[i] == b'%' {
            n += 1;
            if i + 2 >= s.len() || !ishex(s[i + 1]) || !ishex(s[i + 2]) {
                return None; // EscapeError
            }
            // Host mode: %-encoding only for non-ASCII bytes, except the
            // RFC 6874 "%25" zone introducer.
            if mode == ENCODE_HOST && unhex(s[i + 1]) < 8 && &s[i..i + 3] != b"%25" {
                return None; // EscapeError
            }
            if mode == ENCODE_ZONE {
                let v = unhex(s[i + 1]) << 4 | unhex(s[i + 2]);
                if &s[i..i + 3] != b"%25" && v != b' ' && should_escape(v, ENCODE_HOST) {
                    return None; // EscapeError
                }
            }
            i += 3;
        } else {
            if (mode == ENCODE_HOST || mode == ENCODE_ZONE)
                && s[i] < 0x80
                && should_escape(s[i], mode)
            {
                return None; // InvalidHostError
            }
            i += 1;
        }
    }

    if n == 0 {
        return Some(s.to_vec());
    }
    let mut t = Vec::with_capacity(s.len() - 2 * n);
    let mut i = 0usize;
    while i < s.len() {
        if s[i] == b'%' {
            t.push(unhex(s[i + 1]) << 4 | unhex(s[i + 2]));
            i += 3;
        } else {
            t.push(s[i]);
            i += 1;
        }
    }
    Some(t)
}

/// Port of `escape` (query-component `+` handling included for completeness;
/// unused by the redaction path).
fn escape(s: &[u8], mode: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    for &c in s {
        if should_escape(c, mode) {
            out.push(b'%');
            out.push(UPPERHEX[(c >> 4) as usize]);
            out.push(UPPERHEX[(c & 15) as usize]);
        } else {
            out.push(c);
        }
    }
    out
}

/// Port of `validEncoded`: `s` must not contain bytes that require escaping
/// in `mode`; RFC 3986 sub-delims plus `[`/`]`/`%` are allowed outright.
fn valid_encoded(s: &[u8], mode: u8) -> bool {
    for &c in s {
        match c {
            b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' | b'=' | b':'
            | b'@' | b'[' | b']' | b'%' => {}
            _ => {
                if should_escape(c, mode) {
                    return false;
                }
            }
        }
    }
    true
}

/// Port of `validUserinfo` (RFC 3986 §3.2.1 plus the `@` tolerance from
/// go.dev/issue/3439). Non-ASCII bytes fall through to `false` like Go's
/// rune loop does.
fn valid_userinfo(s: &[u8]) -> bool {
    for &c in s {
        match c {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'.'
            | b'_'
            | b':'
            | b'~'
            | b'!'
            | b'$'
            | b'&'
            | b'\''
            | b'('
            | b')'
            | b'*'
            | b'+'
            | b','
            | b';'
            | b'='
            | b'%'
            | b'@' => {}
            _ => return false,
        }
    }
    true
}

/// Port of `stringContainsCTLByte`.
fn contains_ctl_byte(s: &[u8]) -> bool {
    s.iter().any(|&b| b < b' ' || b == 0x7f)
}

/// Port of `validOptionalPort`: "" or `:\d*`.
fn valid_optional_port(port: &[u8]) -> bool {
    if port.is_empty() {
        return true;
    }
    port[0] == b':' && port[1..].iter().all(u8::is_ascii_digit)
}

/// Port of `getScheme`: splits `scheme:` off the front. Returns `Err` only
/// for a leading ':' ("missing protocol scheme").
fn get_scheme(raw: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    for (i, &c) in raw.iter().enumerate() {
        match c {
            b'a'..=b'z' | b'A'..=b'Z' => {}
            b'0'..=b'9' | b'+' | b'-' | b'.' => {
                if i == 0 {
                    return Some((Vec::new(), raw));
                }
            }
            b':' => {
                if i == 0 {
                    return None;
                }
                return Some((raw[..i].to_vec(), &raw[i + 1..]));
            }
            _ => return Some((Vec::new(), raw)),
        }
    }
    Some((Vec::new(), raw))
}

/// Port of `netip.ParseAddr` restricted to a validity verdict plus the
/// `Is4()` bit `parseHost` checks. `Err` covers every parse failure.
fn parse_addr(s: &[u8]) -> Option<bool> {
    for &b in s {
        match b {
            b'.' => return parse_ipv4(s).map(|()| true),
            b':' => return parse_ipv6(s).map(|()| false),
            b'%' => return None, // zone without address
            _ => {}
        }
    }
    None
}

/// Port of `parseIPv4Fields` over `s` into `fields` (exactly 4 octets).
fn parse_ipv4_fields(s: &[u8], fields: &mut [u8]) -> Option<()> {
    let mut val: u32 = 0;
    let mut pos = 0usize;
    let mut dig_len = 0usize;
    for (i, &c) in s.iter().enumerate() {
        if c.is_ascii_digit() {
            if dig_len == 1 && val == 0 {
                return None; // leading zero
            }
            val = val * 10 + u32::from(c - b'0');
            dig_len += 1;
            if val > 255 {
                return None;
            }
        } else if c == b'.' {
            if i == 0 || i == s.len() - 1 || s[i - 1] == b'.' {
                return None;
            }
            if pos == 3 {
                return None; // too long
            }
            // val <= 255 was checked above.
            fields[pos] = u8::try_from(val).expect("ipv4 octet <= 255");
            pos += 1;
            val = 0;
            dig_len = 0;
        } else {
            return None;
        }
    }
    if pos < 3 {
        return None; // too short
    }
    fields[3] = u8::try_from(val).expect("ipv4 octet <= 255");
    Some(())
}

fn parse_ipv4(s: &[u8]) -> Option<()> {
    let mut fields = [0u8; 4];
    parse_ipv4_fields(s, &mut fields)
}

/// Port of `parseIPv6` (zone split, `::` ellipsis, embedded IPv4). Only the
/// success/failure verdict matters to the caller.
fn parse_ipv6(input: &[u8]) -> Option<()> {
    let mut s = input;
    let mut zone: &[u8] = b"";
    if let Some(i) = s.iter().position(|&b| b == b'%') {
        zone = &s[i + 1..];
        s = &s[..i];
        if zone.is_empty() {
            return None;
        }
    }

    let mut ip = [0u8; 16];
    let mut ellipsis: i32 = -1;

    if s.len() >= 2 && s[0] == b':' && s[1] == b':' {
        ellipsis = 0;
        s = &s[2..];
        if s.is_empty() {
            return Some(()); // "::"
        }
    }

    let mut i = 0usize;
    while i < 16 {
        // Hex group.
        let mut off = 0usize;
        let mut acc: u32 = 0;
        while off < s.len() {
            let c = s[off];
            if c.is_ascii_digit() {
                acc = (acc << 4) + u32::from(c - b'0');
            } else if (b'a'..=b'f').contains(&c) {
                acc = (acc << 4) + u32::from(c - b'a' + 10);
            } else if (b'A'..=b'F').contains(&c) {
                acc = (acc << 4) + u32::from(c - b'A' + 10);
            } else {
                break;
            }
            if off > 3 {
                return None; // more than 4 digits
            }
            if acc > 0xffff {
                return None;
            }
            off += 1;
        }
        if off == 0 {
            return None; // empty field
        }

        // Embedded trailing IPv4 ("::ffff:1.2.3.4").
        if off < s.len() && s[off] == b'.' {
            if ellipsis < 0 && i != 12 {
                return None;
            }
            if i + 4 > 16 {
                return None;
            }
            let mut end = input.len();
            if !zone.is_empty() {
                end -= zone.len() + 1;
            }
            parse_ipv4_fields(&input[end - s.len()..end], &mut ip[i..i + 4])?;
            s = &[];
            i += 4;
            break;
        }

        // acc <= 0xffff was checked above; take the big-endian bytes.
        let [hi, lo] = u16::try_from(acc)
            .expect("ipv6 piece <= 0xffff")
            .to_be_bytes();
        ip[i] = hi;
        ip[i + 1] = lo;
        i += 2;

        s = &s[off..];
        if s.is_empty() {
            break;
        }
        if s[0] != b':' {
            return None;
        }
        if s.len() == 1 {
            return None; // trailing ':'
        }
        s = &s[1..];
        if s[0] == b':' {
            if ellipsis >= 0 {
                return None; // second "::"
            }
            ellipsis = i32::try_from(i).expect("ipv6 index fits i32");
            s = &s[1..];
            if s.is_empty() {
                break;
            }
        }
    }

    if !s.is_empty() {
        return None; // trailing garbage
    }
    if i < 16 {
        if ellipsis < 0 {
            return None; // too short
        }
        // Expansion validity is all the caller needs.
    } else if ellipsis >= 0 {
        return None; // "::" must expand at least one field
    }
    Some(())
}

/// Port of `parseHost`: validates `host[:port]` (or `[v6]:port`) and returns
/// the unescaped host form Go stores.
fn parse_host(scheme: &[u8], host: &[u8]) -> Option<Vec<u8>> {
    let open_bracket = host.iter().rposition(|&b| b == b'[');
    if let Some(idx) = open_bracket {
        if idx > 0 {
            return None; // "invalid IP-literal"
        }
        // IP-literal per RFC 3986 / RFC 6874.
        let close_bracket = host.iter().rposition(|&b| b == b']')?;
        let colon_port = &host[close_bracket + 1..];
        if !valid_optional_port(colon_port) {
            return None;
        }
        let unescaped_colon_port = unescape(colon_port, ENCODE_HOST)?;
        let hostname = &host[1..close_bracket];
        let unescaped_hostname = if let Some(z) = hostname.windows(3).position(|w| w == b"%25") {
            let mut v = unescape(&hostname[..z], ENCODE_HOST)?;
            v.extend_from_slice(&unescape(&hostname[z..], ENCODE_ZONE)?);
            v
        } else {
            unescape(hostname, ENCODE_HOST)?
        };
        let is_v4 = parse_addr(&unescaped_hostname)?;
        if is_v4 {
            return None; // IPv4 must not be bracketed
        }
        let mut out = Vec::with_capacity(unescaped_hostname.len() + 2);
        out.push(b'[');
        out.extend_from_slice(&unescaped_hostname);
        out.push(b']');
        out.extend_from_slice(&unescaped_colon_port);
        return Some(out);
    }
    if let Some(first_colon) = host.iter().position(|&b| b == b':') {
        let last_colon = host.iter().rposition(|&b| b == b':').unwrap();
        let mut i = first_colon;
        if last_colon != first_colon {
            // Strict-colons default (urlstrictcolons=1): http/https keep the
            // first colon so "a:b:c" fails the port check; other schemes use
            // the last colon.
            if !(scheme == b"http" || scheme == b"https") {
                i = last_colon;
            }
        }
        if !valid_optional_port(&host[i..]) {
            return None;
        }
    }
    unescape(host, ENCODE_HOST)
}

/// The parsed-URL subset `redactConfigSecrets` observes.
#[derive(Debug, Default)]
struct GoUrl {
    scheme: Vec<u8>,
    opaque: Vec<u8>,
    /// Re-encoded `userinfo` (Go `Userinfo.String()` output) when present.
    user: Option<Vec<u8>>,
    host: Vec<u8>,
    path: Vec<u8>,
    raw_path: Vec<u8>,
    force_query: bool,
    raw_query: Vec<u8>,
    omit_host: bool,
    fragment: Vec<u8>,
    raw_fragment: Vec<u8>,
}

/// Port of `parseAuthority`: returns `(userinfo, host)` where `userinfo` is
/// `Some` exactly when Go's `url.User != nil`.
fn parse_authority(scheme: &[u8], authority: &[u8]) -> Option<(Option<Vec<u8>>, Vec<u8>)> {
    let at = authority.iter().rposition(|&b| b == b'@');
    let host = match at {
        Some(i) => parse_host(scheme, &authority[i + 1..])?,
        None => parse_host(scheme, authority)?,
    };
    let Some(i) = at else {
        return Some((None, host));
    };
    let userinfo = &authority[..i];
    if !valid_userinfo(userinfo) {
        return None;
    }
    let user = if userinfo.contains(&b':') {
        let colon = userinfo.iter().position(|&b| b == b':').unwrap();
        let username = unescape(&userinfo[..colon], ENCODE_USER_PASSWORD)?;
        let password = unescape(&userinfo[colon + 1..], ENCODE_USER_PASSWORD)?;
        // Userinfo.String(): escape(username) + ":" + escape(password).
        let mut s = escape(&username, ENCODE_USER_PASSWORD);
        s.push(b':');
        s.extend_from_slice(&escape(&password, ENCODE_USER_PASSWORD));
        s
    } else {
        unescape(userinfo, ENCODE_USER_PASSWORD)?
    };
    Some((Some(user), host))
}

/// Port of `setPath`: stores the unescaped path and keeps `raw_path` only
/// when the default re-encoding differs.
fn set_path(url: &mut GoUrl, p: &[u8]) -> Option<()> {
    let path = unescape(p, ENCODE_PATH)?;
    url.raw_path = if escape(&path, ENCODE_PATH) == p {
        Vec::new()
    } else {
        p.to_vec()
    };
    url.path = path;
    Some(())
}

/// Port of `URL.EscapedPath`.
fn escaped_path(url: &GoUrl) -> Vec<u8> {
    if !url.raw_path.is_empty()
        && valid_encoded(&url.raw_path, ENCODE_PATH)
        && let Some(p) = unescape(&url.raw_path, ENCODE_PATH)
        && p == url.path
    {
        return url.raw_path.clone();
    }
    if url.path == b"*" {
        return b"*".to_vec();
    }
    escape(&url.path, ENCODE_PATH)
}

/// Port of `setFragment`.
fn set_fragment(url: &mut GoUrl, f: &[u8]) -> Option<()> {
    let frag = unescape(f, ENCODE_FRAGMENT)?;
    url.raw_fragment = if escape(&frag, ENCODE_FRAGMENT) == f {
        Vec::new()
    } else {
        f.to_vec()
    };
    url.fragment = frag;
    Some(())
}

/// Port of `URL.EscapedFragment`.
fn escaped_fragment(url: &GoUrl) -> Vec<u8> {
    if !url.raw_fragment.is_empty()
        && valid_encoded(&url.raw_fragment, ENCODE_FRAGMENT)
        && let Some(f) = unescape(&url.raw_fragment, ENCODE_FRAGMENT)
        && f == url.fragment
    {
        return url.raw_fragment.clone();
    }
    escape(&url.fragment, ENCODE_FRAGMENT)
}

/// Port of `url.Parse` (`viaRequest == false`): the fragment is cut before
/// the inner parse, and a trailing lone `#` leaves no fragment at all.
fn go_parse(raw: &str) -> Option<GoUrl> {
    let bytes = raw.as_bytes();
    let (u, frag) = match bytes.iter().position(|&b| b == b'#') {
        Some(i) => (&bytes[..i], &bytes[i + 1..]),
        None => (bytes, &[][..]),
    };
    let mut url = parse_inner(u)?;
    if !frag.is_empty() {
        set_fragment(&mut url, frag)?;
    }
    Some(url)
}

/// Port of the inner `parse` with `viaRequest == false`.
fn parse_inner(raw: &[u8]) -> Option<GoUrl> {
    if contains_ctl_byte(raw) {
        return None;
    }
    let mut url = GoUrl::default();
    if raw == b"*" {
        url.path = b"*".to_vec();
        return Some(url);
    }

    let (scheme, rest0) = get_scheme(raw)?;
    url.scheme = scheme.iter().map(u8::to_ascii_lowercase).collect();
    let mut rest: &[u8] = rest0;

    if rest.ends_with(b"?") && rest.split(|&b| b == b'?').count() == 2 {
        url.force_query = true;
        rest = &rest[..rest.len() - 1];
    } else if let Some(i) = rest.iter().position(|&b| b == b'?') {
        url.raw_query = rest[i + 1..].to_vec();
        rest = &rest[..i];
    }

    if !rest.starts_with(b"/") {
        if !url.scheme.is_empty() {
            // Rootless path: opaque.
            url.opaque = rest.to_vec();
            return Some(url);
        }
        // Relative-path reference: first segment must not contain ':'.
        let first_seg_end = rest.iter().position(|&b| b == b'/').unwrap_or(rest.len());
        if rest[..first_seg_end].contains(&b':') {
            return None;
        }
    }

    if (!url.scheme.is_empty() || !rest.starts_with(b"///")) && rest.starts_with(b"//") {
        let after = &rest[2..];
        let slash = after.iter().position(|&b| b == b'/').unwrap_or(after.len());
        let (authority, tail) = (&after[..slash], &after[slash..]);
        let (user, host) = parse_authority(&url.scheme, authority)?;
        url.user = user;
        url.host = host;
        rest = tail;
    } else if !url.scheme.is_empty() && rest.starts_with(b"/") {
        url.omit_host = true;
    }

    set_path(&mut url, rest)?;
    Some(url)
}

/// Port of `URL.String`.
fn go_string(url: &GoUrl) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    if !url.scheme.is_empty() {
        buf.extend_from_slice(&url.scheme);
        buf.push(b':');
    }
    if url.opaque.is_empty() {
        if !url.scheme.is_empty() || !url.host.is_empty() || url.user.is_some() {
            if url.omit_host && url.host.is_empty() && url.user.is_none() {
                // omit empty host
            } else {
                if !url.host.is_empty() || !url.path.is_empty() || url.user.is_some() {
                    buf.extend_from_slice(b"//");
                }
                if let Some(user) = &url.user {
                    buf.extend_from_slice(user);
                    buf.push(b'@');
                }
                if !url.host.is_empty() {
                    buf.extend_from_slice(&escape(&url.host, ENCODE_HOST));
                }
            }
        }
        let mut path = escaped_path(url);
        if url.omit_host && url.host.is_empty() && url.user.is_none() && path.starts_with(b"//") {
            buf.extend_from_slice(b"%2F");
            path = path[1..].to_vec();
        }
        if !path.is_empty() && path[0] != b'/' && !url.host.is_empty() {
            buf.push(b'/');
        }
        if buf.is_empty() {
            // A first path segment containing ':' needs "./" protection.
            let first_seg_end = path.iter().position(|&b| b == b'/').unwrap_or(path.len());
            if path[..first_seg_end].contains(&b':') {
                buf.extend_from_slice(b"./");
            }
        }
        buf.extend_from_slice(&path);
    } else {
        buf.extend_from_slice(&url.opaque);
    }
    if url.force_query || !url.raw_query.is_empty() {
        buf.push(b'?');
        buf.extend_from_slice(&url.raw_query);
    }
    if !url.fragment.is_empty() {
        buf.push(b'#');
        buf.extend_from_slice(&escaped_fragment(url));
    }
    buf
}

/// The `redactConfigSecrets` proxy rewrite: `url.Parse(raw)`; when the parse
/// fails or `User` is nil the caller keeps the original string, otherwise
/// the URL is re-serialized with `User = nil` — exactly the Go sequence.
#[must_use]
pub fn strip_url_userinfo(raw: &str) -> Option<String> {
    let mut url = go_parse(raw)?;
    url.user.as_ref()?;
    url.user = None;
    let out = go_string(&url);
    Some(String::from_utf8_lossy(&out).into_owned())
}
