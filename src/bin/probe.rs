//! Auxiliary binary `probe` — controlled experiments against the Devin
//! upstream. Port of `G/cmd/probe/main.go`: subcommand dispatch validates
//! before credentials, `DEVIN_TOKEN`/config/credentials token chain, the
//! captured-`CLI` client identity, and the shared upstream transport so
//! probe traffic shares the proxy's `fingerprint`.

use devin2api::auxiliary::probe::{self, ProbeContext};
use devin2api::upstream::request::ClientIdentity;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // Validate the subcommand before resolving the token: an unknown
    // subcommand prints usage and must not be preempted by "no token".
    let Some(command) = args.first() else {
        probe::usage();
        std::process::exit(2);
    };
    if !probe::COMMANDS.contains(&command.as_str()) {
        probe::usage();
        std::process::exit(2);
    }
    // Config load failure continues with the zero value: token and
    // identity still have the env/credentials/default fallback chain.
    let cfg = probe::load_probe_config();
    let token = probe::resolve_token(&cfg);
    if token.is_empty() {
        eprintln!("no token: set DEVIN_TOKEN or devin.token in config.yaml");
        std::process::exit(1);
    }
    let ctx = ProbeContext {
        aliases: cfg.devin.aliases.clone(),
        identity: ClientIdentity {
            name: cfg.devin.client_name.clone(),
            version: cfg.devin.client_version.clone(),
            os: cfg.devin.client_os.clone(),
        },
        token,
    };
    let client = match probe::build_client(&cfg, &ctx.token) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("ERR: {err:#}");
            std::process::exit(1);
        }
    };
    match tokio::time::timeout(
        std::time::Duration::from_secs(300),
        probe::run(&ctx, &client, command, &args[1..]),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            eprintln!("ERR: {err:#}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("ERR: context deadline exceeded");
            std::process::exit(1);
        }
    }
}
