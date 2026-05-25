use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use orchestrate::{DaemonConfiguration, HarnessKind, RoleName, WirePath};
use owner_signal_orchestrate::{
    CreateRoleOrder, Frame as OwnerOrchestrateFrame, FrameBody as OwnerOrchestrateFrameBody,
    OwnerOrchestrateReply, OwnerOrchestrateRequest, RefreshRepositoryIndexOrder,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, RequestPayload, SessionEpoch, ShortHeader,
};
use signal_orchestrate::{
    Observation, OrchestrateFrame, OrchestrateFrameBody, OrchestrateReply, OrchestrateRequest,
    RoleClaim, ScopeReason, ScopeReference,
};
use tempfile::TempDir;

struct DaemonFixture {
    _temporary: TempDir,
    workspace: PathBuf,
    ordinary_socket: PathBuf,
    owner_socket: PathBuf,
    child: Child,
}

impl DaemonFixture {
    fn start(name: &str) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(name)
            .tempdir()
            .expect("temporary directory");
        let workspace = temporary.path().join("workspace");
        let git_index = temporary.path().join("git-index");
        std::fs::create_dir_all(workspace.join("reports")).expect("reports directory");
        std::fs::create_dir_all(workspace.join("repos")).expect("repos directory");
        std::fs::create_dir_all(&git_index).expect("git index directory");

        let ordinary_socket = temporary.path().join("ordinary.sock");
        let owner_socket = temporary.path().join("owner.sock");
        let configuration = DaemonConfiguration::new(
            wire_path(&temporary.path().join("orchestrate.redb")),
            wire_path(&ordinary_socket),
            wire_path(&owner_socket),
            wire_path(&workspace),
            wire_path(&git_index),
        );
        let configuration_path = temporary.path().join("daemon.nota");
        std::fs::write(&configuration_path, encode_nota(&configuration)).expect("config write");

        let child = Command::new(env!("CARGO_BIN_EXE_orchestrate-daemon"))
            .arg(&configuration_path)
            .spawn()
            .expect("daemon spawn");
        let mut fixture = Self {
            _temporary: temporary,
            workspace,
            ordinary_socket,
            owner_socket,
            child,
        };
        fixture.wait_for_sockets();
        fixture
    }

    fn wait_for_sockets(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if self.ordinary_socket.exists() && self.owner_socket.exists() {
                return;
            }
            if let Some(status) = self.child.try_wait().expect("daemon status") {
                panic!("daemon exited before sockets existed: {status}");
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("daemon sockets were not created");
    }

    fn cli(&self, request: impl NotaEncode) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_orchestrate"))
            .env("PERSONA_ORCHESTRATE_SOCKET", &self.ordinary_socket)
            .env("PERSONA_ORCHESTRATE_OWNER_SOCKET", &self.owner_socket)
            .arg(encode_nota(&request))
            .output()
            .expect("cli output")
    }
}

impl Drop for DaemonFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wire_path(path: &Path) -> WirePath {
    WirePath::from_absolute_path(path.to_string_lossy()).expect("wire path")
}

fn role(value: &str) -> RoleName {
    RoleName::from_wire_token(value).expect("role")
}

fn encode_nota(value: &impl NotaEncode) -> String {
    let mut encoder = Encoder::new();
    value.encode(&mut encoder).expect("encode nota");
    encoder.into_string()
}

fn decode_nota<Value: NotaDecode>(bytes: &[u8]) -> Value {
    let text = std::str::from_utf8(bytes).expect("utf8").trim();
    let mut decoder = Decoder::new(text);
    Value::decode(&mut decoder).expect("decode nota")
}

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

#[test]
fn cli_creates_dynamic_role_through_daemon_owner_socket() {
    let fixture = DaemonFixture::start("orchestrate-cli-role");
    let role = role("primary-orchestrate-daemon-zxq9-never-collide");

    let output = fixture.cli(OwnerOrchestrateRequest::Create(CreateRoleOrder {
        role: role.clone(),
        harness: HarnessKind::Codex,
    }));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: OwnerOrchestrateReply = decode_nota(&output.stdout);
    let OwnerOrchestrateReply::RoleCreated(created) = reply else {
        panic!("expected role created");
    };
    assert_eq!(created.role, role);
    assert!(Path::new(created.report_repository_path.as_str()).is_dir());
    assert!(Path::new(created.report_lane_path.as_str()).exists());

    let output = fixture.cli(OrchestrateRequest::Observe(Observation::Roles));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: OrchestrateReply = decode_nota(&output.stdout);
    let OrchestrateReply::RoleSnapshot(snapshot) = reply else {
        panic!("expected role snapshot");
    };
    assert!(snapshot.roles.iter().any(
        |status| status.role.as_wire_token() == "primary-orchestrate-daemon-zxq9-never-collide"
    ));

    let output = fixture.cli(OrchestrateRequest::Claim(RoleClaim {
        role,
        scopes: vec![ScopeReference::Path(
            WirePath::from_absolute_path("/tmp/primary-orchestrate-daemon-zxq9-never-collide")
                .expect("claim path"),
        )],
        reason: ScopeReason::from_text("daemon CLI claim projection").expect("reason"),
    }));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lock_path = fixture
        .workspace
        .join("orchestrate")
        .join("primary-orchestrate-daemon-zxq9-never-collide.lock");
    assert_eq!(
        std::fs::read_to_string(lock_path).expect("lock file"),
        "/tmp/primary-orchestrate-daemon-zxq9-never-collide # daemon CLI claim projection\n"
    );
}

#[test]
fn ordinary_socket_rejects_owner_frame() {
    let fixture = DaemonFixture::start("orchestrate-owner-reject");
    let frame = OwnerOrchestrateFrame::new(OwnerOrchestrateFrameBody::Request {
        exchange: exchange(),
        request: OwnerOrchestrateRequest::Refresh(RefreshRepositoryIndexOrder {}).into_request(),
    });
    let mut stream = UnixStream::connect(&fixture.ordinary_socket).expect("connect ordinary");
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    stream
        .write_all(&frame.encode_length_prefixed().expect("encode frame"))
        .expect("write frame");
    let mut prefix = [0_u8; 4];
    assert!(stream.read_exact(&mut prefix).is_err());
}

#[test]
fn owner_socket_rejects_ordinary_frame() {
    let fixture = DaemonFixture::start("orchestrate-ordinary-reject");
    let frame = OrchestrateFrame::new(OrchestrateFrameBody::Request {
        exchange: exchange(),
        request: OrchestrateRequest::Observe(Observation::Roles).into_request(),
    });
    let mut stream = UnixStream::connect(&fixture.owner_socket).expect("connect owner");
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    stream
        .write_all(&frame.encode_length_prefixed().expect("encode frame"))
        .expect("write frame");
    let mut prefix = [0_u8; 4];
    assert!(stream.read_exact(&mut prefix).is_err());
}

#[test]
fn ordinary_socket_rejects_mismatched_short_header_before_dispatch() {
    let fixture = DaemonFixture::start("orchestrate-ordinary-header");
    let frame = OrchestrateFrame::with_short_header(
        ShortHeader::new(0),
        OrchestrateFrameBody::Request {
            exchange: exchange(),
            request: OrchestrateRequest::Observe(Observation::Roles).into_request(),
        },
    );
    let mut stream = UnixStream::connect(&fixture.ordinary_socket).expect("connect ordinary");
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    stream
        .write_all(&frame.encode_length_prefixed().expect("encode frame"))
        .expect("write frame");
    let mut prefix = [0_u8; 4];
    assert!(stream.read_exact(&mut prefix).is_err());
}

#[test]
fn owner_socket_rejects_mismatched_short_header_before_dispatch() {
    let fixture = DaemonFixture::start("orchestrate-owner-header");
    let frame = OwnerOrchestrateFrame::with_short_header(
        ShortHeader::new(0),
        OwnerOrchestrateFrameBody::Request {
            exchange: exchange(),
            request: OwnerOrchestrateRequest::Refresh(RefreshRepositoryIndexOrder {})
                .into_request(),
        },
    );
    let mut stream = UnixStream::connect(&fixture.owner_socket).expect("connect owner");
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    stream
        .write_all(&frame.encode_length_prefixed().expect("encode frame"))
        .expect("write frame");
    let mut prefix = [0_u8; 4];
    assert!(stream.read_exact(&mut prefix).is_err());
}

#[test]
fn daemon_rejects_non_signal_traffic_on_ordinary_socket() {
    let fixture = DaemonFixture::start("orchestrate-non-signal");
    let mut stream = UnixStream::connect(&fixture.ordinary_socket).expect("connect ordinary");
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("timeout");
    stream.write_all(b"junk").expect("write junk");
    let mut prefix = [0_u8; 4];
    assert!(stream.read_exact(&mut prefix).is_err());
}

#[test]
fn cli_rejects_flag_style_argument_shapes() {
    let output = Command::new(env!("CARGO_BIN_EXE_orchestrate"))
        .args(["--role", "operator"])
        .output()
        .expect("cli output");
    assert!(!output.status.success());
}
