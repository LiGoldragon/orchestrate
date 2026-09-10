//! Process-level proof for the privileged Datom client boundary.

use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    path::PathBuf,
    process::{Command, Output},
};

use meta_signal_orchestrate::{
    ByteViewable, Configure, Query, Response, Restorable, Signal, Signalizable,
};

struct ClientHarness {
    _directory: tempfile::TempDir,
    socket: PathBuf,
}

trait CreatesClientHarness: Sized {
    fn create() -> Self;
    fn invoke(&self, argument: Option<&str>) -> Output;
    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query>;
}

impl CreatesClientHarness for ClientHarness {
    fn create() -> Self {
        let directory = tempfile::tempdir().expect("temporary meta client");
        let socket = directory.path().join("meta.sock");
        Self {
            _directory: directory,
            socket,
        }
    }

    fn invoke(&self, argument: Option<&str>) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_orchestrate-meta"));
        command.env("ORCHESTRATE_META_SOCKET", &self.socket);
        if let Some(argument) = argument {
            command.arg(argument);
        }
        command.output().expect("run meta client")
    }

    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query> {
        let listener = UnixListener::bind(&self.socket).expect("bind meta fixture socket");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept meta client");
            let mut prefix = [0; 4];
            stream.read_exact(&mut prefix).expect("read query length");
            let mut bytes = vec![0; u32::from_le_bytes(prefix) as usize];
            stream.read_exact(&mut bytes).expect("read query");
            let query = Signal::<Query>::from(bytes)
                .restore()
                .expect("restore meta query");
            let signal = response.signalize().expect("archive meta response");
            let length = u32::try_from(signal.bytes().len()).expect("fixture response length");
            stream
                .write_all(&length.to_le_bytes())
                .expect("write response length");
            stream.write_all(signal.bytes()).expect("write response");
            query
        })
    }
}

#[test]
fn client_actualizes_datoms_and_textualizes_typed_responses() {
    let harness = ClientHarness::create();
    let configure = Configure {
        ordinary_socket_path: "/tmp/ordinary.sock".to_owned(),
        meta_socket_path: "/tmp/meta.sock".to_owned(),
    };
    let server = harness.answer_once(Response::Configured(configure.clone()));
    let output = harness.invoke(Some("Configure.{ «/tmp/ordinary.sock» «/tmp/meta.sock» }"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "Configured.{ «/tmp/ordinary.sock» «/tmp/meta.sock» }\n"
    );
    assert_eq!(
        server.join().expect("meta fixture server"),
        Query::Configure(configure),
    );
}

#[test]
fn client_describes_contracts_and_reports_datom_failures() {
    let harness = ClientHarness::create();
    let description = harness.invoke(None);
    assert!(description.status.success());
    let stdout = String::from_utf8(description.stdout).unwrap();
    assert!(stdout.contains("Signal"));
    assert!(stdout.contains("Library"));

    let malformed = harness.invoke(Some("Configure.{ broken"));
    assert!(!malformed.status.success());
    let stderr = String::from_utf8(malformed.stderr).unwrap();
    assert!(stderr.starts_with("Unreadable."), "{stderr}");
}
