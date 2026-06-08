use orchestrate::OrchestrateDaemon;
use orchestrate::schema::daemon::DaemonEntry;

fn main() -> std::process::ExitCode {
    <OrchestrateDaemon as DaemonEntry>::run_to_exit_code()
}
