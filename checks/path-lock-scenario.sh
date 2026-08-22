#!/usr/bin/env bash
set -euo pipefail

daemon=$1
client=$2

temporary=$(mktemp -d)
store=$temporary/path-locks.sema
ordinary_socket=$temporary/ordinary.sock
meta_socket=$temporary/meta.sock
upgrade_socket=$temporary/upgrade.sock
existing_path=$temporary/existing-marker
absent_path=$temporary/must-stay-absent
daemon_pid=
watcher_pid=

cleanup() {
  local status=$?
  if [[ -n $daemon_pid ]]; then
    kill "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
  if [[ -n $watcher_pid ]]; then
    kill "$watcher_pid" 2>/dev/null || true
    wait "$watcher_pid" 2>/dev/null || true
  fi
  rm -rf "$temporary"
  return "$status"
}
trap cleanup EXIT

await_sockets() {
  local ordinary_seen=false
  local meta_seen=false
  local upgrade_seen=false
  local created=
  while IFS= read -r created <&"${SocketEvents[0]}"; do
    case $created in
      ordinary.sock) ordinary_seen=true ;;
      meta.sock) meta_seen=true ;;
      upgrade.sock) upgrade_seen=true ;;
    esac
    if [[ $ordinary_seen == true && $meta_seen == true && $upgrade_seen == true ]]; then
      return
    fi
  done
  printf 'daemon socket event stream ended before all sockets were bound\n' >&2
  return 1
}

start_daemon() {
  coproc SocketEvents { inotifywait --monitor --quiet --event create --format '%f' "$temporary"; }
  watcher_pid=$SocketEvents_PID
  "$daemon" "$store" "$ordinary_socket" "$meta_socket" "$upgrade_socket" &
  daemon_pid=$!
  await_sockets
  kill "$watcher_pid"
  wait "$watcher_pid" 2>/dev/null || true
  watcher_pid=
}

stop_daemon() {
  kill "$daemon_pid"
  wait "$daemon_pid" 2>/dev/null || true
  daemon_pid=
  rm -f "$ordinary_socket" "$meta_socket" "$upgrade_socket"
}

register() {
  ORCHESTRATE_ORDINARY_SOCKET=$ordinary_socket "$client" "$1"
}

printf 'untouched' > "$existing_path"
request="PathLock.{daemonFirst [$existing_path $absent_path] (daemon registration)}"
expected_registered="PathLockRegistered.{PathLock.{daemonFirst [$existing_path $absent_path] (daemon registration)}}"

start_daemon
[[ -S $ordinary_socket && -S $meta_socket && -S $upgrade_socket ]]
[[ $(register "$request") == "$expected_registered" ]]
[[ $(<"$existing_path") == untouched ]]
[[ ! -e $absent_path ]]

stop_daemon
start_daemon
duplicate="PathLock.{daemonFirst [$temporary/other] (duplicate name)}"
expected_duplicate="PathLockRegistrationRejected.{PathLock.{daemonFirst [$temporary/other] (duplicate name)} DuplicateActiveName.{PathLock.{daemonFirst [$existing_path $absent_path] (daemon registration)}}}"
[[ $(register "$duplicate") == "$expected_duplicate" ]]
[[ $(<"$existing_path") == untouched ]]
[[ ! -e $absent_path ]]
