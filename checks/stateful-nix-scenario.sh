#!/usr/bin/env bash
set -euo pipefail

daemon=$1
ordinary_client=$2
meta_client=$3

temporary=$(mktemp -d)
store=$temporary/orchestrate.sema
ordinary_socket=$temporary/ordinary.sock
meta_socket=$temporary/meta.sock
upgrade_socket=$temporary/upgrade.sock
workspace=$temporary/workspace
git_index=$temporary/git-index

"$daemon" "$store" "$ordinary_socket" "$meta_socket" "$upgrade_socket" "$workspace" "$git_index" &
daemon_pid=$!
trap 'kill "$daemon_pid" 2>/dev/null || true; wait "$daemon_pid" 2>/dev/null || true' EXIT

for _ in $(seq 1 100); do
  if [[ -S $ordinary_socket && -S $meta_socket ]]; then
    break
  fi
  sleep 0.05
done
[[ -S $ordinary_socket && -S $meta_socket ]]

ordinary() {
  PERSONA_ORCHESTRATE_SOCKET=$ordinary_socket "$ordinary_client" "$1"
}

meta() {
  PERSONA_ORCHESTRATE_META_SOCKET=$meta_socket "$meta_client" "$1"
}

expect() {
  local reply=$1
  local expected=$2
  [[ $reply == *"$expected"* ]] || {
    printf 'expected %s in reply %s\n' "$expected" "$reply" >&2
    exit 1
  }
}

# Roles: create, duplicate refusal, and explicit retirement.
expect "$(meta '(Create (scenario-role Codex))')" RoleCreated
expect "$(meta '(Create (scenario-role Codex))')" RoleCreationRejected
expect "$(meta '(Retire (Role scenario-role))')" RoleRetired

# Three synthetic lanes establish Fresh, duplicate Fresh refusal, Recovery,
# authority mutation, release, and retirement semantics.
expect "$(meta '(Register ((Alpha alpha ([Alpha Operator] Structural) [alpha state]) Fresh))')" LaneRegistered
expect "$(meta '(Register ((Beta beta ([Beta Operator] Structural) [beta state]) Fresh))')" LaneRegistered
expect "$(meta '(Register ((Gamma gamma ([Gamma Operator] Structural) [gamma state]) Fresh))')" LaneRegistered
expect "$(meta '(Register ((Alpha alpha ([Alpha Operator] Structural) [alpha retry]) Fresh))')" LaneAlreadyRegistered
expect "$(meta '(Register ((Alpha alpha ([Alpha Operator] Structural) [alpha recovery]) Recovery))')" RecoveryInherited
expect "$(meta '(SetAuthority (gamma Support))')" LaneAuthoritySet

# Claims: nested path contention, atomic handoff, and release all use the
# ordinary NOTA client against the actual daemon socket.
expect "$(ordinary '(Claim (alpha [(Path /scenario/shared)] [first claim]))')" ClaimAcceptance
expect "$(ordinary '(Claim (beta [(Path /scenario/shared/file)] [contended claim]))')" ClaimRejection
expect "$(ordinary '(Handoff (alpha beta [(Path /scenario/shared)] [handoff claim]))')" HandoffAcceptance
expect "$(ordinary '(Release beta)')" ReleaseAcknowledgment

# Activity and its filtered pure-read query are durable across daemon restart.
expect "$(ordinary '(Submit (alpha (Task scenario-task) [record activity]))')" ActivityAcknowledgment
expect "$(ordinary '(Query (10 [(TaskToken scenario-task)]))')" scenario-task

# Explicit topic seating creates only durable agent/topic records. The
# returned identifier lets the scenario exercise both routed and rejected
# message outcomes without any external messenger endpoint.
agent_reply=$(ordinary '(RegisterAgent (Alpha [explicit topic agent] Codex (Explicit [coordination]) None))')
expect "$agent_reply" AgentRegistered
agent_identifier=$(printf '%s\n' "$agent_reply" | sed -E 's/^\(AgentRegistered \(([[:alnum:]]+).*/\1/')
[[ $agent_identifier =~ ^[[:alnum:]]+$ ]]
expect "$(ordinary '(Observe Topics)')" coordination
expect "$(ordinary '(Observe (Topic coordination))')" TopicDetail
expect "$(ordinary '(Observe Agents)')" "$agent_identifier"
expect "$(ordinary "(SendOrchestratorMessage ($agent_identifier (Agent $agent_identifier) ((Guidance Standard) [scenario message] [durable route])))")" OrchestratorMessageRouted
expect "$(ordinary "(SendOrchestratorMessage ($agent_identifier Orchestrator (Report escalate [needs coordinator])))")" MissingCoordinator

# Allocation is durable; launch has no configured harness, so the test expects
# the typed refusal rather than starting a real agent process.
expect "$(ordinary '(MintAgentIdentity (Beta [minted agent] Codex))')" AgentIdentityMinted
expect "$(ordinary '(LaunchAgent unknown)')" AgentLaunchRefused

# Watch/unwatch is request state, not a background poller.
expect "$(ordinary '(Watch (True False))')" ObservationOpened
expect "$(ordinary '(Unwatch 1)')" ObservationClosed

# Repositories have no host discovery operation: refresh and observation are
# state-only projections. RequestWorktree refuses rather than creating a tree.
expect "$(meta '(Refresh ())')" RepositoryIndexRefreshed
expect "$(ordinary '(Observe Repositories)')" RepositoriesObserved
expect "$(ordinary '(RequestWorktree (orchestrate scenario alpha [state only]))')" WorktreeRequestRejected

# Declared worktrees are caller-supplied rows. Archive and conclusion change
# only their durable status; no declared path is created or removed.
archived_path=$temporary/declared-archive
expect "$(meta "(RegisterWorktree (orchestrate archive $archived_path alpha Active [state only reservation] 1 Unpushed))")" WorktreeRegistered
expect "$(meta "(ArchiveWorktree $archived_path)")" WorktreeArchived
expect "$(meta '(RefreshWorktreeIndex ())')" WorktreeIndexRefreshed

rejected_path=$temporary/declared-rejected
expect "$(meta "(RegisterWorktree (orchestrate rejected $rejected_path alpha Active [state only conclusion] 1 Unpushed))")" WorktreeRegistered
expect "$(ordinary '(ConcludeWorktree (alpha Rejected))')" WorktreeConcluded

merged_path=$temporary/declared-merged
expect "$(meta "(RegisterWorktree (orchestrate merged $merged_path beta Active [state only conclusion] 1 Unpushed))")" WorktreeRegistered
expect "$(ordinary '(ConcludeWorktree (beta Merged))')" WorktreeConcluded
worktrees=$(ordinary '(Observe Worktrees)')
expect "$worktrees" Archived
expect "$worktrees" Years.

# A valid request whose topic does not exist returns the contract's partial
# outcome. It witnesses partial-reply presentation without assuming a cause.
expect "$(ordinary '(Observe (Topic absent-topic))')" PartialApplied

# Stop and restart the packaged daemon over the same isolated store, then
# prove that durable state survived and host paths were merely declared.
kill "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
trap - EXIT
rm -f "$ordinary_socket" "$meta_socket" "$upgrade_socket"
"$daemon" "$store" "$ordinary_socket" "$meta_socket" "$upgrade_socket" "$workspace" "$git_index" &
daemon_pid=$!
trap 'kill "$daemon_pid" 2>/dev/null || true; wait "$daemon_pid" 2>/dev/null || true' EXIT
for _ in $(seq 1 100); do
  [[ -S $ordinary_socket && -S $meta_socket ]] && break
  sleep 0.05
done
expect "$(ordinary '(Query (10 [(TaskToken scenario-task)]))')" scenario-task
expect "$(ordinary '(Observe Worktrees)')" "$rejected_path"
[[ ! -e $archived_path && ! -e $rejected_path && ! -e $merged_path ]]
