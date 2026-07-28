#!/usr/bin/env bash
set -euo pipefail

daemon=$1
ordinary_client=$2
meta_client=$3
nota_assert=$4
upgrade_scenario=$5
workflow_fixtures=$6

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
  if [[ -S $ordinary_socket && -S $meta_socket && -S $upgrade_socket ]]; then
    break
  fi
  sleep 0.05
done
[[ -S $ordinary_socket && -S $meta_socket && -S $upgrade_socket ]]

ordinary() {
  PERSONA_ORCHESTRATE_SOCKET=$ordinary_socket "$ordinary_client" "(Explicit (Canonical $1))"
}

meta() {
  PERSONA_ORCHESTRATE_META_SOCKET=$meta_socket "$meta_client" "$1"
}

expect() {
  local reply=$1
  local expected=$2
  local tier=ordinary
  case $expected in
    RoleCreated|RoleCreationRejected|RoleRetired|RepositoryIndexRefreshed|LaneRegistered|LaneAlreadyRegistered|LaneUnregistered|SessionCleared|LaneRetired|LaneAuthoritySet|WorktreeRegistered|WorktreeIndexRefreshed|WorktreeArchived)
      tier=meta
      ;;
  esac
  "$nota_assert" "$tier" "$expected" "$reply"
}

ordinary_rejected() {
  if ordinary "$1" >/dev/null 2>&1; then
    printf 'expected ordinary request to be rejected: %s\n' "$1" >&2
    exit 1
  fi
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
expect "$(meta '(Register ((Alpha alpha ([Alpha Operator] Structural) [alpha recovery]) Recovery))')" LaneAlreadyRegistered
expect "$(meta '(SetAuthority (gamma Support))')" LaneAuthoritySet

# Claims: nested path contention, atomic handoff, and release all use the
# ordinary NOTA client against the actual daemon socket.
expect "$(ordinary '(Claim (alpha [(Path /scenario/shared)] [first claim]))')" ClaimAcceptance
expect "$(ordinary '(Claim (beta [(Path /scenario/shared/file)] [contended claim]))')" ClaimRejection
expect "$(ordinary '(Handoff (alpha beta [(Path /scenario/shared)] [handoff claim]))')" HandoffAcceptance
expect "$(ordinary '(Handoff (gamma beta [(Path /scenario/shared)] [missing source]))')" HandoffRejection
expect "$(ordinary '(Release beta)')" ReleaseAcknowledgment

# Activity and its filtered pure-read query are durable across daemon restart.
expect "$(ordinary '(Submit (alpha (Task scenario-task) [record activity]))')" ActivityAcknowledgment
expect "$(ordinary '(Query (10 [(TaskToken scenario-task)]))')" ActivityList

# Explicit topic seating creates only durable agent/topic records. The
# returned identifier lets the scenario exercise both routed and rejected
# message outcomes without any external messenger endpoint.
agent_reply=$(ordinary '(RegisterAgent (Alpha [explicit topic agent] Codex (Explicit [coordination]) None))')
expect "$agent_reply" AgentRegistered
agent_identifier=$("$nota_assert" ordinary-identifier unused "$agent_reply")
[[ $agent_identifier =~ ^[[:alnum:]]+$ ]]
expect "$(ordinary '(Observe Topics)')" TopicTree
expect "$(ordinary '(Observe (Topic coordination))')" TopicDetail
expect "$(ordinary '(Observe Agents)')" AgentDirectory
expect "$(ordinary "(SendOrchestratorMessage ($agent_identifier (Agent $agent_identifier) ((Guidance Standard) [scenario message] [durable route])))")" OrchestratorMessageRouted
expect "$(ordinary "(SendOrchestratorMessage ($agent_identifier Orchestrator (Report escalate [needs coordinator])))")" OrchestratorMessageRejected

# Allocation is durable; launch has no configured harness, so the test expects
# the typed refusal rather than starting a real agent process.
minted_reply=$(ordinary '(MintAgentIdentity (Beta [minted agent] Codex))')
expect "$minted_reply" AgentIdentityMinted
minted_identifier=$("$nota_assert" ordinary-identifier unused "$minted_reply")
expect "$(ordinary '(LaunchAgent unknown)')" AgentLaunchRefused:UnknownAgent
expect "$(ordinary "(LaunchAgent $minted_identifier)")" AgentLaunchRefused:HarnessUnreachable
expect "$(ordinary '(RegisterAgent (Beta [automatic topic] Codex Automatic None))')" AgentRegistrationRejected:JudgeUnavailable

# Watch/unwatch is request state, not a background poller.
expect "$(ordinary '(Watch (True False))')" ObservationOpened
expect "$(ordinary '(Unwatch 1)')" ObservationClosed

# Typed fixture generation keeps the workflow inputs aligned to the pinned
# contract. The resolved form correctly refuses without its separate
# meta-harness; the fixture runner executes without one.
mapfile -t workflow_inputs < <("$workflow_fixtures")
expect "$(ordinary "${workflow_inputs[0]}")" WorkflowReceiptProduced
expect "$(ordinary "${workflow_inputs[1]}")" WorkflowRunObservationOpened
expect "$(ordinary "${workflow_inputs[2]}")" WorkflowRunObservationClosed
ordinary_rejected "${workflow_inputs[3]}"

# Repositories have no host discovery operation: refresh and observation are
# state-only projections. RequestWorktree refuses rather than creating a tree.
expect "$(meta '(Refresh ())')" RepositoryIndexRefreshed
expect "$(ordinary '(Observe Repositories)')" RepositoriesObserved
expect "$(ordinary '(RequestWorktree (orchestrate scenario alpha [state only]))')" WorktreeRequestRejected

# Every durable observation selector is an ordinary canonical NOTA read.
expect "$(ordinary '(Observe Roles)')" RoleSnapshot
expect "$(ordinary '(Observe Sessions)')" SessionsObserved
expect "$(ordinary '(Observe (SessionLanes Alpha))')" LanesObserved
expect "$(ordinary '(Observe Lanes)')" LanesObserved

# Declared worktrees are caller-supplied rows. Archive and conclusion change
# only their durable status; no declared path is created or removed.
archived_path=$temporary/declared-archive
expect "$(meta "(RegisterWorktree (orchestrate archive $archived_path alpha Active [state only reservation] 1 Unpushed))")" WorktreeRegistered
expect "$(meta "(ArchiveWorktree $archived_path)")" WorktreeArchived
expect "$(meta '(RefreshWorktreeIndex ())')" WorktreeIndexRefreshed

rejected_path=$temporary/declared-rejected
expect "$(meta "(RegisterWorktree (orchestrate rejected $rejected_path alpha Active [state only conclusion] 1 Unpushed))")" WorktreeRegistered
expect "$(ordinary '(ConcludeWorktree (alpha Rejected))')" WorktreeConcluded
ordinary_rejected '(ConcludeWorktree (gamma Merged))'

merged_path=$temporary/declared-merged
expect "$(meta "(RegisterWorktree (orchestrate merged $merged_path beta Active [state only conclusion] 1 Unpushed))")" WorktreeRegistered
expect "$(ordinary '(ConcludeWorktree (beta Merged))')" WorktreeConcluded
expect "$(ordinary '(Observe Worktrees)')" WorktreesObserved

# A valid request whose topic does not exist returns the contract's partial
# outcome. It witnesses partial-reply presentation without assuming a cause.
expect "$(ordinary '(Observe (Topic absent-topic))')" PartialApplied

# Explicit terminal lifecycle remains caller-directed; it is never age-driven.
expect "$(meta '(Unregister (Gamma gamma [terminal lane]))')" LaneUnregistered
expect "$(meta '(Retire (Lane gamma))')" LaneRetired
expect "$(meta '(ClearSession (Beta [clear beta session]))')" SessionCleared

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
  [[ -S $ordinary_socket && -S $meta_socket && -S $upgrade_socket ]] && break
  sleep 0.05
done
expect "$(ordinary '(Query (10 [(TaskToken scenario-task)]))')" ActivityList
expect "$(ordinary '(Observe Worktrees)')" WorktreesObserved
expect "$(ordinary '(Observe Topics)')" TopicTree
expect "$(ordinary '(Observe Agents)')" AgentDirectory
expect "$(ordinary '(Observe Lanes)')" LanesObserved
[[ ! -e $archived_path && ! -e $rejected_path && ! -e $merged_path ]]

# Exercise every version-handover operation through the packaged upgrade
# socket. Finalization deliberately runs last because it retires public tiers.
"$upgrade_scenario" "$upgrade_socket"
