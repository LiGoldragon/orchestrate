#!/usr/bin/env bash
set -euo pipefail

daemon=$1
ordinary_client=$2
meta_client=$3
nota_assert=$4
upgrade_scenario=$5
workflow_fixtures=$6
workflow_harness=$7
store_assert=$8

temporary=$(mktemp -d)
store=$temporary/orchestrate.sema
ordinary_socket=$temporary/ordinary.sock
meta_socket=$temporary/meta.sock
upgrade_socket=$temporary/upgrade.sock
harness_socket=$temporary/meta-harness.sock
workspace=$temporary/workspace
git_index=$temporary/git-index
daemon_pid=
harness_pid=
harness_meta_socket=

cleanup() {
  local status=$?
  if [[ -n $daemon_pid ]]; then
    kill "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
  if [[ -n $harness_pid ]]; then
    kill "$harness_pid" 2>/dev/null || true
    wait "$harness_pid" 2>/dev/null || true
  fi
  return "$status"
}
trap cleanup EXIT

wait_for_daemon() {
  for _ in $(seq 1 100); do
    if [[ -S $ordinary_socket && -S $meta_socket && -S $upgrade_socket ]]; then
      return
    fi
    sleep 0.05
  done
  printf 'daemon did not bind its three sockets\n' >&2
  exit 1
}

start_daemon() {
  if [[ -n $harness_meta_socket ]]; then
    HARNESS_META_SOCKET=$harness_meta_socket "$daemon" "$store" "$ordinary_socket" "$meta_socket" "$upgrade_socket" "$workspace" "$git_index" &
  else
    "$daemon" "$store" "$ordinary_socket" "$meta_socket" "$upgrade_socket" "$workspace" "$git_index" &
  fi
  daemon_pid=$!
  wait_for_daemon
}

stop_daemon() {
  kill "$daemon_pid" 2>/dev/null || true
  wait "$daemon_pid" 2>/dev/null || true
  daemon_pid=
  rm -f "$ordinary_socket" "$meta_socket" "$upgrade_socket"
}

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
    RoleCreated|RoleCreationRejected*|RoleRetired|RepositoryIndexRefreshed|LaneRegistered|LaneAlreadyRegistered*|LaneUnregistered|SessionCleared|LaneRetired|LaneAuthoritySet|WorktreeRegistered|WorktreeIndexRefreshed|WorktreeArchived*)
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

meta_rejected() {
  if meta "$1" >/dev/null 2>&1; then
    printf 'expected meta request to be rejected: %s\n' "$1" >&2
    exit 1
  fi
}

start_daemon

# Roles: create, duplicate refusal, and explicit retirement.
expect "$(meta '(Create (scenario-role Codex))')" RoleCreated
expect "$(meta '(Create (scenario-role Codex))')" RoleCreationRejected:RoleAlreadyExists
expect "$(meta '(Retire (Role scenario-role))')" RoleRetired

# Three synthetic lanes establish Fresh, duplicate Fresh refusal, Recovery,
# authority mutation, release, and retirement semantics.
expect "$(meta '(Register ((Alpha alpha ([Alpha Operator] Structural) [alpha state]) Fresh))')" LaneRegistered
expect "$(meta '(Register ((Beta beta ([Beta Operator] Structural) [beta state]) Fresh))')" LaneRegistered
expect "$(meta '(Register ((Gamma gamma ([Gamma Operator] Structural) [gamma state]) Fresh))')" LaneRegistered
expect "$(meta '(Register ((Alpha alpha ([Alpha Operator] Structural) [alpha retry]) Fresh))')" LaneAlreadyRegistered:FreshConflict
expect "$(meta '(Register ((Alpha alpha ([Alpha Operator] Structural) [alpha recovery]) Recovery))')" LaneAlreadyRegistered:RecoveryInherited
expect "$(meta '(SetAuthority (gamma Support))')" LaneAuthoritySet

# Claims: the response fields prove the nested conflict and handoff direction;
# the last claim is deliberately retained for the restart-store witness.
expect "$(ordinary '(Claim (alpha [(Path /scenario/shared)] [first claim]))')" ClaimAcceptance
expect "$(ordinary '(Claim (beta [(Path /scenario/shared/file)] [contended claim]))')" ClaimRejection:NestedScope
expect "$(ordinary '(Handoff (alpha beta [(Path /scenario/shared)] [handoff claim]))')" HandoffAcceptance:AlphaToBeta
expect "$(ordinary '(Handoff (gamma beta [(Path /scenario/shared)] [missing source]))')" HandoffRejection:SourceRoleDoesNotHold
expect "$(ordinary '(Release beta)')" ReleaseAcknowledgment
expect "$(ordinary '(Claim (alpha [(Path /scenario/restart-claim)] [restart claim]))')" ClaimAcceptance

# Activity's stored slot and task token are checked after the daemon restart.
expect "$(ordinary '(Submit (alpha (Task scenario-task) [record activity]))')" ActivityAcknowledgment:FirstSlot
expect "$(ordinary '(Query (10 [(TaskToken scenario-task)]))')" ActivityList:ScenarioActivity

# Explicit topic seating creates only durable agent/topic records. The returned
# identifier is subsequently used for both durable routed and rejected triage.
agent_reply=$(ordinary '(RegisterAgent (Alpha [explicit topic agent] Codex (Explicit [coordination]) None))')
expect "$agent_reply" AgentRegistered
agent_identifier=$("$nota_assert" ordinary-identifier unused "$agent_reply")
[[ $agent_identifier =~ ^[[:alnum:]]+$ ]]
expect "$(ordinary '(Observe Topics)')" TopicTree
expect "$(ordinary '(Observe (Topic coordination))')" TopicDetail
expect "$(ordinary '(Observe Agents)')" AgentDirectory
expect "$(ordinary "(SendOrchestratorMessage ($agent_identifier (Agent $agent_identifier) ((Guidance Standard) [scenario message] [durable route])))")" OrchestratorMessageRouted:FirstReceipt
expect "$(ordinary "(SendOrchestratorMessage ($agent_identifier Orchestrator (Report escalate [needs coordinator])))")" OrchestratorMessageRejected:MissingCoordinator

# Allocation is durable; launch has no configured harness, so the test expects
# typed refusals instead of starting any real agent process.
minted_reply=$(ordinary '(MintAgentIdentity (Beta [minted agent] Codex))')
expect "$minted_reply" AgentIdentityMinted
minted_identifier=$("$nota_assert" ordinary-identifier unused "$minted_reply")
expect "$(ordinary '(LaunchAgent unknown)')" AgentLaunchRefused:UnknownAgent
expect "$(ordinary "(LaunchAgent $minted_identifier)")" AgentLaunchRefused:HarnessUnreachable
expect "$(ordinary '(RegisterAgent (Beta [automatic topic] Codex Automatic None))')" AgentRegistrationRejected:JudgeUnavailable

# Watch/unwatch is request state, not a background poller.
expect "$(ordinary '(Watch (True False))')" ObservationOpened
expect "$(ordinary '(Unwatch 1)')" ObservationClosed

# The fixture keeps the absent-harness transport refusal separate from the two
# real-protocol fake-harness replies exercised after the restart.
mapfile -t workflow_inputs < <("$workflow_fixtures")
expect "$(ordinary "${workflow_inputs[0]}")" WorkflowReceiptProduced
expect "$(ordinary "${workflow_inputs[1]}")" WorkflowRunObservationOpened
expect "$(ordinary "${workflow_inputs[2]}")" WorkflowRunObservationClosed
ordinary_rejected "${workflow_inputs[3]}"

# Repositories have no host discovery operation: refresh and observation are
# state-only projections. RequestWorktree refuses rather than creating a tree.
expect "$(meta '(Refresh ())')" RepositoryIndexRefreshed
expect "$(ordinary '(Observe Repositories)')" RepositoriesObserved
expect "$(ordinary '(RequestWorktree (orchestrate scenario alpha [state only]))')" WorktreeRequestRejected:RepositoryNotFound

# Every durable observation selector is an ordinary canonical NOTA read.
expect "$(ordinary '(Observe Roles)')" RoleSnapshot
expect "$(ordinary '(Observe Sessions)')" SessionsObserved
expect "$(ordinary '(Observe (SessionLanes Alpha))')" LanesObserved
expect "$(ordinary '(Observe Lanes)')" LanesObserved

# Declared worktrees are caller-supplied rows. Archive and conclusion change
# only durable status; no declared path is created or removed.
archived_path=$temporary/declared-archive
expect "$(meta "(RegisterWorktree (orchestrate archive $archived_path alpha Active [state only reservation] 1 Unpushed))")" WorktreeRegistered
expect "$(meta "(ArchiveWorktree $archived_path)")" WorktreeArchived:Archived
expect "$(meta '(RefreshWorktreeIndex ())')" WorktreeIndexRefreshed

rejected_path=$temporary/declared-rejected
expect "$(meta "(RegisterWorktree (orchestrate rejected $rejected_path alpha Active [state only conclusion] 1 Unpushed))")" WorktreeRegistered
expect "$(ordinary '(ConcludeWorktree (alpha Rejected))')" WorktreeConcluded:Archived
expect "$(ordinary '(ConcludeWorktree (gamma Merged))')" PartialApplied

ambiguous_one=$temporary/declared-ambiguous-one
ambiguous_two=$temporary/declared-ambiguous-two
expect "$(meta "(RegisterWorktree (orchestrate ambiguous-one $ambiguous_one gamma Active [ambiguous conclusion] 1 Unpushed))")" WorktreeRegistered
expect "$(meta "(RegisterWorktree (orchestrate ambiguous-two $ambiguous_two gamma Active [ambiguous conclusion] 1 Unpushed))")" WorktreeRegistered
expect "$(ordinary '(ConcludeWorktree (gamma Merged))')" PartialApplied

merged_path=$temporary/declared-merged
expect "$(meta "(RegisterWorktree (orchestrate merged $merged_path beta Active [state only conclusion] 1 Unpushed))")" WorktreeRegistered
expect "$(ordinary '(ConcludeWorktree (beta Merged))')" WorktreeConcluded:Merged
expect "$(ordinary '(Observe Worktrees)')" WorktreesObserved

# A valid request whose topic does not exist returns the contract's partial
# outcome. It witnesses partial-reply presentation without assuming a cause.
expect "$(ordinary '(Observe (Topic absent-topic))')" PartialApplied

# Explicit terminal lifecycle remains caller-directed; it is never age-driven.
expect "$(meta '(Unregister (Gamma gamma [terminal lane]))')" LaneUnregistered
expect "$(meta '(Retire (Lane gamma))')" LaneRetired
expect "$(meta '(ClearSession (Beta [clear beta session]))')" SessionCleared

# Restart over the same store. The fake harness uses the same framed
# meta-harness protocol as production and emits one accepted then one
# unavailable resolution. The earlier absent-harness refusal remains covered.
stop_daemon
"$workflow_harness" "$harness_socket" &
harness_pid=$!
for _ in $(seq 1 100); do
  [[ -S $harness_socket ]] && break
  sleep 0.05
done
[[ -S $harness_socket ]]
harness_meta_socket=$harness_socket
start_daemon

expect "$(ordinary '(Query (10 [(TaskToken scenario-task)]))')" ActivityList:ScenarioActivity
expect "$(ordinary '(Observe Worktrees)')" WorktreesObserved
expect "$(ordinary '(Observe Topics)')" TopicTree
expect "$(ordinary '(Observe Agents)')" AgentDirectory
expect "$(ordinary '(Observe Lanes)')" LanesObserved
expect "$(ordinary "${workflow_inputs[4]}")" WorkflowResolutionAccepted:FakeHarness
expect "$(ordinary "${workflow_inputs[5]}")" WorkflowResolutionUnavailable:FakeHarness
wait "$harness_pid"
harness_pid=
[[ ! -e $archived_path && ! -e $rejected_path && ! -e $merged_path ]]

# The direct typed table assertion runs only after the restarted daemon has
# released the isolated store. It verifies every exact durable field that has
# no public observation surface, including triage and workflow rows.
stop_daemon
harness_meta_socket=
"$store_assert" "$store" "$agent_identifier" "$archived_path" "$merged_path"

# The same complete assertion must reject a newly initialized empty store; a
# route-only check would not satisfy this negative witness.
empty_store=$temporary/empty.sema
if "$store_assert" "$empty_store" "$agent_identifier" "$archived_path" "$merged_path" >/dev/null 2>&1; then
  printf 'fresh empty store unexpectedly satisfied restart assertion\n' >&2
  exit 1
fi

# A framed mirror replaces target lane/claim rows with known source rows.
# That deliberate conflicting snapshot also makes the handoff conflict reply
# reachable through the actual version-handover protocol.
start_daemon
"$upgrade_scenario" "$upgrade_socket" prepare
expect "$(ordinary '(Claim (mirror-target [(Path /scenario/commit-advanced)] [advance handover marker]))')" ClaimAcceptance
"$upgrade_scenario" "$upgrade_socket" commit-advanced
expect "$(ordinary '(Observe Lanes)')" LanesObserved:MirrorRestored
expect "$(ordinary '(Handoff (mirror-source mirror-target [(Path /scenario/mirror-handoff)] [target conflict]))')" HandoffRejection:TargetRoleConflict

# Finalization retires both public tiers, leaves the upgrade tier bound, and
# proves its post-finalization recovery result without touching live sockets.
"$upgrade_scenario" "$upgrade_socket" finalize
[[ ! -S $ordinary_socket && ! -S $meta_socket && -S $upgrade_socket ]]
ordinary_rejected '(Observe Lanes)'
meta_rejected '(Refresh ())'
stop_daemon
