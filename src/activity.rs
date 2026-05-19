use signal_persona_orchestrate::{
    ActivityAcknowledgment, ActivityFilter, ActivityList, ActivityQuery, ActivitySubmission,
    OrchestrateReply, ScopeReference,
};

use crate::{OrchestrateTables, Result, StoredActivity};

pub struct ActivityLedger<'tables> {
    tables: &'tables OrchestrateTables,
}

impl<'tables> ActivityLedger<'tables> {
    pub fn new(tables: &'tables OrchestrateTables) -> Self {
        Self { tables }
    }

    pub fn submit(&self, submission: ActivitySubmission) -> Result<OrchestrateReply> {
        let activity =
            self.tables
                .append_activity(submission.role, submission.scope, submission.reason)?;

        Ok(OrchestrateReply::ActivityAcknowledgment(
            ActivityAcknowledgment {
                slot: activity.slot,
            },
        ))
    }

    pub fn query(&self, query: ActivityQuery) -> Result<OrchestrateReply> {
        let limit = query.limit as usize;
        let mut records = self.tables.activity_records()?;
        records.sort_by_key(|activity| activity.slot);
        records.reverse();

        let records = records
            .into_iter()
            .filter(|activity| ActivityPredicate::new(&query.filters).matches(activity))
            .take(limit)
            .map(StoredActivity::into_activity)
            .collect();

        Ok(OrchestrateReply::ActivityList(ActivityList { records }))
    }
}

struct ActivityPredicate<'filters> {
    filters: &'filters [ActivityFilter],
}

impl<'filters> ActivityPredicate<'filters> {
    fn new(filters: &'filters [ActivityFilter]) -> Self {
        Self { filters }
    }

    fn matches(&self, activity: &StoredActivity) -> bool {
        self.filters
            .iter()
            .all(|filter| ActivityFilterMatch::new(filter).matches(activity))
    }
}

struct ActivityFilterMatch<'filter> {
    filter: &'filter ActivityFilter,
}

impl<'filter> ActivityFilterMatch<'filter> {
    fn new(filter: &'filter ActivityFilter) -> Self {
        Self { filter }
    }

    fn matches(&self, activity: &StoredActivity) -> bool {
        match self.filter {
            ActivityFilter::RoleFilter(role) => &activity.role == role,
            ActivityFilter::PathPrefix(prefix) => match &activity.scope {
                ScopeReference::Path(path) => path_matches_prefix(path.as_str(), prefix.as_str()),
                ScopeReference::Task(_) => false,
            },
            ActivityFilter::TaskToken(token) => match &activity.scope {
                ScopeReference::Path(_) => false,
                ScopeReference::Task(activity_token) => activity_token == token,
            },
        }
    }
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|tail| tail.starts_with('/'))
}
