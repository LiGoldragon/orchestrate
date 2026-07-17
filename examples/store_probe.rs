//! Read-only probe: open a store at a path and report what survived.
//!
//! Usage: `store_probe <store-path>`. Prints either the open error (the
//! reproduction path) or the surviving record counts plus every lane
//! assignment (the migration proof). Read-only: it opens, reads, and drops.

use orchestrate::{OrchestrateTables, StoreLocation};

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: store_probe <store-path>");
    let location = StoreLocation::new(path.clone());
    match OrchestrateTables::open(&location) {
        Ok(tables) => {
            let lanes = tables.lane_records().expect("read lanes");
            let claims = tables.claim_records().expect("read claims");
            let agents = tables
                .orchestrator_agent_records()
                .expect("read agent registry");
            println!(
                "OPEN OK: {} lanes, {} claims, {} agents",
                lanes.len(),
                claims.len(),
                agents.len()
            );
            for lane in &lanes {
                println!("  lane {:?} status={:?}", lane.assignment, lane.status);
            }
        }
        Err(error) => {
            println!("OPEN ERR: {error:?}");
        }
    }
}
