use crate::CrawlResult;
use anyhow::{Error, Ok};
use chrono::{DateTime, Utc};
use log::{debug, trace};
use neo4rs::{query, Graph, Query};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, WeakUnboundedSender};
use tokio::sync::Mutex;

#[derive(new)]
pub struct GraphStoreParams {
    merged_instances: Mutex<HashSet<String>>,
    graph: Arc<Graph>,
    index_time: DateTime<Utc>,
}

#[derive(new)]
pub struct GraphStoreJob {
    params: Arc<GraphStoreParams>,
    crawl_result: Arc<CrawlResult>,
}

impl std::fmt::Debug for GraphStoreJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "GraphStoreJob {{ crawl_result: {:?}, params: ... }}",
            self.crawl_result
        ))
    }
}

#[derive(Clone, Copy)]
enum InstanceRelation {
    Allows,
    Blocks,
    Linked,
}

impl GraphStoreJob {
    pub async fn update_graph(&self) -> Result<(), Error> {
        let Some(federated) = self.crawl_result.site_info.federated_instances.as_ref()
        else {
            return Ok(());
        };

        let mut queries: Vec<Query> = Vec::new();
        let build_queries = |other_domains: &[String], relation: InstanceRelation| {
            futures::future::join_all(
                other_domains
                    .iter()
                    .map(|domain| self.build_relation_query(domain.to_owned(), relation)),
            )
        };
        if let Some(ref allowed) = federated.allowed {
            queries.extend(build_queries(allowed, InstanceRelation::Allows).await);
        }
        if let Some(ref blocked) = federated.blocked {
            queries.extend(build_queries(blocked, InstanceRelation::Blocks).await);
        }
        queries.extend(build_queries(&federated.linked, InstanceRelation::Linked).await);

        for q in queries.into_iter() {
            self.params.graph.run(q).await?;
        }

        Ok(())
    }

    /// Build a query for creating a relationship of type `relation` between
    /// the current result instance and `other`.
    async fn build_relation_query(&self, other: String, relation: InstanceRelation) -> Query {
        // To avoid DB deadlocks we make sure to only MERGE once per instance,
        // and MATCH the rest of the time.
        // TODO: this did not seem to help with deadlocks
        let self_op = {
            let mut merged_instances = self.params.merged_instances.lock().await;
            match merged_instances.insert(self.crawl_result.domain.clone()) {
                false => "MATCH",
                true => "MERGE",
            }
        };
        let other_op = {
            let mut merged_instances = self.params.merged_instances.lock().await;
            match merged_instances.insert(other.to_owned()) {
                false => "MATCH",
                true => "MERGE",
            }
        };

        query(
            format!(
                "
                {self_op}  (self:Instance  {{domain: $self}})
                {other_op} (other:Instance {{domain: $other}})
                MERGE (self)-[:{relation} {{index_time_utc: $time}}]->(other)
                SET self = {{
                    domain: $self,
                    detail_time_utc: $time,
                    software_name: $software_name,
                    software_version: $software_version,
                    open_registrations: $open_registrations,
                    users_total: $users_total,
                    users_active_month: $users_active_month,
                    users_active_halfyear: $users_active_halfyear,
                    posts: $posts,
                    comments: $comments
                }}
                SET other.link_time_utc = $time
            ",
                relation = match relation {
                    InstanceRelation::Allows => "ALLOWS",
                    InstanceRelation::Blocks => "BLOCKS",
                    InstanceRelation::Linked => "LINKED",
                }
            )
            .as_str(),
        )
        .param("self", self.crawl_result.domain.as_str())
        .param("other", other)
        .param(
            "software_name",
            self.crawl_result.node_info.software.name.as_str(),
        )
        .param(
            "software_version",
            self.crawl_result.node_info.software.version.as_str(),
        )
        .param(
            "open_registrations",
            self.crawl_result.node_info.open_registrations,
        )
        .param("users_total", self.crawl_result.node_info.usage.users.total)
        .param(
            "users_active_month",
            self.crawl_result.node_info.usage.users.active_month,
        )
        .param(
            "users_active_halfyear",
            self.crawl_result.node_info.usage.users.active_halfyear,
        )
        .param("posts", self.crawl_result.node_info.usage.posts)
        .param("comments", self.crawl_result.node_info.usage.comments)
        .param("time", self.params.index_time.naive_utc())
    }
}

pub(crate) async fn background_task(
    i: u32,
    send: WeakUnboundedSender<()>,
    rcv: Arc<Mutex<UnboundedReceiver<GraphStoreJob>>>,
) {
    loop {
        let maybe_job = {
            let mut lock = rcv.lock().await;
            lock.recv().await
        };
        if let Some(store_job) = maybe_job {
            let domain = &store_job.crawl_result.domain;
            debug!("Worker graphdb:{i} starting to index results for {domain}");
            // hold an upgraded sender to prevent shutdown before finishing
            let _send = send.upgrade().unwrap();
            let res = store_job.update_graph().await;
            if let Err(e) = res {
                trace!(
                    "Graph store job {domain} on worker graphdb:{i} errored with: {}",
                    e
                )
            }
        } else {
            return;
        }
    }
}
