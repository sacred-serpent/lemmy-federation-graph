#[macro_use]
extern crate derive_new;

use crate::crawl::{CrawlJob, CrawlParams, CrawlResult};
use crate::node_info::{NodeInfo, NodeInfoUsage, NodeInfoUsers};
use anyhow::Error;
use graph::{GraphStoreJob, GraphStoreParams};
use lemmy_api_common::site::GetSiteResponse;
use log::{debug, trace};
use neo4rs::Graph;
use once_cell::sync::Lazy;
use reqwest::{Client, ClientBuilder};
use semver::Version;
use serde::Serialize;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, WeakUnboundedSender};
use tokio::sync::{mpsc, Mutex};

pub mod crawl;
mod graph;
mod node_info;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

static CLIENT: Lazy<Client> = Lazy::new(|| {
    ClientBuilder::new()
        .timeout(REQUEST_TIMEOUT)
        .user_agent("lemmy-stats-crawler")
        .pool_idle_timeout(Some(Duration::from_millis(100)))
        .pool_max_idle_per_host(1)
        .build()
        .expect("build reqwest client")
});

#[derive(Serialize, Debug)]
pub struct CrawlResult2 {
    pub domain: String,
    pub site_info: GetSiteResponse,
    pub federated_counts: Option<NodeInfoUsage>,
}

pub async fn start_crawl(
    start_instances: Vec<String>,
    exclude_domains: Vec<String>,
    crawl_jobs_count: u32,
    store_jobs_count: u32,
    neo4j_uri: Option<String>,
    neo4j_db: String,
    neo4j_user: String,
    neo4j_pass: String,
    max_distance: u8,
) -> Result<Vec<CrawlResult2>, Error> {
    let (crawl_jobs_sender, crawl_jobs_receiver) = mpsc::unbounded_channel::<CrawlJob>();
    let (crawl_results_sender, mut crawl_results_receiver) = mpsc::unbounded_channel();
    let crawl_params = Arc::new(CrawlParams::new(
        min_lemmy_version().await?,
        exclude_domains.into_iter().collect(),
        max_distance,
        Mutex::new(HashSet::new()),
        crawl_results_sender,
    ));

    let neo4j_vars = match neo4j_uri {
        Some(neo4j_uri) => {
            let (store_job_sender, store_job_receiver) = mpsc::unbounded_channel::<GraphStoreJob>();
            let (store_results_sender, store_results_receiver) = mpsc::unbounded_channel();
            let store_params = Arc::new(GraphStoreParams::new(
                Mutex::new(HashSet::new()),
                Arc::new(
                    Graph::connect(
                        neo4rs::ConfigBuilder::default()
                            .uri(neo4j_uri)
                            .user(neo4j_user)
                            .password(neo4j_pass)
                            .db(neo4j_db)
                            .build()?,
                    )
                    .await?,
                ),
                chrono::offset::Utc::now(),
            ));
            let store_job_receiver = Arc::new(Mutex::new(store_job_receiver));

            Some((
                store_params,
                store_job_sender,
                store_job_receiver,
                store_results_sender,
                store_results_receiver,
            ))
        }
        None => None,
    };

    let job_rcv = Arc::new(Mutex::new(crawl_jobs_receiver));
    let job_send = crawl_jobs_sender.downgrade();
    for i in 0..crawl_jobs_count {
        let rcv = job_rcv.clone();
        let send = job_send.clone();
        tokio::spawn(background_task(i, send, rcv));
    }

    for domain in start_instances.into_iter() {
        let job = CrawlJob::new(domain, 0, crawl_params.clone());
        crawl_jobs_sender.send(job).unwrap();
    }

    if let Some((_, _, ref store_job_receiver, ref store_results_sender, _)) = neo4j_vars {
        let store_results_send = store_results_sender.downgrade();
        for i in 0..store_jobs_count {
            let rcv = store_job_receiver.clone();
            let send = store_results_send.clone();
            tokio::spawn(graph::background_task(i, send, rcv));
        }
    }
    // give time to start background tasks
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(crawl_params);

    let mut results = vec![];
    while let Some(res) = crawl_results_receiver.recv().await {
        let res = Arc::new(res);
        if let Some((ref store_params, ref store_job_sender, _, _, _)) = neo4j_vars {
            // dispatch a store job from a crawl result.
            // TODO: maybe this should be done from within crawl tasks and not
            //       centrally here?
            let store_job = GraphStoreJob::new(store_params.clone(), res.clone());
            store_job_sender
                .send(store_job)
                .expect("Failed to send store job!");
        }
        results.push(res);
    }

    if let Some((_, _, _, store_results_sender, mut store_results_receiver)) = neo4j_vars {
        // give time to start background tasks
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(store_results_sender);
        // wait for all store jobs to finish
        while let Some(_) = store_results_receiver.recv().await {
            // not sleeping here may cause the channel to close before
            // a store worker can upgrade it's weak sender,
            // ending the process prematurely
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    let mut crawl_results = calculate_federated_site_aggregates(results)?;

    // Sort by active monthly users descending
    crawl_results.sort_unstable_by_key(|i| i.site_info.site_view.counts.users_active_month);
    crawl_results.reverse();
    Ok(crawl_results)
}

async fn background_task(
    i: u32,
    sender: WeakUnboundedSender<CrawlJob>,
    rcv: Arc<Mutex<UnboundedReceiver<CrawlJob>>>,
) {
    loop {
        let maybe_job = {
            let mut lock = rcv.lock().await;
            lock.recv().await
        };
        if let Some(job) = maybe_job {
            let domain = job.domain.clone();
            debug!(
                "Worker {i} starting job {domain} at distance {}",
                job.current_distance
            );
            let sender = sender.upgrade().unwrap();
            let res = job.crawl(sender).await;
            if let Err(e) = res {
                trace!("Job {domain} errored with: {}", e)
            }
        } else {
            return;
        }
    }
}

/// calculate minimum allowed lemmy version based on current version. in case of current version
/// 0.16.3, the minimum from this function is 0.15.3. this is to avoid rejecting all instances on
/// the previous version when a major lemmy release is published.
async fn min_lemmy_version() -> Result<Version, Error> {
    // let lemmy_version_url = "https://raw.githubusercontent.com/LemmyNet/lemmy-ansible/main/VERSION";
    // let req = CLIENT
    //     .get(lemmy_version_url)
    //     .timeout(REQUEST_TIMEOUT)
    //     .send()
    //     .await?;
    //req.text().await?.trim()

    // timeouts are frequent so just a static version number for now
    let version = Version::parse("0.1.0")?;

    // version.minor -= 1;
    Ok(version)
}

// TODO: not quite sure what this is doing
fn calculate_federated_site_aggregates(
    crawl_results: Vec<Arc<CrawlResult>>,
) -> Result<Vec<CrawlResult2>, Error> {
    let node_info: Vec<(String, NodeInfo)> = crawl_results
        .iter()
        .map(|c| (c.domain.clone(), c.node_info.clone()))
        .collect();
    let lemmy_instances: Vec<(String, GetSiteResponse)> = crawl_results
        .into_iter()
        .map(|c| {
            let domain = c.domain.clone();
            (domain, c.site_info.clone())
        })
        .collect();
    let mut ret = vec![];
    for instance in &lemmy_instances {
        let federated_counts = if let Some(federated_instances) = &instance.1.federated_instances {
            node_info
                .iter()
                .filter(|i| federated_instances.linked.contains(&i.0) || i.0 == instance.0)
                .map(|i| i.1.usage.clone())
                .reduce(|a, b| NodeInfoUsage {
                    users: NodeInfoUsers {
                        total: a.users.total + b.users.total,
                        active_halfyear: a.users.active_halfyear + b.users.active_halfyear,
                        active_month: a.users.active_month + b.users.active_month,
                    },
                    posts: a.posts + b.posts,
                    comments: a.comments + b.comments,
                })
        } else {
            None
        };
        ret.push(CrawlResult2 {
            domain: instance.0.clone(),
            site_info: instance.1.clone(),
            federated_counts,
        });
    }
    Ok(ret)
}
