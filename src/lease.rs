use futures::future::{AbortHandle, Abortable};
use http::StatusCode;
use k8s_openapi::api::coordination::v1::Lease as LeaseObject;
use kube::api::PatchParams;
use std::convert::TryFrom;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_retry::strategy::ExponentialBackoff;

type Api = kube::Api<LeaseObject>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("timeout waiting for acquire")]
    AcquireTimeout,

    #[error("trying to lock recursively")]
    RecursiveLockAttempt,

    #[error("Integer overflow in duration value")]
    IntOverflow(#[from] std::num::TryFromIntError),

    #[error("key {0} not found in Lease")]
    Format(String),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error(transparent)]
    Kube(#[from] kube::Error),
}

#[derive(Clone)]
struct LeaseLockClient {
    lease_name: String,
    api: Api,
    lease_duration_sec: i32,
    expo: ExponentialBackoff,
}

pub struct LeaseLock {
    client: LeaseLockClient,
    completion_tx: Sender<()>,
    completion_rx: Receiver<()>,
}

pub struct LeaseGuard {
    api: Api,
    lease_state: LeaseState,
    abort_handle: AbortHandle,
    completion_tx: Sender<()>,
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        log::debug!(
            "{}.drop({:?})",
            &self.lease_state.lease_name,
            &self.lease_state.holder
        );
        self.abort_handle.abort();
        tokio::spawn({
            let api = self.api.clone();
            let lease_state = self.lease_state.clone();
            let completion_tx = self.completion_tx.clone();
            async move {
                match release_lock(api, &lease_state).await {
                    Err(e) => log::error!(
                        "{}.release_lock({:?}) => {}",
                        &lease_state.lease_name,
                        &lease_state.holder,
                        e
                    ),
                    Ok(_) => log::debug!(
                        "release_lock({}, {:?}) => OK",
                        &lease_state.lease_name,
                        &lease_state.holder
                    ),
                }
                drop(completion_tx);
            }
        });
    }
}

async fn release_lock(api: Api, lease_state: &LeaseState) -> Result<LeaseState, Error> {
    let patch: LeaseObject = serde_json::from_value(serde_json::json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "name": &lease_state.lease_name,
            "resourceVersion": &lease_state.resource_version,
        },
        "spec": {
            "holderIdentity": serde_json::json!(null),
        }
    }))?;

    api.patch(
        &lease_state.lease_name,
        &PatchParams::apply("lease-rs").force(),
        &kube::api::Patch::Apply(&patch),
    )
    .await
    .map(LeaseState::try_from)?
}

impl LeaseLock {
    pub fn new(api: Api, lease_name: String) -> Self {
        let (completion_tx, completion_rx) = channel(1);
        Self {
            client: LeaseLockClient {
                api,
                lease_name,
                lease_duration_sec: 10,
                expo: ExponentialBackoff::from_millis(10).max_delay(Duration::from_secs(1)),
            },
            completion_tx: completion_tx,
            completion_rx: completion_rx,
        }
    }

    pub fn with_lease_duration_sec(mut self, sec: i32) -> Self {
        self.client.lease_duration_sec = sec;
        self
    }

    pub fn with_expo_backoff(mut self, expo: ExponentialBackoff) -> Self {
        self.client.expo = expo;
        self
    }

    pub async fn complete_all_operations(&mut self) {
        let (completion_tx, completion_rx) = channel(1);
        self.completion_tx = completion_tx;
        let _ = self.completion_rx.recv().await;
        self.completion_rx = completion_rx;
    }

    pub async fn acquire(
        &self,
        holder_id: &str,
        acquire_timeout: Option<Duration>,
    ) -> Result<LeaseGuard, Error> {
        self.client
            .acquire(holder_id, acquire_timeout, self.completion_tx.clone())
            .await
    }

    pub async fn try_acquire(&self, holder_id: &str) -> Result<Option<LeaseGuard>, Error> {
        match self.acquire(holder_id, Some(Duration::ZERO)).await {
            Ok(lg) => Ok(Some(lg)),
            Err(e) => match e {
                Error::AcquireTimeout => Ok(None),
                _ => Err(e),
            },
        }
    }
}

impl LeaseLockClient {
    pub async fn acquire(
        &self,
        holder_id: &str,
        acquire_timeout: Option<Duration>,
        completion_tx: Sender<()>,
    ) -> Result<LeaseGuard, Error> {
        log::debug!(
            "{}.acquire({}, {:?})",
            &self.lease_name,
            holder_id,
            acquire_timeout
        );

        let deadline = acquire_timeout.map(|to| Instant::now() + to);

        loop {
            let lease_state = self
                .try_overwrite(holder_id, self.wait_free(deadline).await?)
                .await?;
            if lease_state.owner() == Some(holder_id) {
                return Ok(LeaseGuard {
                    api: self.api.clone(),
                    lease_state,
                    abort_handle: self.clone().schedule_renewal(holder_id.to_string()),
                    completion_tx,
                });
            }
        }
    }

    #[must_use]
    fn schedule_renewal(self, holder_id: String) -> AbortHandle {
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        tokio::spawn(Abortable::new(
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(
                        (self.lease_duration_sec * 400) as u64,
                    ))
                    .await;
                    match self.get_state().await {
                        Ok(lease_state) => {
                            if lease_state.owner().as_ref() == Some(&holder_id.as_str()) {
                                if let Err(e) = self.renew_lease(lease_state).await {
                                    log::error!(
                                        "renew_lease({}, {}) => {}",
                                        self.lease_name,
                                        holder_id,
                                        e
                                    );
                                }
                            } else {
                                log::warn!(
                                    "lost ownership; new owner: {:?}; stop renewal",
                                    lease_state.owner()
                                );
                                return;
                            }
                        }
                        Err(e) => log::error!(
                            "schedule_renewal({}, {}) => {}",
                            self.lease_name,
                            holder_id,
                            e
                        ),
                    }
                }
            },
            abort_reg,
        ));

        abort_handle
    }

    async fn renew_lease(&self, lease_state: LeaseState) -> Result<LeaseState, Error> {
        let now: &str = &chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, false);
        let patch: LeaseObject = serde_json::from_value(serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": &lease_state.lease_name,
                "resourceVersion": &lease_state.resource_version,
            },
            "spec": {
                "renewTime": now,
                "holderIdentity": &lease_state.holder,
            }
        }))?;

        self.api
            .patch(
                &lease_state.lease_name,
                &PatchParams::apply("lease-rs").force(),
                &kube::api::Patch::Apply(&patch),
            )
            .await
            .map(LeaseState::try_from)?
    }

    async fn get_state(&self) -> Result<LeaseState, Error> {
        self.api
            .get(&self.lease_name)
            .await
            .map(LeaseState::try_from)?
    }

    async fn wait_free(&self, deadline: Option<Instant>) -> Result<LeaseState, Error> {
        let lease_state = self.get_state().await?;
        if lease_state.owner().is_none() {
            return Ok(lease_state);
        }

        for backoff in self.expo.clone() {
            if let Some(d) = deadline {
                if Instant::now() + backoff >= d {
                    return Err(Error::AcquireTimeout);
                }
            }

            tokio::time::sleep(backoff).await;

            let lease_state = self.get_state().await?;
            if lease_state.owner().is_none() {
                return Ok(lease_state);
            }
        }

        panic!("impossible");
    }

    async fn try_overwrite(
        &self,
        holder_id: &str,
        lease_state: LeaseState,
    ) -> Result<LeaseState, Error> {
        let now: &str = &chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, false);
        let patch: LeaseObject = serde_json::from_value(serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": &lease_state.lease_name,
                "resourceVersion": &lease_state.resource_version,
            },
            "spec": {
                "acquireTime": now,
                "renewTime": now,
                "holderIdentity": holder_id,
                "leaseDurationSeconds": self.lease_duration_sec,
            }
        }))?;

        let patch_res = self
            .api
            .patch(
                &self.lease_name,
                &PatchParams::apply("lease-rs").force(),
                &kube::api::Patch::Apply(&patch),
            )
            .await;
        match patch_res {
            Ok(lease_obj) => Ok(LeaseState::try_from(lease_obj)?),
            Err(e) => {
                if let kube::Error::Api(api_err) = e {
                    if api_err.code == StatusCode::CONFLICT {
                        return Ok(lease_state);
                    }
                    return Err(kube::Error::Api(api_err).into());
                }
                Err(e.into())
            }
        }
    }
}

type UtcInstant = chrono::DateTime<chrono::offset::Utc>;

#[derive(Clone)]
pub struct LeaseState {
    lease_name: String,
    holder: Option<String>,
    renew_time: UtcInstant,
    lease_duration: chrono::Duration,
    resource_version: String,
}

impl TryFrom<LeaseObject> for LeaseState {
    type Error = crate::lease::Error;
    fn try_from(lo: LeaseObject) -> Result<Self, Error> {
        Ok(LeaseState {
            lease_name: lo
                .metadata
                .name
                .ok_or_else(|| Error::Format("lease name".into()))?,

            holder: lo.spec.as_ref().and_then(|x| x.holder_identity.clone()),

            renew_time: lo
                .spec
                .as_ref()
                .and_then(|x| x.renew_time.as_ref())
                .map(|x| x.0)
                .unwrap_or(chrono::MIN_DATETIME),

            lease_duration: chrono::Duration::seconds(
                (lo.spec.and_then(|x| x.lease_duration_seconds).unwrap_or(0) as u64)
                    .try_into()
                    .map_err(Error::from)?,
            ),

            resource_version: lo
                .metadata
                .resource_version
                .ok_or_else(|| Error::Format("resourceVersion".into()))?,
        })
    }
}

impl LeaseState {
    fn expired(&self) -> bool {
        self.renew_time + self.lease_duration <= chrono::Utc::now()
    }

    fn owner(&self) -> Option<&str> {
        if self.expired() {
            None
        } else {
            self.holder.as_deref()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::lease::*;
    use futures::stream::StreamExt;
    use kube::api::{DeleteParams, PostParams};
    use rand::Rng;
    use std::sync::Once;
    use taken::take;
    use test_context::{test_context, AsyncTestContext};

    static LOG_INIT: Once = Once::new();

    struct TestContext {
        pub lease_name: String,
        pub api: Api,
        pub lease_lock: LeaseLock,
    }

    #[async_trait::async_trait]
    impl AsyncTestContext for TestContext {
        async fn setup() -> Self {
            LOG_INIT.call_once(|| env_logger::init());

            let lease_name = format!("test-lease-{}", rand::thread_rng().gen::<u32>());
            log::debug!("{}.setup()", &lease_name);

            let api = kube::Api::default_namespaced(kube::Client::try_default().await.unwrap());
            let lease: LeaseObject = serde_json::from_value(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": { "name": &lease_name },
                "spec": {},
            }))
            .unwrap();
            let _ = api.create(&PostParams::default(), &lease).await;
            let lease_lock = LeaseLock::new(api.clone(), lease_name.clone());
            Self {
                lease_name,
                api,
                lease_lock,
            }
        }

        async fn teardown(mut self) {
            log::debug!("{}.teardown()", &self.lease_name);
            self.lease_lock.complete_all_operations().await;
            self.api
                .delete(&self.lease_name, &DeleteParams::default())
                .await
                .unwrap();
        }
    }

    #[test_context(TestContext)]
    #[tokio::test]
    async fn raii(ctx: &mut TestContext) {
        {
            let _guard = ctx
                .lease_lock
                .try_acquire("initial")
                .await
                .unwrap()
                .unwrap();
            assert!(ctx
                .lease_lock
                .try_acquire("within scope")
                .await
                .unwrap()
                .is_none());
        }
        ctx.lease_lock
            .acquire("outside scope", Some(Duration::from_secs(1)))
            .await
            .unwrap();
    }

    #[test_context(TestContext)]
    #[tokio::test]
    async fn concurrent_locks(ctx: &mut TestContext) {
        use std::sync::Arc;
        use tokio::sync::Mutex;
        let glob = Arc::new(Mutex::new(0));
        take!(&glob, &ctx);
        (1..10)
            .map(|i| async move {
                let _guard = ctx
                    .lease_lock
                    .acquire(&format!("{}", i), Some(Duration::from_secs(5)))
                    .await
                    .unwrap();
                *glob.lock().await = i;
                tokio::time::sleep(Duration::from_millis(10)).await;
                assert_eq!(*glob.lock().await, i);
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;
    }

    #[test_context(TestContext)]
    #[tokio::test]
    async fn complete(ctx: &mut TestContext) {
        {
            let _ = ctx.lease_lock.try_acquire("1").await.unwrap().unwrap();
        }
        ctx.lease_lock.complete_all_operations().await;
        {
            let _ = ctx.lease_lock.try_acquire("1").await.unwrap().unwrap();
        }
    }
}
