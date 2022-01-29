use futures::future::{AbortHandle, Abortable};
use http::StatusCode;
use k8s_openapi::api::coordination::v1::Lease as LeaseObject;
use kube::api::PatchParams;
use std::convert::TryFrom;
use std::time::{Duration, Instant};
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
pub struct LeaseLock {
    lease_name: String,
    api: Api,
    lease_duration_sec: i32,
    expo: ExponentialBackoff,
}

pub struct LeaseGuard {
    api: Api,
    lease_state: LeaseState,
    abort_handle: AbortHandle,
}

impl LeaseGuard {
    pub fn new(api: Api, lease_state: LeaseState, abort_handle: AbortHandle) -> Self {
        Self {
            api,
            lease_state,
            abort_handle,
        }
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.abort_handle.abort();
        tokio::spawn({
            let api = self.api.clone();
            let lease_state = self.lease_state.clone();
            async move {
                match release_lock(api, &lease_state).await {
                    Err(e) => log::error!(
                        "release_lock({}, {:?}) => {}",
                        &lease_state.lease_name,
                        &lease_state.holder,
                        e
                    ),
                    Ok(_) => log::debug!(
                        "release_lock({}, {:?})",
                        &lease_state.lease_name,
                        &lease_state.holder
                    ),
                }
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
        Self {
            api,
            lease_name,
            lease_duration_sec: 10,
            expo: ExponentialBackoff::from_millis(10).max_delay(Duration::from_secs(1)),
        }
    }

    pub fn with_lease_duration_sec(mut self, sec: i32) -> Self {
        self.lease_duration_sec = sec;
        self
    }

    pub fn with_expo_backoff(mut self, expo: ExponentialBackoff) -> Self {
        self.expo = expo;
        self
    }

    pub async fn acquire(
        &self,
        holder_id: &str,
        acquire_timeout: Option<Duration>,
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
                return Ok(LeaseGuard::new(
                    self.api.clone(),
                    lease_state,
                    self.clone().schedule_renewal(holder_id.to_string()),
                ));
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

    pub async fn try_acquire(&self, holder_id: &str) -> Result<Option<LeaseGuard>, Error> {
        match self.acquire(holder_id, Some(Duration::ZERO)).await {
            Ok(lg) => Ok(Some(lg)),
            Err(e) => match e {
                Error::AcquireTimeout => Ok(None),
                _ => Err(e),
            },
        }
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
    use kube::api::{DeleteParams, PostParams};
    use rand::Rng;
    use std::sync::Once;
    use test_context::{test_context, AsyncTestContext};

    static LOG_INIT: Once = Once::new();

    struct TestContext {
        pub lease_name: String,
        pub api: Api,
    }

    impl TestContext {
        async fn create_lease(&self) {
            LOG_INIT.call_once(|| env_logger::init());
            log::debug!("create_lease({})", &self.lease_name);
            let lease: LeaseObject = serde_json::from_value(serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": { "name": &self.lease_name },
                "spec": {},
            }))
            .unwrap();
            let _ = self.api.create(&PostParams::default(), &lease).await;
        }
    }

    #[async_trait::async_trait]
    impl AsyncTestContext for TestContext {
        async fn setup() -> Self {
            let ctx = TestContext {
                lease_name: format!("test-lease-{}", rand::thread_rng().gen::<u32>()),
                api: kube::Api::default_namespaced(kube::Client::try_default().await.unwrap()),
            };
            ctx.create_lease().await;
            ctx
        }

        async fn teardown(self) {
            log::debug!("delete_lease({})", &self.lease_name);
            self.api
                .delete(&self.lease_name, &DeleteParams::default())
                .await
                .unwrap();
        }
    }

    #[test_context(TestContext)]
    #[tokio::test]
    async fn raii(ctx: &mut TestContext) {
        let ll = LeaseLock::new(ctx.api.clone(), ctx.lease_name.clone());
        {
            let _guard = ll.try_acquire("initial").await.unwrap().unwrap();
            assert!(ll.try_acquire("within scope").await.unwrap().is_none());
        }
        ll.acquire("outside scope", Some(Duration::from_secs(1)))
            .await
            .unwrap();
    }
}
