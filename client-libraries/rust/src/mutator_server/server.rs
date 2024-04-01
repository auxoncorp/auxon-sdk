use crate::mutator_protocol::mutator::ActuatorDescriptor;
use crate::mutator_server::MUTATOR_API_KEY_HEADER;
use async_trait::async_trait;
use axum::{extract::FromRequestParts, http::StatusCode, Router};
use std::collections::BTreeMap;
use std::future::Future;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use utoipa::{
    openapi::security::{ApiKey, ApiKeyValue, SecurityScheme},
    Modify, OpenApi,
};
use utoipa_swagger_ui::SwaggerUi;

pub async fn serve_mutators(
    mutators: BTreeMap<String, Box<dyn ActuatorDescriptor + Send>>,
    required_api_key_value: Option<String>,
    addr: impl Into<SocketAddr>,
    shutdown_signal: impl Future<Output = ()> + Send + 'static,
) {
    let listener = TcpListener::bind(addr.into()).unwrap();
    serve_mutators_on_listener(mutators, required_api_key_value, listener, shutdown_signal).await
}

pub async fn serve_mutators_on_listener(
    mutators: BTreeMap<String, Box<dyn ActuatorDescriptor + Send>>,
    required_api_key_value: Option<String>,
    listener: TcpListener,
    shutdown_signal: impl Future<Output = ()> + Send + 'static,
) {
    let store = mutator::Store {
        required_api_key_value,
        mutators: Arc::new(tokio::sync::Mutex::new(mutators)),
    };

    let routes = Router::new()
        .merge(mutator::routes().with_state(store))
        .merge(swagger_routes());

    let server = axum::Server::from_tcp(listener)
        .expect("useable socket")
        .serve(routes.into_make_service());

    let addr = server.local_addr();
    let server = server.with_graceful_shutdown(shutdown_signal);

    tracing::debug!(%addr, "Serving mutator HTTP API");

    if let Err(e) = server.await {
        tracing::error!(
            err = &e as &dyn std::error::Error,
            "Error running mutator http server"
        );
    }
}

fn swagger_routes() -> Router {
    use crate::mutator_server::{Mutation, Mutator};
    #[derive(OpenApi)]
    #[openapi(
        paths(mutator::list_mutators, mutator::create_mutation, mutator::delete_mutations),
        components(schemas(Mutator, Mutation)),
        modifiers(&SecurityAddon),
        tags(
            (name = "mutator", description = "Mutator API")
        )
    )]
    struct ApiDoc;

    struct SecurityAddon;
    impl Modify for SecurityAddon {
        fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
            if let Some(components) = openapi.components.as_mut() {
                components.add_security_scheme(
                    "api_key",
                    SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new(
                        MUTATOR_API_KEY_HEADER,
                    ))),
                )
            }
        }
    }

    Router::new().merge(SwaggerUi::new("/swagger-ui").url("/api-doc.json", ApiDoc::openapi()))
}

pub(crate) mod mutator {
    use crate::mutator_server::{GetAllMutatorsResponse, Mutation, Mutator};
    use std::collections::BTreeMap;
    use std::{convert::Infallible, sync::Arc};

    use crate::mutator_protocol::mutator::ActuatorDescriptor;
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use axum::{Json, Router};

    use super::ValidApiKeyHeader;

    /// See: <https://github.com/seanmonstar/warp/issues/242>.
    mod url_path_part {
        use percent_encoding::percent_decode_str;
        use std::str::FromStr;
        use std::string::ToString;

        /// A type intended exclusively for use in warp path parameter parsing.
        /// Do *not* use the `FromStr::from_str` implementation in any other context,
        /// because it assumes that the input is already percent-encoded.
        #[derive(Clone, Debug)]
        #[repr(transparent)]
        pub struct UrlPathPart(String);

        /// Warning! This FromStr implementation is not for general purpose use.
        /// It assumes the input `str` contains percent-encoded content.
        impl FromStr for UrlPathPart {
            type Err = std::str::Utf8Error;
            #[inline]
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(UrlPathPart(
                    percent_decode_str(s).decode_utf8()?.to_string(),
                ))
            }
        }
        impl From<UrlPathPart> for String {
            fn from(v: UrlPathPart) -> Self {
                v.0
            }
        }
        impl AsRef<str> for UrlPathPart {
            fn as_ref(&self) -> &str {
                self.0.as_str()
            }
        }
    }

    #[derive(Clone)]
    pub struct Store {
        pub required_api_key_value: Option<String>,
        pub mutators: Arc<tokio::sync::Mutex<BTreeMap<String, Box<dyn ActuatorDescriptor + Send>>>>,
    }

    pub fn routes() -> Router<Store> {
        Router::new().route("/mutator", get(list_mutators)).route(
            "/mutator/:mutator_correlation_id/mutation",
            post(create_mutation).delete(delete_mutations),
        )
    }

    /// List all mutators.
    #[utoipa::path(
        get,
        path = "/mutator",
        responses(
            (status = 200, description = "List mutators successfully", body = [Mutator]),
            (status = 400, description = "Missing mutator_apikey request header"),
            (status = 401, description = "Unauthorized to view mutators")
        ),
        security(
            ("api_key" = [])
        )
    )]
    pub async fn list_mutators(
        State(store): State<Store>,
        _h: ValidApiKeyHeader,
    ) -> Result<Json<GetAllMutatorsResponse>, Infallible> {
        let mutators_map = store.mutators.lock().await;

        let mut mutator_components: Vec<Mutator> = vec![];
        for (corr_id, actuator_descriptor) in mutators_map.iter() {
            let attr_iter = actuator_descriptor.get_description_attributes();
            mutator_components.push(Mutator {
                mutator_correlation_id: corr_id.clone(),
                attributes: attr_iter.collect(),
            })
        }

        Ok(Json(mutator_components))
    }

    /// Create new mutation for a mutator.
    #[utoipa::path(
        post,
        path = "/mutator/{mutator_correlation_id}/mutation",
        request_body = Mutation,
        responses(
            (status = 201, description = "Mutation created successfully"),
            (status = 400, description = "Missing mutator_apikey request header"),
            (status = 401, description = "Unauthorized to create mutations"),
            (status = 404, description = "Mutator not found"),
            (status = 500, description = "Internal mutator error")
        ),
        params(
            ("mutator_correlation_id" = String, Path, description = "Mutator's server-local correlation id")
        ),
        security(
            ("api_key" = [])
        )
    )]
    pub async fn create_mutation(
        State(store): State<Store>,
        _h: ValidApiKeyHeader,
        Path(mutator_correlation_id): Path<String>,
        Json(mutation): Json<Mutation>,
    ) -> Result<StatusCode, StatusCode> {
        tracing::debug!(%mutator_correlation_id);
        let mut mutators = store.mutators.lock().await;

        let actuator_descriptor = mutators
            .get_mut(&mutator_correlation_id)
            .ok_or(StatusCode::NOT_FOUND)?;

        match actuator_descriptor
            .inject(mutation.mutation, mutation.params)
            .await
        {
            Ok(()) => Ok(StatusCode::CREATED),
            Err(err) => {
                tracing::error!(
                    err = err.as_ref() as &dyn std::error::Error,
                    "Failed to inject mutation"
                );
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }

    /// Delete / reset all mutations for a mutator
    #[utoipa::path(
        delete,
        path = "/mutator/{mutator_correlation_id}/mutation",
        responses(
            (status = 200, description = "Mutations reset / deleted successful"),
            (status = 400, description = "Missing mutator_apikey request header"),
            (status = 401, description = "Unauthorized to delete mutations"),
            (status = 404, description = "Mutator not found to delete mutations"),
        ),
        params(
            ("mutator_correlation_id" = String, Path, description = "Mutator's server-local correlation id")
        ),
        security(
            ("api_key" = [])
        )
    )]
    pub async fn delete_mutations(
        State(store): State<Store>,
        Path(mutator_correlation_id): Path<String>,
        _h: ValidApiKeyHeader,
    ) -> Result<StatusCode, StatusCode> {
        tracing::debug!(%mutator_correlation_id);
        let mut mutators = store.mutators.lock().await;

        let actuator_descriptor = mutators
            .get_mut(&mutator_correlation_id)
            .ok_or(StatusCode::NOT_FOUND)?;
        match actuator_descriptor.reset().await {
            Ok(()) => Ok(StatusCode::OK),
            Err(err) => {
                tracing::error!(
                    err = err.as_ref() as &dyn std::error::Error,
                    "Failed to delete mutation"
                );
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }
}

/// An extractor to read the 'mutator_apikey' header and check that it's the right one.
pub struct ValidApiKeyHeader(());

#[async_trait]
impl FromRequestParts<mutator::Store> for ValidApiKeyHeader {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        store: &mutator::Store,
    ) -> Result<Self, Self::Rejection> {
        let api_key_bytes = parts.headers.get(MUTATOR_API_KEY_HEADER).ok_or((
            StatusCode::BAD_REQUEST,
            "Missing required header 'mutator_apikey'",
        ))?;

        let api_key = std::str::from_utf8(api_key_bytes.as_bytes())
            .map_err(|_| (StatusCode::BAD_REQUEST, "Malformed 'mutator_apikey' header"))?;

        if let Some(required_api_key_value) = store.required_api_key_value.as_ref() {
            if api_key != required_api_key_value {
                return Err((StatusCode::UNAUTHORIZED, "Invalid 'mutator_apikey'"));
            }
        }

        Ok(ValidApiKeyHeader(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{AttrKey, AttrType, AttrVal};
    use crate::mutator_protocol::actuator::MutatorActuator;
    use crate::mutator_protocol::descriptor::owned::{
        MutatorOperation, OrganizationCustomMetadata, OwnedMutatorDescriptor,
        OwnedMutatorParamDescriptor,
    };
    use crate::mutator_protocol::descriptor::MutatorDescriptor;
    use crate::mutator_server::server::serve_mutators_on_listener;
    use crate::mutator_server::{Mutation, Mutator};
    use async_trait::async_trait;
    use std::net::TcpListener;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;
    use tokio::sync::oneshot::Sender;
    use uuid::Uuid;

    #[tokio::test]
    async fn it_works() {
        let listener = TcpListener::bind("localhost:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx): (Sender<()>, _) = tokio::sync::oneshot::channel();
        let mut mutators_map: BTreeMap<_, Box<(dyn ActuatorDescriptor + Send + 'static)>> =
            BTreeMap::new();
        let original_value = 31;
        let atomic_inner_state = Arc::new(AtomicI64::new(original_value));
        mutators_map.insert(
            "abc".to_string(),
            Box::new(AtomicMutator::new(atomic_inner_state.clone())),
        );
        let mutators = mutators_map;
        let server_fut = serve_mutators_on_listener(mutators, None, listener, async {
            shutdown_rx.await.ok();
        });
        let join_handle = tokio::spawn(server_fut);
        tokio::time::sleep(Duration::from_secs(1)).await;
        // N.B. consider looping until we get a response
        let mutator_url = reqwest::Url::from_str(&format!("http://{}/mutator", addr)).unwrap();
        let client = reqwest::Client::builder().build().unwrap();
        let mutators_resp = client
            .get(mutator_url)
            .header(MUTATOR_API_KEY_HEADER, "whatever")
            .send()
            .await
            .unwrap();
        assert_eq!(reqwest::StatusCode::OK, mutators_resp.status());
        let mutators: Vec<Mutator> = mutators_resp.json().await.unwrap();
        assert_eq!(1, mutators.len());
        assert_eq!(
            &AttrVal::String("foo".into()),
            mutators[0].attributes.get(&"mutator.name".into()).unwrap()
        );

        // Do a mutation
        let mutation_url =
            reqwest::Url::from_str(&format!("http://{}/mutator/abc/mutation", addr)).unwrap();
        let mut mutation_params = BTreeMap::new();
        let set_to_value = 42;
        mutation_params.insert(
            AttrKey::from(MutatorOperation::SetToValue.name()),
            AttrVal::Integer(set_to_value),
        );
        let mutation = Mutation {
            mutation: Default::default(),
            params: mutation_params,
        };
        let mutation_resp = client
            .post(mutation_url.clone())
            .json(&mutation)
            .header(MUTATOR_API_KEY_HEADER, "whatever")
            .send()
            .await
            .unwrap();
        assert_eq!(reqwest::StatusCode::CREATED, mutation_resp.status());
        assert_eq!(set_to_value, atomic_inner_state.load(Ordering::SeqCst));

        let reset_resp = client
            .delete(mutation_url)
            .header(MUTATOR_API_KEY_HEADER, "whatever")
            .send()
            .await
            .unwrap();
        assert_eq!(reqwest::StatusCode::OK, reset_resp.status());
        assert_eq!(original_value, atomic_inner_state.load(Ordering::SeqCst));

        let _ = shutdown_tx.send(());
        let join_res = join_handle.await;
        assert!(join_res.is_ok());
    }

    pub struct AtomicMutator {
        initial: i64,
        inner: Arc<AtomicI64>,
    }
    impl AtomicMutator {
        pub fn new(inner: Arc<AtomicI64>) -> Self {
            Self {
                initial: inner.load(Ordering::SeqCst),
                inner,
            }
        }

        fn description() -> OwnedMutatorDescriptor {
            OwnedMutatorDescriptor {
                name: Some("foo".into()),
                description: None,
                layer: None,
                group: None,
                operation: Some(MutatorOperation::SetToValue),
                statefulness: None,
                organization_custom_metadata: Some(
                    OrganizationCustomMetadata::new(
                        "some_jerks".to_owned(),
                        std::iter::once(("fleet".to_owned(), AttrVal::Integer(99))).collect(),
                    )
                    .unwrap(),
                ),
                params: vec![OwnedMutatorParamDescriptor::new(
                    AttrType::Integer,
                    MutatorOperation::SetToValue.name().to_owned(),
                )
                .unwrap()],
            }
        }
    }

    impl ActuatorDescriptor for AtomicMutator {}

    impl MutatorDescriptor for AtomicMutator {
        fn get_description_attributes(&self) -> Box<dyn Iterator<Item = (AttrKey, AttrVal)> + '_> {
            // Wasteful, but hey, it's a test util
            let desc = AtomicMutator::description();
            let attrs: Vec<_> = desc.get_description_attributes().collect();
            Box::new(attrs.into_iter())
        }
    }

    #[async_trait]
    impl MutatorActuator for AtomicMutator {
        async fn inject(
            &mut self,
            _mutation_id: Uuid,
            mut params: BTreeMap<AttrKey, AttrVal>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let v = params
                .remove(&AttrKey::from(MutatorOperation::SetToValue.name()))
                .expect("Expected the set_to_value parameter");
            if let AttrVal::Integer(i) = v {
                self.inner.store(i, Ordering::SeqCst);
            } else {
                panic!("Unexpected param of value {:?} for set_to_value", v);
            }
            Ok(())
        }

        async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.inner.store(self.initial, Ordering::SeqCst);
            Ok(())
        }
    }
}
