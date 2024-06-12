use async_trait::async_trait;
use auxon_sdk::api::{AttrKey, AttrType, AttrVal};
use auxon_sdk::mutator_protocol::actuator::MutatorActuator;
use auxon_sdk::mutator_protocol::descriptor::owned::{
    MutatorLayer, MutatorOperation, OrganizationCustomMetadata, OwnedMutatorDescriptor,
    OwnedMutatorParamDescriptor,
};
use auxon_sdk::mutator_protocol::mutator::{ActuatorDescriptor, CombinedMutator};
use auxon_sdk::plugin_utils::BearingConfigFilePath;
use std::collections::{BTreeMap, HashMap};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Default, clap::Parser)]
pub struct CliOptions {
    /// Use configuration from file
    #[clap(
        long = "config",
        name = "config file",
        env = "MODALITY_REFLECTOR_PLUGIN_CONFIG_FILE"
    )]
    pub config_file: Option<PathBuf>,
}
impl BearingConfigFilePath for CliOptions {
    fn config_file_path(&self) -> Option<&Path> {
        self.config_file.as_deref()
    }
}

fn main() {
    #[allow(deprecated)]
    std::process::exit(auxon_sdk::plugin_utils::server_main::<CliOptions, _, _>(
        |config, _auth_token, _opts, shutdown_signal| async move {
            let mutators = std::iter::once(("lonesome".to_string(), mutator())).collect();
            let port = config
                .mutation
                .as_ref()
                .and_then(|m| m.mutator_http_api_port)
                .unwrap_or(8080);
            auxon_sdk::mutator_server::server::serve_mutators(
                mutators,
                None,
                (Ipv4Addr::UNSPECIFIED, port),
                async {
                    shutdown_signal.await;
                },
            )
            .await;
            Ok(())
        },
    ));
}

fn mutator() -> Box<dyn ActuatorDescriptor + Send> {
    struct SingleValueSetActuator {
        original: i64,
        value: i64,
    }
    impl SingleValueSetActuator {
        fn new(v: i64) -> Self {
            SingleValueSetActuator {
                original: v,
                value: v,
            }
        }
    }

    #[async_trait]
    impl MutatorActuator for SingleValueSetActuator {
        async fn inject(
            &mut self,
            mutation_id: Uuid,
            mut params: BTreeMap<AttrKey, AttrVal>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let mutation_id_as_integer = i128::from_le_bytes(*mutation_id.as_bytes());
            if let Some(AttrVal::Integer(i)) =
                params.remove(&AttrKey::from(MutatorOperation::SetToValue.name()))
            {
                if i < -50 {
                    tracing::warn!("Clipping mutation to the minimum, -50");
                    self.value = -50;
                } else if i > 9000 {
                    tracing::warn!("Clipping mutation to the maximum, 9000");
                    self.value = 9000;
                } else {
                    tracing::info!("Applying mutation");
                    self.value = i;
                }
                tracing::info!(
                    mutation.id = mutation_id_as_integer,
                    mutation.success = true,
                    name = "modality.mutation.injected"
                );
            } else {
                tracing::error!(
                    mutation.id = mutation_id_as_integer,
                    mutation.success = false,
                    name = "modality.mutation.injected",
                    "Expected an integer-type parameter with the name set_to_value",
                );
            }
            Ok(())
        }

        async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            tracing::info!("Reset to original value");
            self.value = self.original;
            Ok(())
        }
    }
    Box::new(CombinedMutator::new(
        SingleValueSetActuator::new(99),
        OwnedMutatorDescriptor {
            name: Some("single_value_mutator".to_owned()),
            description: Some("Manipulates the value of a single integer".to_owned()),
            layer: Some(MutatorLayer::Implementational),
            group: None,
            operation: Some(MutatorOperation::SetToValue),
            statefulness: None,
            organization_custom_metadata: OrganizationCustomMetadata::new(
                "test_org_name".to_string(),
                HashMap::from([("forty_two".to_string(), 42.into())]),
            ),
            params: vec![OwnedMutatorParamDescriptor {
                value_type: AttrType::Integer,
                name: MutatorOperation::SetToValue.name().to_string(),
                description: Some(
                    "the critical parameter - what to set the targeted value in memory to"
                        .to_owned(),
                ),
                value_min: Some(AttrVal::Integer(-50)),
                value_max: Some(9000.into()),
                default_value: None,
                least_effect_value: None,
                value_distribution_kind: None,
                value_distribution_scaling: None,
                value_distribution_option_set: None,
                organization_custom_metadata: None,
            }],
        },
    ))
}
