use async_trait::async_trait;
use auxon_sdk::api::{AttrKey, AttrType, AttrVal};
use auxon_sdk::mutator_protocol::actuator::MutatorActuator;
use auxon_sdk::mutator_protocol::descriptor::owned::{
    MutatorLayer, MutatorOperation, OwnedMutatorDescriptor, OwnedMutatorParamDescriptor,
};
use auxon_sdk::mutator_protocol::mutator::{ActuatorDescriptor, CombinedMutator};
use auxon_sdk::plugin_utils::BearingConfigFilePath;
use std::collections::BTreeMap;
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
            let mutators = vec![
                ("setter".to_string(), mutator_a()),
                ("reorderer".to_string(), mutator_b()),
            ]
            .into_iter()
            .collect();
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

fn mutator_a() -> Box<dyn ActuatorDescriptor + Send> {
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
                    tracing::info!("Applying set to value mutation");
                    self.value = i;
                }
                tracing::info!(
                    mutation.id = mutation_id_as_integer,
                    mutation.success = true,
                    name = "modality.mutation.injected",
                );
            } else {
                tracing::error!(
                    mutation.id = mutation_id_as_integer,
                    mutation.success = false,
                    name = "modality.mutation.injected",
                    "There was not a valid integer parameter with the correct name",
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
            name: Some("set_mutator".to_owned()),
            description: Some("Manipulates the value of a single integer".to_owned()),
            layer: Some(MutatorLayer::Implementational),
            group: Some("right".into()),
            operation: Some(MutatorOperation::SetToValue),
            statefulness: None,
            organization_custom_metadata: None,
            params: vec![OwnedMutatorParamDescriptor {
                value_type: AttrType::Integer,
                name: MutatorOperation::SetToValue.name().to_string(),
                description: Some(
                    "the critical parameter - what to set the targeted value in memory to"
                        .to_owned(),
                ),
                value_min: Some(AttrVal::Integer(-10_000)),
                value_max: Some(5000.into()),
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
fn mutator_b() -> Box<dyn ActuatorDescriptor + Send> {
    struct ReorderActuator {
        original: Vec<f64>,
        value: Vec<f64>,
    }
    impl ReorderActuator {
        fn new(v: Vec<f64>) -> Self {
            ReorderActuator {
                original: v.clone(),
                value: v,
            }
        }
    }

    #[async_trait]
    impl MutatorActuator for ReorderActuator {
        async fn inject(
            &mut self,
            mutation_id: Uuid,
            mut params: BTreeMap<AttrKey, AttrVal>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let mutation_id_as_integer = i128::from_le_bytes(*mutation_id.as_bytes());
            let reorder_by_amount = if let Some(AttrVal::Integer(i)) =
                params.remove(&AttrKey::from(MutatorOperation::Reorder.name()))
            {
                i
            } else {
                tracing::error!(
                    mutation.id = mutation_id_as_integer,
                    mutation.success = false,
                    name = "modality.mutation.injected",
                    "There was not a valid integer parameter with the correct name, reorder",
                );
                return Err(
                    "There was not a valid integer parameter with the correct name, reorder".into(),
                );
            };
            let target_index =
                if let Some(AttrVal::Integer(i)) = params.remove(&AttrKey::from("target_index")) {
                    i
                } else {
                    tracing::error!(mutation.id = mutation_id_as_integer, mutation.success = false,
                    name = "modality.mutation.injected",
                    "There was not a valid integer parameter with the correct name, target_index");
                    return Err(
                    "There was not a valid integer parameter with the correct name, target_index"
                        .into(),
                );
                };
            if target_index < 0 {
                tracing::error!(
                    mutation.id = mutation_id_as_integer,
                    mutation.success = false,
                    name = "modality.mutation.injected",
                    "The target_index value must be > 0.",
                );
                return Err("The target_index value must be > 0.".into());
            }
            let target_index = target_index as usize;
            if target_index >= self.value.len() {
                tracing::error!(
                    mutation.id = mutation_id_as_integer,
                    mutation.success = false,
                    name = "modality.mutation.injected",
                    "The target_index value must be less than the size of the list, 4.",
                );
                return Err(
                    "The target_index value must be less than the size of the list, 4.".into(),
                );
            }
            let destination = ((target_index as i64).wrapping_add(reorder_by_amount)
                / self.value.len() as i64)
                .unsigned_abs() as usize;
            tracing::info!(
                mutation.id = mutation_id_as_integer,
                mutation.success = true,
                name = "modality.mutation.injected"
            );
            self.value.swap(target_index, destination);
            Ok(())
        }

        async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            tracing::info!("Reset to original value");
            self.value = self.original.clone();
            Ok(())
        }
    }
    Box::new(CombinedMutator::new(
        ReorderActuator::new(vec![55.0, 77.0, 99.0, 101.0]),
        OwnedMutatorDescriptor {
            name: Some("reorder_list_mutator".to_owned()),
            description: Some("Manipulates the value of a list of numbers".to_owned()),
            layer: Some(MutatorLayer::Implementational),
            group: Some("left".into()),
            operation: Some(MutatorOperation::Reorder),
            statefulness: None,
            organization_custom_metadata: None,
            params: vec![
                OwnedMutatorParamDescriptor {
                    value_type: AttrType::Integer,
                    name: MutatorOperation::Reorder.name().to_string(),
                    description: Some(
                        "the critical parameter - how much to reorder the list by".to_owned(),
                    ),
                    value_min: Some(0.into()),
                    value_max: None,
                    default_value: None,
                    least_effect_value: None,
                    value_distribution_kind: None,
                    value_distribution_scaling: None,
                    value_distribution_option_set: None,
                    organization_custom_metadata: None,
                },
                OwnedMutatorParamDescriptor {
                    value_type: AttrType::Integer,
                    name: "target_index".to_string(),
                    description: Some("which member of the list to move around".to_owned()),
                    value_min: Some(0.into()),
                    value_max: Some(3.into()),
                    default_value: None,
                    least_effect_value: None,
                    value_distribution_kind: None,
                    value_distribution_scaling: None,
                    value_distribution_option_set: None,
                    organization_custom_metadata: None,
                },
            ],
        },
    ))
}
