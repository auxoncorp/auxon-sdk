//! Standard-library-required traits and utilities for modality mutator implementation
//! The core traits are `descriptor::InfallibleFlatMutatorDescriptor` and `actuator::MutatorActuator`,

pub mod params_attributes;

pub mod actuator;
pub mod attrs;
pub mod descriptor;

pub mod mutator {
    use async_trait::async_trait;

    use crate::api::{AttrKey, AttrVal};
    use crate::mutator_protocol::actuator::MutatorActuator;
    use crate::mutator_protocol::descriptor::MutatorDescriptor;
    use std::collections::BTreeMap;

    pub trait ActuatorDescriptor: MutatorActuator + MutatorDescriptor {}

    pub struct CombinedMutator<A, D>
    where
        A: MutatorActuator,
        D: MutatorDescriptor,
    {
        actuator: A,
        descriptor: D,
    }

    impl<A: MutatorActuator, D: MutatorDescriptor> CombinedMutator<A, D> {
        pub fn new(actuator: A, descriptor: D) -> Self {
            CombinedMutator {
                actuator,
                descriptor,
            }
        }
        pub fn actuator_ref(&self) -> &A {
            &self.actuator
        }
        pub fn actuator_mut(&mut self) -> &mut A {
            &mut self.actuator
        }
        pub fn descriptor_ref(&self) -> &D {
            &self.descriptor
        }
        pub fn descriptor_mut(&mut self) -> &mut D {
            &mut self.descriptor
        }
    }

    #[async_trait]
    impl<A: MutatorActuator + Send + Sync, D: MutatorDescriptor + Send + Sync> MutatorActuator
        for CombinedMutator<A, D>
    {
        async fn inject(
            &mut self,
            mutation_id: uuid::Uuid,
            params: BTreeMap<AttrKey, AttrVal>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.actuator.inject(mutation_id, params).await
        }

        async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.actuator.reset().await
        }
    }

    impl<A: MutatorActuator, D: MutatorDescriptor> MutatorDescriptor for CombinedMutator<A, D> {
        fn get_description_attributes(&self) -> Box<dyn Iterator<Item = (AttrKey, AttrVal)> + '_> {
            self.descriptor.get_description_attributes()
        }
    }

    impl<A: MutatorActuator + Send + Sync, D: MutatorDescriptor + Send + Sync> ActuatorDescriptor
        for CombinedMutator<A, D>
    {
    }
}

#[cfg(test)]
mod tests {
    use crate::api::{AttrKey, AttrType, AttrVal};
    use crate::mutator_protocol::actuator::MutatorActuator;
    use crate::mutator_protocol::descriptor::owned::{
        MutatorOperation, OrganizationCustomMetadata, OwnedMutatorDescriptor,
        OwnedMutatorParamDescriptor,
    };
    use crate::mutator_protocol::descriptor::MutatorDescriptor;
    use crate::mutator_protocol::mutator::CombinedMutator;
    use async_trait::async_trait;
    use std::collections::{BTreeMap, HashMap};
    use uuid::Uuid;

    pub struct OwnedValueMutator {
        mutation_active: bool,
        last_natural: i64,
        inner: i64,
    }
    impl OwnedValueMutator {
        pub fn new(initial: i64) -> Self {
            OwnedValueMutator {
                mutation_active: false,
                last_natural: initial,
                inner: initial,
            }
        }
        pub fn current(&self) -> i64 {
            self.inner
        }
    }

    #[async_trait]
    impl MutatorActuator for OwnedValueMutator {
        async fn inject(
            &mut self,
            mutation_id: Uuid,
            params: BTreeMap<AttrKey, AttrVal>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let mutation_id_as_integer = i128::from_le_bytes(*mutation_id.as_bytes());
            let v = if let Some((_k, v)) = params.into_iter().next() {
                v
            } else {
                tracing::error!(
                    mutation.id = mutation_id_as_integer,
                    mutation.success = false,
                    name = "modality.mutation.injected",
                    "Expected exactly one one parameter",
                );
                return Ok(());
            };
            if let AttrVal::Integer(i) = v {
                tracing::info!(
                    mutation.id = mutation_id_as_integer,
                    mutation.success = true,
                    name = "modality.mutation.injected"
                );
                if !self.mutation_active {
                    self.last_natural = self.inner;
                }
                self.inner = i;
                self.mutation_active = true;
            } else {
                tracing::error!(
                    mutation.id = mutation_id_as_integer,
                    mutation.success = false,
                    name = "modality.mutation.injected",
                    "Expected an integer-type parameter",
                );
                panic!("Unexpected param of value {v:?}");
            }
            Ok(())
        }

        async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.inner = self.last_natural;
            Ok(())
        }
    }

    #[tokio::test]
    async fn description_and_actuation_in_one_spot() {
        let ovm = OwnedValueMutator::new(5);

        let mut combined_mutator = CombinedMutator::new(
            ovm,
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
            },
        );
        let all_attrs: HashMap<AttrKey, AttrVal> =
            combined_mutator.get_description_attributes().collect();
        assert!(!all_attrs.is_empty());
        assert_eq!(
            &AttrVal::String("foo".into()),
            all_attrs.get(&AttrKey::from("mutator.name")).unwrap()
        );
        assert_eq!(
            &AttrVal::String("set_to_value".into()),
            all_attrs.get(&AttrKey::from("mutator.operation")).unwrap()
        );
        assert_eq!(
            &AttrVal::Integer(99.into()),
            all_attrs
                .get(&AttrKey::from("mutator.some_jerks.fleet"))
                .unwrap()
        );

        assert_eq!(
            &AttrVal::String("Integer".into()),
            all_attrs
                .get(&AttrKey::from("mutator.params.set_to_value.value_type"))
                .unwrap()
        );

        assert_eq!(5, combined_mutator.actuator_ref().current());
        combined_mutator
            .inject(
                uuid::Uuid::nil(),
                std::iter::once((
                    AttrKey::from(MutatorOperation::SetToValue.name()),
                    AttrVal::Integer(42),
                ))
                .collect(),
            )
            .await
            .unwrap();
        assert_eq!(42, combined_mutator.actuator_ref().current());
        combined_mutator.reset().await.unwrap();
        assert_eq!(5, combined_mutator.actuator_ref().current());
    }
}
