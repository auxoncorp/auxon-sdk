use std::{
    collections::{BTreeMap, HashMap},
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

use auxon_sdk::{
    api::AttrVal,
    mutation_plane::types::{MutationId, MutatorId},
    mutator_protocol::descriptor::owned::{OwnedMutatorDescriptor, OwnedMutatorParamDescriptor},
    plugin_utils::{config::Config, mutation::Mutator},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    auxon_sdk::init_tracing!();

    let value = Arc::new(AtomicI64::new(0));

    println!("Connecting...");
    let config = Config::load_common()?;
    let mut mh = config.connect_and_authenticate_mutation().await?;
    mh.register_mutator(Box::new(OffsetNumberMutator::new(value.clone())))
        .await?;
    println!("Connected");
    let _jh = tokio::task::spawn(async move { mh.message_loop().await.unwrap() });

    loop {
        println!("{}", value.load(std::sync::atomic::Ordering::Relaxed));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

//////////////////
// Mutator impl //
//////////////////

struct OffsetNumberMutator {
    id: MutatorId,
    mutations: HashMap<MutationId, i64>,
    target: Arc<AtomicI64>,
}

impl OffsetNumberMutator {
    fn new(target: Arc<AtomicI64>) -> Self {
        Self {
            id: MutatorId::allocate(),
            mutations: Default::default(),
            target,
        }
    }
}

impl Mutator for OffsetNumberMutator {
    fn id(&self) -> MutatorId {
        self.id
    }

    fn descriptor(&self) -> OwnedMutatorDescriptor {
        OwnedMutatorDescriptor {
            name: Some("OffsetNumber".to_string()),
            description: Some("Offset the nubmer being printed by a certain amount".to_string()),
            params: vec![OwnedMutatorParamDescriptor {
                value_type: auxon_sdk::api::AttrType::Integer,
                name: "offset".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn inject(&mut self, mutation_id: MutationId, mut params: BTreeMap<String, AttrVal>) -> bool {
        let Some(offset) = params.remove("offset").and_then(|v| v.as_int().ok()) else {
            return false;
        };

        self.mutations.insert(mutation_id, offset);
        self.target
            .fetch_add(offset, std::sync::atomic::Ordering::Relaxed);
        true
    }

    fn clear_mutation(&mut self, mutation_id: &MutationId) {
        let Some(offset) = self.mutations.remove(mutation_id) else {
            return;
        };
        self.target
            .fetch_add(-offset, std::sync::atomic::Ordering::Relaxed);
    }

    fn reset(&mut self) {
        let total_offset: i64 = self.mutations.drain().map(|(_k, v)| v).sum();
        self.target
            .store(-total_offset, std::sync::atomic::Ordering::Relaxed);
    }
}
