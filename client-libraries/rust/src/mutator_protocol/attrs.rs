pub mod mutator {
    use crate::api::AttrKey;

    pub const ID: AttrKey = AttrKey::new_static("mutator.id");
    pub const NAME: AttrKey = AttrKey::new_static("mutator.name");
    pub const DESCRIPTION: AttrKey = AttrKey::new_static("mutator.description");
    pub const LAYER: AttrKey = AttrKey::new_static("mutator.layer");
    pub const GROUP: AttrKey = AttrKey::new_static("mutator.group");
    pub const STATEFULNESS: AttrKey = AttrKey::new_static("mutator.statefulness");
    pub const OPERATION: AttrKey = AttrKey::new_static("mutator.operation");
    pub const SAFETY: AttrKey = AttrKey::new_static("mutator.safety");
    pub const SOURCE_FILE: AttrKey = AttrKey::new_static("mutator.source.file");
    pub const SOURCE_LINE: AttrKey = AttrKey::new_static("mutator.source.line");

    pub const MUTATION_EDGE_ID: AttrKey = AttrKey::new_static("mutator.mutation_edge_id");
    pub const RECEIVE_TIME: AttrKey = AttrKey::new_static("mutator.receive_time");
}
