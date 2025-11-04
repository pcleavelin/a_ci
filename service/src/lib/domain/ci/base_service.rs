use super::{interface::CiService, models::repo::Repository};
use crate::outbound::{
    db_custom::{DatabaseAccessor, Id},
    resource::Resource,
};

#[derive(Clone)]
pub struct Service {
    store: DatabaseAccessor,
}

impl Service {
    pub fn new() -> Self {
        Self {
            store: DatabaseAccessor::new(Default::default()),
        }
    }
}

impl CiService for Service {
    fn repos(&self) -> Vec<Repository> {
        self.store.get_all()
    }
}
