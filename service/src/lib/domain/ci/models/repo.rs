use crate::outbound::db_custom::{DocumentField, Modifiable, Storable, Value};

#[derive(Debug, Clone)]
pub struct Repository {
    pub name: String,
}

impl Storable for Repository {
    fn cloned(&self) -> Box<dyn Storable + Send> {
        Box::new(self.clone())
    }

    fn write_modification(&mut self, field: &DocumentField) {
        todo!()
    }

    fn field(&self, field: &'static str) -> Option<Value> {
        todo!()
    }
}

impl Modifiable for Repository {
    fn modifications_between(&self, other: &Self) -> Vec<DocumentField> {
        todo!()
    }
}
