use std::marker::PhantomData;

pub struct Resource {}

pub enum DataStore {
    Direct,
}

impl Resource {
    pub fn fetch_all<T>() -> Vec<T> {
        vec![]
    }
}
