use crate::outbound::db_custom::{
    Value,
    write_set::{Diffable, Storable},
};

#[derive(Debug)]
struct DbRepository {
    name: String,
    deprecated_field: i32,
}

impl Storable for DbRepository {
    fn as_full_write_op(&self) -> crate::outbound::db_custom::write_set::WriteOperation {
        todo!()
    }

    fn as_partial_write_op(
        &self,
        other: &Self,
    ) -> crate::outbound::db_custom::write_set::WriteOperation {
        todo!()
    }

    fn apply_partial_write(&mut self, op: &crate::outbound::db_custom::write_set::PartialWrite) {
        todo!()
    }

    fn field(&self, field_ident: &str) -> Option<Value>
    where
        Self: Sized,
    {
        todo!()
    }

    fn set_field(&mut self, field_ident: &str, value: Value)
    where
        Self: Sized,
    {
        todo!()
    }
}

impl Diffable for DbRepository {
    fn diff(&self, other: &Self) -> Vec<crate::outbound::db_custom::write_set::FieldDiff> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct Repository {
    pub name: String,
}

impl Storable for Repository {
    fn as_full_write_op(&self) -> crate::outbound::db_custom::write_set::WriteOperation
    where
        Self: Sized,
    {
        todo!()
    }

    fn as_partial_write_op(
        &self,
        other: &Self,
    ) -> crate::outbound::db_custom::write_set::WriteOperation
    where
        Self: Sized,
    {
        todo!()
    }

    fn apply_partial_write(&mut self, op: &crate::outbound::db_custom::write_set::PartialWrite) {
        todo!()
    }

    fn set_field(&mut self, field_ident: &str, value: Value)
    where
        Self: Sized,
    {
        todo!()
    }

    fn field(&self, field_ident: &str) -> Option<Value>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl Diffable for Repository {
    fn diff(&self, other: &Self) -> Vec<crate::outbound::db_custom::write_set::FieldDiff>
    where
        Self: Sized,
    {
        todo!()
    }
}
