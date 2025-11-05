use bincode::{Decode, Encode};

use crate::outbound::db_custom::{TypelessId, Value};

pub trait Storable: Diffable + std::fmt::Debug + std::any::Any {
    fn as_full_write_op(&self) -> WriteOperation
    where
        Self: Sized;
    fn as_partial_write_op(&self, other: &Self) -> WriteOperation
    where
        Self: Sized;

    fn apply_partial_write(&mut self, op: &PartialWrite);

    fn set_field(&mut self, field_ident: &str, value: Value)
    where
        Self: Sized;

    fn field(&self, field_ident: &str) -> Option<Value>
    where
        Self: Sized;
}

pub trait Diffable {
    fn diff(&self, other: &Self) -> Vec<FieldDiff>
    where
        Self: Sized;
}

pub struct FieldDiff {
    pub(super) field_ident: String,
    pub(super) value: Value,
}

impl FieldDiff {
    pub fn of(field_ident: impl ToString, value: Value) -> Self {
        Self {
            field_ident: field_ident.to_string(),
            value,
        }
    }
}

impl From<FieldDiff> for PartialWrite {
    fn from(value: FieldDiff) -> Self {
        let FieldDiff { field_ident, value } = value;

        PartialWrite {
            field_ident,
            data: value.as_bytes(),
        }
    }
}

#[derive(Debug, Default, Encode, Decode)]
pub struct WriteSet {
    // TODO: make an actual set
    pub(super) writes: Vec<Write>,
}

#[derive(Debug, Encode, Decode)]
pub struct Write {
    pub(super) id: TypelessId,
    pub(super) operation: WriteOperation,
}

#[derive(Debug, Encode, Decode)]
pub enum WriteOperation {
    Full(Vec<u8>),
    Partial(Vec<PartialWrite>),
}

#[derive(Debug, Encode, Decode)]
pub struct PartialWrite {
    pub(super) field_ident: String,
    pub(super) data: Vec<u8>,
}
