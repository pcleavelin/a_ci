use std::collections::HashSet;

use bincode::{Decode, Encode};

use crate::outbound::db_custom::{TypelessId, Value};

#[derive(bincode::Encode)]
pub struct TransactionEntity<T> {
    name: String,
    data: T,
}
impl<T> TransactionEntity<T> {
    pub fn with(name: impl ToString, data: T) -> Self {
        Self {
            name: name.to_string(),
            data,
        }
    }
}

pub trait Storable: Diffable + std::fmt::Debug + std::any::Any {
    fn as_full_write_op(&self) -> WriteOperation
    where
        Self: Sized;
    fn as_partial_write_op(&self, other: &Self) -> WriteOperation
    where
        Self: Sized,
    {
        WriteOperation::Partial(self.diff(other, "").into_iter().map(Into::into).collect())
    }

    fn apply_partial_write(&mut self, op: &PartialWrite);

    fn set_field(&mut self, field_ident: &str, value: Value)
    where
        Self: Sized;

    fn field(&self, field_ident: &str) -> Option<Value>
    where
        Self: Sized;
}

pub trait Diffable {
    fn diff(&self, other: &Self, field_prefix: &str) -> Vec<FieldDiff>
    where
        Self: Sized;
}

pub trait Valuable {
    fn update_with_value(&mut self, field_name: &str, value: Value);
    fn as_value(&self, field_name: &str) -> Option<Value>;
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

impl Valuable for i64 {
    fn update_with_value(&mut self, _field_name: &str, value: Value) {
        if let Some(value) = value.as_int() {
            *self = *value;
        } else {
            panic!("value is not an i64");
        }
    }

    fn as_value(&self, _field_name: &str) -> Option<Value> {
        Some(Value::Int(*self))
    }
}

impl Valuable for String {
    fn update_with_value(&mut self, _field_name: &str, value: Value) {
        if let Some(value) = value.as_string() {
            *self = value.clone();
        } else {
            panic!("value is not a String");
        }
    }

    fn as_value(&self, _field_name: &str) -> Option<Value> {
        Some(Value::String(self.clone()))
    }
}

impl<T: std::cmp::Eq + std::hash::Hash> Valuable for HashSet<T>
where
    T: for<'a> From<&'a TypelessId>,
    TypelessId: for<'a> From<&'a T>,
{
    fn update_with_value(&mut self, _field_name: &str, value: Value) {
        if let Some(value) = value.as_array() {
            *self = value.iter().map(Into::into).collect();
        } else {
            panic!("value is not a String");
        }
    }

    fn as_value(&self, _field_name: &str) -> Option<Value> {
        Some(Value::Array(self.iter().map(|id| id.into()).collect()))
    }
}

impl Diffable for i64 {
    fn diff(&self, other: &Self, field_prefix: &str) -> Vec<FieldDiff>
    where
        Self: Sized,
    {
        if *self != *other {
            vec![FieldDiff::of(field_prefix, Value::Int(*other))]
        } else {
            vec![]
        }
    }
}

impl Diffable for String {
    fn diff(&self, other: &Self, field_prefix: &str) -> Vec<FieldDiff>
    where
        Self: Sized,
    {
        if *self != *other {
            vec![FieldDiff::of(field_prefix, Value::String(other.clone()))]
        } else {
            vec![]
        }
    }
}

impl<T: std::cmp::Eq + std::hash::Hash> Diffable for HashSet<T>
where
    TypelessId: for<'a> From<&'a T>,
{
    fn diff(&self, other: &Self, field_prefix: &str) -> Vec<FieldDiff>
    where
        Self: Sized,
    {
        let diff = self.difference(other);
        if diff.count() > 0 {
            vec![FieldDiff::of(
                field_prefix,
                Value::Array(other.iter().map(|id| id.into()).collect()),
            )]
        } else {
            vec![]
        }
    }
}

#[macro_export]
macro_rules! Storable {
    derive() {
        struct $n:ident {
            $(
                $field_name:ident: $field_type:ty,
            )*
        }
    } => {
        impl $crate::outbound::db_custom::write_set::Valuable for $n {
            fn update_with_value(&mut self, field_name: &str, value: Value) {
                self.set_field(field_name, value);
            }

            fn as_value(&self, field_name: &str) -> Option<Value> {
                self.field(field_name)
            }
        }

        impl Storable for $n {
            fn as_full_write_op(&self) -> WriteOperation
            where
                Self: Sized
            {
                use $crate::outbound::db_custom::write_set::TransactionEntity;

                let data =
                    ::bincode::encode_to_vec(TransactionEntity::with(stringify!($n), self), ::bincode::config::standard())
                        .expect("encode MUST succeed");

                WriteOperation::Full(data)
            }

            fn apply_partial_write(&mut self, op: &$crate::outbound::db_custom::write_set::PartialWrite)
            where
                Self: Sized,
            {
                let value = Value::from_bytes(&op.data);
                self.set_field(&op.field_ident, value);
            }

            fn set_field(&mut self, field_ident: &str, value: Value)
            where
                Self: Sized
            {
                use $crate::outbound::db_custom::write_set::Valuable;

                let mut idents = field_ident.split(".");
                let next = idents.next().unwrap();

                match next {
                    $(
                        stringify!($field_name) => {
                            self.$field_name.update_with_value(&field_ident[(next.len()+1)..], value);
                        }
                    ),*

                    _ => panic!("set_field: invalid field '{next}'"),
                }
            }

            fn field(&self, field_ident: &str) -> Option<Value>
            where
                Self: Sized
            {
                use $crate::outbound::db_custom::write_set::Valuable;

                let mut idents = field_ident.split(".");
                let next = idents.next().unwrap();

                let nested_field_name = if next.len()+1 >= field_ident.len() {
                    ""
                } else {
                    &field_ident[(next.len()+1)..]
                };

                match next {
                    $(
                        stringify!($field_name) => {
                            println!("{} - {}", stringify!($field_name), field_ident);
                            self.$field_name.as_value(nested_field_name)
                        }
                    ),*

                    _ => panic!("set_field: invalid field '{next}'"),
                }
            }
        }

        impl Diffable for $n {
            fn diff(&self, other: &Self, field_prefix: &str) -> Vec<$crate::outbound::db_custom::write_set::FieldDiff>
            where
                Self: Sized
            {
                use $crate::outbound::db_custom::write_set::Valuable;

                let mut modifications = vec![];

                $(
                    modifications.extend(self.$field_name.diff(&other.$field_name, &format!("{}{}.", field_prefix, stringify!($field_name))).into_iter());
                )*

                modifications
            }
        }
    };
}
