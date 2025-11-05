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

#[macro_export]
macro_rules! primitive_assign {
    ($self:ident, $field_name:ident, $value:ident, $ty:ty, $variant:ident) => {{
        let Value::$variant(value) = $value else {
            panic!(
                "expected '{}' to be a '{}'",
                stringify!($field_name),
                stringify!($ty)
            );
        };

        $self.$field_name = value;
    }};
}

#[macro_export]
macro_rules! primitive_return {
    ($self:ident, $field_name:ident, $variant:ident) => {
        Some(Value::$variant($self.$field_name.clone()))
    };
}

#[macro_export]
macro_rules! db_type {
    ($self:ident, $field_name:ident, i64, $value:ident, $field_ident:ident) => {
        $crate::primitive_assign!($self, $field_name, $value, i64, Int)
    };
    ($self:ident, $field_name:ident, String, $value:ident, $field_ident:ident) => {
        $crate::primitive_assign!($self, $field_name, $value, String, String)
    };

    ($self:ident, $field_name:ident, (HashSet<Id<$ty:ty>>), $value:ident, $field_ident:ident) => {
        {
            let Value::Array(value) = $value else {
                panic!(
                    "expected '{}' to be a 'Vec<Id<{}>>'",
                    stringify!($field_name),
                    stringify!($ty)
                );
            };

            $self.$field_name = value.into_iter().map(|id| Id::<_>::new(id.0)).collect();
        }
    };

    ($self:ident, $field_name:ident, $field_type:ty, $value:ident, $field_ident:ident) => {
        {
            let field_name_len = stringify!($field_name).len();
            if field_name_len == $field_ident.len() {
                panic!("attempt to assign non-primitive");
            }

            $self.$field_name.set_field(&$field_ident[(field_name_len+1)..], $value);
        }
    };

    ($self:ident, $field_name:ident, i64, $field_ident:ident) => {{
        Some(Value::Int($self.$field_name.clone()))
    }};
    ($self:ident, $field_name:ident, String, $field_ident:ident) => {{
        Some(Value::String($self.$field_name.clone()))
    }};
    ($self:ident, $field_name:ident, (HashSet<Id<$ty:ty>>), $field_ident:ident) => {
        Some(Value::Array($self.$field_name.iter().map(|id| id.into()).collect()))
    };
    ($self:ident, $field_name:ident, $field_type:ty, $field_ident:ident) => {{
        let field_name_len = stringify!($field_name).len();
        if field_name_len == $field_ident.len() {
            panic!("attempt to assign non-primitive");
        }

        $self.$field_name.field(&$field_ident[(field_name_len+1)..])
    }};

    (@diff $self:ident, $other:ident, $modifications:ident, $field_name:ident, i64) => {
        if $self.$field_name != $other.$field_name {
            $modifications.push(FieldDiff::of(
                stringify!($field_name),
                Value::Int($other.$field_name.clone()),
            ));
        }
    };
    (@diff $self:ident, $other:ident, $modifications:ident, $field_name:ident, String) => {
        if $self.$field_name != $other.$field_name {
            $modifications.push(FieldDiff::of(
                stringify!($field_name),
                Value::String($other.$field_name.clone()),
            ));
        }
    };
    (@diff $self:ident, $other:ident, $modifications:ident, $field_name:ident, (HashSet<Id<$ty:ty>>)) => {
        let diff = $self.$field_name.difference(&$other.$field_name);
        if diff.count() > 0 {
            $modifications.push(FieldDiff::of(
                stringify!($field_name),
                Value::Array($other.$field_name.iter().map(|id| id.into()).collect())
            ));
        }
    };
    (@diff $self:ident, $other:ident, $modifications:ident, $field_name:ident, $field_type:ty) => {
        $modifications.extend(
            $self.$field_name
                .diff(&$other.$field_name)
                .into_iter()
                .map(|diff| {
                    // NOTE(pcleavelin): we have to recreate the `field_ident` path via recursive
                    // string building lol
                    FieldDiff::of(
                        format!("{}.{}", stringify!($field_name), &diff.field_ident),
                        diff.value
                    )
                })
        );
    };

    {
        $(
            struct $db_type_name:ident {
                $(
                    $field_name:ident: $field_type:tt,
                )*
            }
        )*
    } => {
        $(
            #[derive(Debug, Clone, ::bincode::Encode, ::bincode::Decode)]
            pub struct $db_type_name {
                $(
                    $field_name: $field_type,
                )*
            }

            impl Storable for $db_type_name {
                fn as_full_write_op(&self) -> WriteOperation
                where
                    Self: Sized,
                {
                    let data =
                        bincode::encode_to_vec(A::with(stringify!($db_type_name), self), bincode::config::standard())
                            .expect("encode MUST succeed");

                    WriteOperation::Full(data)
                }
                fn as_partial_write_op(&self, other: &Self) -> WriteOperation
                where
                    Self: Sized,
                {
                    WriteOperation::Partial(self.diff(other).into_iter().map(Into::into).collect())
                }

                fn apply_partial_write(&mut self, op: &write_set::PartialWrite) {
                    let value = Value::from_bytes(&op.data);
                    self.set_field(&op.field_ident, value);
                }

                fn set_field(&mut self, field_ident: &str, value: Value)
                    where Self: Sized,
                {
                    let mut idents = field_ident.split(".");
                    let next = idents.next().unwrap();

                    match next {
                        $(
                            stringify!($field_name) => {
                                db_type!(self, $field_name, $field_type, value, field_ident)
                            }
                        ),*

                        _ => panic!("set_field: invalid field '{next}'"),
                    }
                }

                fn field(&self, field_ident: &str) -> Option<Value>
                where
                    Self: Sized,
                {
                    let mut idents = field_ident.split(".");
                    let next = idents.next().unwrap();

                    match next {
                        $(
                            stringify!($field_name) => {
                                db_type!(self, $field_name, $field_type, field_ident)
                            }
                        ),*

                        _ => panic!("field: invalid field '{next}'"),
                    }
                }
            }

            impl Diffable for $db_type_name {
                fn diff(&self, other: &Self) -> Vec<FieldDiff>
                where
                    Self: Sized,
                {
                    let mut modifications = vec![];

                    $(
                        db_type!(@diff self, other, modifications, $field_name, $field_type);
                    )*

                    modifications
                }
            }
        )*
    };
}
