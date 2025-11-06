use bincode::{Decode, Encode};
use facet::{Facet, Field, PtrConst, Shape};

use crate::outbound::db_custom::{TypelessId, Value};

#[derive(bincode::Encode)]
struct TransactionEntity<T> {
    name: String,
    data: T,
}
impl<T> TransactionEntity<T> {
    fn with(name: impl ToString, data: T) -> Self {
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
        WriteOperation::Partial(self.diff(other).into_iter().map(Into::into).collect())
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
    fn diff(&self, other: &Self) -> Vec<FieldDiff>
    where
        Self: Sized;
}

struct ShapeVal<'a, 's>(&'a Shape, PtrConst<'a>, &'s str);

impl<'a, 's> Diffable for ShapeVal<'a, 's> {
    fn diff(&self, other: &Self) -> Vec<FieldDiff>
    where
        Self: Sized,
    {
        let mut modifications = vec![];

        let facet::Type::User(user) = self.0.ty else {
            panic!("invalid type");
        };

        match user {
            facet::UserType::Struct(struct_type) => {
                //println!("{:#?}", struct_type.fields);

                for field in struct_type.fields {
                    let field_shape = field.shape();
                    //println!("{:#?}", field_shape);

                    let (self_field_ptr, other_field_ptr) = unsafe {
                        let offset = field.offset.try_into().unwrap();

                        let self_ptr: *const u8 = self.1.as_ptr();
                        let me_field_ptr = self_ptr.offset(offset);

                        let other_ptr: *const u8 = other.1.as_ptr();
                        let other_field_ptr = other_ptr.offset(offset);

                        let me = PtrConst::new(std::ptr::NonNull::new_unchecked(
                            me_field_ptr as *mut u8,
                        ));
                        let other = PtrConst::new(std::ptr::NonNull::new_unchecked(
                            other_field_ptr as *mut u8,
                        ));

                        (me, other)
                    };

                    match field_shape.def {
                        facet::Def::Undefined => {
                            let me = ShapeVal(
                                field_shape,
                                self_field_ptr,
                                &format!("{}{}.", self.2, field.name),
                            );
                            let other = ShapeVal(
                                field_shape,
                                other_field_ptr,
                                &format!("{}{}.", self.2, field.name),
                            );

                            modifications.extend(me.diff(&other).into_iter());
                        }
                        facet::Def::Scalar => {
                            let partial_eq = field_shape.vtable.partial_eq.unwrap();

                            let are_partially_eq =
                                unsafe { partial_eq(self_field_ptr, other_field_ptr) };

                            if !are_partially_eq {
                                //println!("{:#?}", field);
                                //println!("{:#?}", field_shape);
                                modifications.push(FieldDiff::of(
                                    format!("{}{}", self.2, field.name),
                                    Value::from_field_value(other, field),
                                ));
                            }
                        }
                        facet::Def::Map(map_def) => todo!(),
                        facet::Def::Set(set_def) => {}
                        facet::Def::List(list_def) => todo!(),
                        facet::Def::Array(array_def) => todo!(),
                        facet::Def::NdArray(nd_array_def) => todo!(),
                        facet::Def::Slice(slice_def) => todo!(),
                        facet::Def::Option(option_def) => todo!(),
                        facet::Def::Pointer(pointer_def) => todo!(),
                        _ => todo!(),
                    }
                }
            }
            facet::UserType::Enum(enum_type) => todo!(),
            facet::UserType::Union(union_type) => panic!("union types are unsupported"),
            facet::UserType::Opaque => panic!("opaque types are invalid"),
        }

        modifications
    }
}

impl<'a, 's> ShapeVal<'a, 's> {
    fn set_field(&self, field_ident: &str, value: Value) {
        let mut idents = field_ident.split(".");
        let next = idents.next().unwrap();

        let facet::Type::User(user) = self.0.ty else {
            panic!("invalid type");
        };

        println!("next: {next}");

        match user {
            facet::UserType::Struct(struct_type) => {
                if let Some(field) = struct_type.fields.iter().find(|f| f.name == next) {
                    let field_shape = field.shape();
                    match field_shape.ty {
                        facet::Type::Primitive(primitive_type) => {
                            let layout = field_shape
                                .layout
                                .sized_layout()
                                .expect("primitive type is sized");
                            let primitive_size = layout.size();
                            if Some(primitive_size) != value.size() {
                                panic!("invalid primitive size");
                            }

                            unsafe {
                                let self_ptr = self.1.as_byte_ptr() as *mut u8;
                                let field_ptr = self_ptr.offset(field.offset.try_into().unwrap());

                                match value {
                                    Value::String(_) | Value::Array(_) => {
                                        panic!("String & Array are not primitive types")
                                    }

                                    Value::Int(value) => {
                                        let r = (field_ptr as *mut i64).as_mut().unwrap();
                                        println!("value: {value}, r before: {r}");
                                        *r = value;
                                        println!("r after: {r}");
                                    }
                                    Value::Float(value) => *(field_ptr as *mut f64) = value,
                                    Value::Bool(value) => *(field_ptr as *mut bool) = value,
                                }
                            }
                        }
                        facet::Type::Sequence(sequence_type) => todo!(),
                        facet::Type::User(user_type) => match user_type {
                            facet::UserType::Struct(struct_type) => {
                                let field_ptr = unsafe {
                                    let self_ptr = self.1.as_byte_ptr() as *mut u8;
                                    let field_ptr =
                                        self_ptr.offset(field.offset.try_into().unwrap());

                                    PtrConst::new(std::ptr::NonNull::new_unchecked(field_ptr))
                                };
                                let field_val = ShapeVal(field_shape, field_ptr, "");

                                field_val.set_field(&field_ident[(field.name.len() + 1)..], value);
                            }
                            facet::UserType::Enum(enum_type) => todo!(),
                            facet::UserType::Union(union_type) => todo!(),
                            facet::UserType::Opaque if field_shape.type_identifier == "String" => {
                                let Value::String(value) = value else {
                                    panic!("got non-string value");
                                };

                                let field_ref = unsafe {
                                    let self_ptr = self.1.as_byte_ptr() as *mut u8;
                                    let field_ptr = self_ptr
                                        .offset(field.offset.try_into().unwrap())
                                        as *mut String;

                                    field_ptr.as_mut().unwrap()
                                };

                                *field_ref = value;
                            }
                            facet::UserType::Opaque => panic!("unsupported type"),
                        },
                        facet::Type::Pointer(pointer_type) => todo!(),
                    }
                } else {
                    panic!("couldn't find field: {next}");
                }
            }
            facet::UserType::Enum(enum_type) => todo!(),
            facet::UserType::Union(union_type) => panic!("union types are unsupported"),
            facet::UserType::Opaque => panic!("opaque types are invalid"),
        }
    }
}

impl<T: for<'a> Facet<'a> + for<'a> FieldValueRef<'a>> Diffable for T {
    fn diff(&self, other: &Self) -> Vec<FieldDiff>
    where
        Self: Sized,
    {
        let (me, other) = unsafe {
            let me = ShapeVal(
                Self::SHAPE,
                PtrConst::new(std::ptr::NonNull::new_unchecked(
                    self as *const _ as *mut u8,
                )),
                "",
            );

            let other = ShapeVal(
                Self::SHAPE,
                PtrConst::new(std::ptr::NonNull::new_unchecked(
                    other as *const _ as *mut u8,
                )),
                "",
            );

            (me, other)
        };

        me.diff(&other)
    }
}

impl<T: for<'a> Facet<'a> + Diffable + Encode + std::fmt::Debug> Storable for T {
    fn as_full_write_op(&self) -> WriteOperation
    where
        Self: Sized,
    {
        let data = bincode::encode_to_vec(
            TransactionEntity::with(Self::SHAPE.type_identifier, self),
            bincode::config::standard(),
        )
        .expect("encode MUST succeed");

        WriteOperation::Full(data)
    }

    fn apply_partial_write(&mut self, op: &PartialWrite) {
        println!("{:#?}", op);
        println!("before: {:#?}", self);
        let value = Value::from_bytes(&op.data);
        self.set_field(&op.field_ident, value);
        println!("after: {:#?}", self);
    }

    fn set_field(&mut self, field_ident: &str, value: Value)
    where
        Self: for<'a> Facet<'a> + Sized,
    {
        let me = unsafe {
            ShapeVal(
                Self::SHAPE,
                PtrConst::new(std::ptr::NonNull::new_unchecked(
                    self as *const _ as *mut u8,
                )),
                "",
            )
        };

        me.set_field(field_ident, value);
    }

    fn field(&self, field_ident: &str) -> Option<Value>
    where
        Self: for<'a> Facet<'a> + Sized,
    {
        None
    }
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

trait FieldValueRef<'a> {
    fn get_field_value_as_ref<F>(&'a self, field: &Field) -> &'a F {
        unsafe {
            let container_ptr = self as *const _ as *const u8;
            let field_ptr = container_ptr.offset(field.offset.try_into().unwrap()) as *const F;

            field_ptr.as_ref().unwrap()
        }
    }
}

impl<'a, T: Facet<'a>> FieldValueRef<'a> for T {}

impl<'a, 's> FieldValueRef<'a> for ShapeVal<'a, 's> {
    fn get_field_value_as_ref<F>(&'a self, field: &Field) -> &'a F {
        unsafe {
            let container_ptr: *const u8 = self.1.as_ptr();
            let field_ptr = container_ptr.offset(field.offset.try_into().unwrap()) as *const F;

            field_ptr.as_ref().unwrap()
        }
    }
}

impl Value {
    fn from_field_value<'a, T: FieldValueRef<'a>>(container: &'a T, field: &Field) -> Self {
        let shape = field.shape();

        match shape.ty {
            facet::Type::Primitive(primitive_type) => match primitive_type {
                facet::PrimitiveType::Boolean => {
                    Self::Bool(*container.get_field_value_as_ref(field))
                }
                facet::PrimitiveType::Numeric(numeric_type) => {
                    let field_layout = field.shape().layout.sized_layout().unwrap();

                    const I32_SIZE: usize = size_of::<i32>();
                    const I64_SIZE: usize = size_of::<i64>();

                    match (field_layout.size(), numeric_type) {
                        (I32_SIZE, facet::NumericType::Integer { signed }) if signed => {
                            Value::Int(*container.get_field_value_as_ref::<i32>(field) as i64)
                        }
                        (I32_SIZE, facet::NumericType::Integer { .. }) => {
                            Value::Int(*container.get_field_value_as_ref::<u32>(field) as i64)
                        }

                        (I64_SIZE, facet::NumericType::Integer { signed }) if signed => {
                            Value::Int(*container.get_field_value_as_ref::<u64>(field) as i64)
                        }
                        (I64_SIZE, facet::NumericType::Integer { .. }) => {
                            Value::Int(*container.get_field_value_as_ref::<i64>(field) as i64)
                        }
                        _ => panic!("invalid numeric size"),
                    }
                }
                facet::PrimitiveType::Textual(textual_type) => todo!(),
                facet::PrimitiveType::Never => todo!(),
            },

            facet::Type::Sequence(sequence_type) => todo!(),
            facet::Type::User(user_type) => {
                if shape.type_identifier == "String" {
                    Value::String(container.get_field_value_as_ref::<String>(field).clone())
                } else {
                    todo!("asdkjhfadsfkdasjhfdssklsadkljfkljasdhfkjlhsdfklsdkfhsd");
                }
            }
            facet::Type::Pointer(pointer_type) => todo!("alkjhdfklajdhs"),
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
