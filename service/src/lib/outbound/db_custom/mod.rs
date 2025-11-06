pub mod write_set;

use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use facet::Facet;

use crate::outbound::db_custom::write_set::{Diffable, Storable, Write, WriteOperation, WriteSet};

#[derive(
    Default, Copy, Clone, PartialOrd, PartialEq, Eq, Hash, Debug, bincode::Encode, bincode::Decode,
)]
struct TransactionId(u64);

impl From<u64> for TransactionId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Debug, Copy, Clone, facet::Facet)]
pub struct Id<T> {
    id: u64,
    _type: PhantomData<T>,
}

impl<T> Id<T> {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            _type: PhantomData,
        }
    }
}

impl<T> bincode::Encode for Id<T> {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        bincode::enc::Encode::encode(&self.id, encoder)?;

        Ok(())
    }
}

impl<T, C> bincode::Decode<C> for Id<T> {
    fn decode<D: bincode::de::Decoder<Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let id = bincode::de::Decode::decode(decoder)?;

        Ok(Self {
            id,
            _type: PhantomData,
        })
    }
}

impl<'de, T, C> bincode::BorrowDecode<'de, C> for Id<T> {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let id = bincode::de::Decode::decode(decoder)?;

        Ok(Self {
            id,
            _type: PhantomData,
        })
    }
}

impl<T> std::cmp::Eq for Id<T> {}
impl<T> std::cmp::PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl<T> std::hash::Hash for Id<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Default, Copy, Clone, PartialEq, Eq, Hash, Debug, bincode::Encode, bincode::Decode)]
struct TypelessId(u64);

impl<T> From<Id<T>> for TypelessId {
    fn from(id: Id<T>) -> Self {
        Self(id.id)
    }
}

impl<T> From<&Id<T>> for TypelessId {
    fn from(id: &Id<T>) -> Self {
        Self(id.id)
    }
}

impl From<u64> for TypelessId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Debug, Default)]
pub struct Database {
    log: Vec<Transaction>,
    snapshots: HashMap<TransactionId, Snapshot>,
    registered_types: HashMap<String, fn(&[u8]) -> Box<dyn Storable + Send>>,
}

impl Database {
    fn register_type(
        &mut self,
        name: impl ToString,
        decode_fn: fn(&[u8]) -> Box<dyn Storable + Send>,
    ) {
        self.registered_types.insert(name.to_string(), decode_fn);
    }

    fn decode(&self, data: &[u8]) -> Result<Box<dyn Storable + Send>, anyhow::Error> {
        let (name, len): (String, usize) =
            bincode::decode_from_slice(data, bincode::config::standard())
                .expect("decode MUST succeed");

        let create_storable = self.registered_types.get(&name).expect("aalkdjhfl");

        Ok(create_storable(&data[len..]))
    }

    fn build_snapshot(&mut self, timestamp: TransactionId) -> &Snapshot {
        let mut snapshot = Snapshot {
            timestamp,
            cached: Default::default(),
        };

        for t in self.transactions_to(timestamp) {
            for write in &t.write_set.writes {
                match &write.operation {
                    WriteOperation::Full(data) => {
                        snapshot
                            .cached
                            .insert(write.id, self.decode(data).expect("decode MUST succeed"));
                    }
                    WriteOperation::Partial(partial) => match snapshot.get_typeless_mut(write.id) {
                        Some(data) => {
                            for op in partial {
                                data.apply_partial_write(op);
                            }
                        }
                        None => {
                            panic!(
                                "somehow found a partial write for id {:?} at timestamp {:?} that doesn't exist in snapshot at timestamp {:?}",
                                write.id, t.id, snapshot.timestamp
                            )
                        }
                    },
                }
            }
        }

        self.snapshots.insert(timestamp, snapshot);
        self.snapshots.get(&timestamp).unwrap()
    }

    fn transactions_to(&self, timestamp: TransactionId) -> impl Iterator<Item = &Transaction> {
        self.log.iter().take_while(move |t| t.id <= timestamp)
    }

    fn transactions_from(&self, timestamp: TransactionId) -> impl Iterator<Item = &Transaction> {
        self.log.iter().skip_while(move |t| t.id < timestamp)
    }

    fn apply_transaction(&mut self, candidate: TransactionCandidate) {
        let timestamp = (self.log.len() as u64).into();

        println!("applying transaction to {:?}: {:?}", timestamp, &candidate);

        self.log.push(Transaction::commit(timestamp, candidate));
        self.build_snapshot(timestamp);

        let b = bincode::encode_to_vec(&self.log, bincode::config::standard())
            .expect("encode MUST succeed");
    }
}

#[derive(Clone)]
pub struct DatabaseAccessor {
    db: Arc<Mutex<Database>>,
}

impl DatabaseAccessor {
    pub fn new(db: Database) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
        }
    }
}

impl DatabaseAccessor {
    // TODO
    pub fn get_all<S: Storable>(&self) -> Vec<S> {
        vec![]
    }

    pub fn transact<F, O>(&self, f: F) -> O
    where
        F: Fn(&mut PendingTransaction) -> O,
    {
        let mut me = self.clone();

        for i in 0..5 {
            println!("Starting transaction attempt {i}");

            let current_timestamp = self.db.lock().unwrap().log.len() as u64;

            let (candidate, result): (TransactionCandidate, O) = {
                let mut pending = PendingTransaction::new(&mut me, current_timestamp.into());
                let result = f(&mut pending);

                (pending.into(), result)
            };

            println!("transaction read set: {:?}", candidate.read_set);

            let mut db = self.db.lock().unwrap();

            if db
                .transactions_from(candidate.timestamp)
                .any(|t| candidate.read_set.overlaps_with(&t.write_set))
            {
                println!(
                    "Read set overlaps with write set: {:#?}",
                    candidate.read_set
                );

                continue;
            }

            db.apply_transaction(candidate);

            return result;
        }

        panic!("failed to apply transaction after 5 attempts");
    }

    fn get<T: Storable + Clone + 'static>(
        &mut self,
        id: Id<T>,
        timestamp: TransactionId,
    ) -> Option<T> {
        let db = &mut self.db.lock().unwrap();

        let snapshot = {
            if db.snapshots.contains_key(&timestamp) {
                db.snapshots.get(&timestamp).unwrap()
            } else {
                db.build_snapshot(timestamp)
            }
        };

        Some(snapshot.get(id).unwrap())
    }
}

#[derive(Debug, bincode::Encode, bincode::Decode)]
struct Transaction {
    id: TransactionId,
    write_set: WriteSet,
}

impl Transaction {
    fn commit(id: TransactionId, candidate: TransactionCandidate) -> Self {
        Self {
            id,
            write_set: candidate.write_set,
        }
    }
}

#[derive(Debug, Default)]
struct Snapshot {
    timestamp: TransactionId,
    cached: HashMap<TypelessId, Box<dyn Storable + Send>>,
}

impl Snapshot {
    fn get<T: Storable + Clone + 'static>(&self, id: Id<T>) -> Option<T> {
        let readable = self.cached.get(&id.into())?;

        if (**readable).type_id() != TypeId::of::<T>() {
            panic!(
                "Type mismatch: expected {:?}, got {:?}",
                TypeId::of::<T>(),
                (**readable).type_id()
            );
            // None
        } else if let Some(data) = (&**readable as &dyn Any).downcast_ref::<T>() {
            Some((*data).clone())
        } else {
            panic!(
                "Type mismatch: expected {:?}, got {:?}",
                TypeId::of::<T>(),
                (**readable).type_id()
            );
        }
    }

    fn get_typeless_mut(
        &mut self,
        id: TypelessId,
    ) -> Option<&mut Box<dyn Storable + Send + 'static>> {
        self.cached.get_mut(&id)
    }
}

#[derive(Debug)]
struct TransactionCandidate {
    timestamp: TransactionId,
    read_set: ReadSet,
    write_set: WriteSet,
}

pub struct PendingTransaction<'a> {
    accessor: &'a mut DatabaseAccessor,
    timestamp: TransactionId,
    read_set: ReadSet,
    write_set: WriteSet,
}

impl<'a> From<PendingTransaction<'a>> for TransactionCandidate {
    fn from(pending: PendingTransaction) -> Self {
        Self {
            timestamp: pending.timestamp,
            read_set: pending.read_set,
            write_set: pending.write_set,
        }
    }
}

impl<'a> PendingTransaction<'a> {
    fn new(accessor: &'a mut DatabaseAccessor, timestamp: TransactionId) -> Self {
        Self {
            accessor,
            timestamp,
            read_set: ReadSet::default(),
            write_set: WriteSet::default(),
        }
    }
    pub fn insert<T: Storable + Send + 'static>(&mut self, id: Id<T>, data: T) {
        // FIXME: decide on a way to generate unqiue ids

        self.write_set.writes.push(Write {
            id: id.into(),
            operation: data.as_full_write_op(),
        });
    }

    pub fn modify<T: Storable + Send + Clone + 'static>(
        &mut self,
        id: Id<T>,
        transform: impl Fn(T) -> T,
    ) {
        println!("attempting modification in timestamp {:?}", self.timestamp);

        let old_data = self.accessor.get(id.clone(), self.timestamp).unwrap();
        let new_data = transform(old_data.clone());

        let operation = old_data.as_partial_write_op(&new_data);

        match &operation {
            WriteOperation::Full(_) => self.read_set.push(DocumentRead::Complete),
            WriteOperation::Partial(partial_writes) => self.read_set.0.extend(
                partial_writes
                    .iter()
                    .map(|w| DocumentRead::Field(w.field_ident.clone())),
            ),
        }

        self.write_set.writes.push(Write {
            id: (&id).into(),
            operation,
        });
    }

    pub fn get<T: Storable + Clone + 'static>(&mut self, id: Id<T>) -> Option<T> {
        self.read_set.push(DocumentRead::Complete);
        self.accessor.get(id, self.timestamp)
    }

    pub fn get_field<T: Storable + for<'facet> Facet<'facet> + Clone + 'static>(
        &mut self,
        id: Id<T>,
        field: &'static str,
    ) -> Option<Value> {
        self.read_set.push(DocumentRead::Field(field.to_string()));
        self.accessor
            .get(id, self.timestamp)
            .and_then(|data| data.field(field))
    }
}

// TODO: make these actual sets?
#[derive(Debug, Default)]
struct ReadSet(Vec<DocumentRead>);

impl ReadSet {
    fn push(&mut self, read: DocumentRead) {
        self.0.push(read);
    }
}

impl ReadSet {
    fn overlaps_with(&self, write_set: &WriteSet) -> bool {
        if !write_set.writes.is_empty()
            && self
                .0
                .iter()
                .any(|read| matches!(read, DocumentRead::Complete))
        {
            return true;
        }

        let writes = write_set
            .writes
            .iter()
            .filter_map(|write| match &write.operation {
                WriteOperation::Full(_) => None,
                WriteOperation::Partial(partial) => Some(partial.iter()),
            })
            .flatten()
            .map(|field| field.field_ident.as_str())
            .collect::<HashSet<_>>();
        let reads = self
            .0
            .iter()
            .filter_map(|read| match read {
                DocumentRead::Complete => None,
                DocumentRead::Field(field) => Some(field.as_ref()),
            })
            .collect::<HashSet<_>>();

        reads.intersection(&writes).count() > 0
    }
}

#[derive(Debug)]
enum DocumentRead {
    Complete,
    Field(String),
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Array(Vec<TypelessId>),
}

impl Value {
    fn size(&self) -> Option<usize> {
        match self {
            Value::String(_) | Value::Array(_) => None,

            Value::Int(_) => Some(size_of::<i64>()),
            Value::Float(_) => Some(size_of::<f64>()),
            Value::Bool(_) => Some(size_of::<bool>()),
        }
    }

    fn as_string(&self) -> Option<&String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    fn as_int(&self) -> Option<&i64> {
        match self {
            Value::Int(i) => Some(i),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<&Vec<TypelessId>> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    fn as_bytes(&self) -> Vec<u8> {
        bincode::encode_to_vec(self, bincode::config::standard()).expect("encode MUST succeed")
    }

    fn from_bytes(data: &[u8]) -> Self {
        let (value, _): (Self, _) = bincode::decode_from_slice(data, bincode::config::standard())
            .expect("decode MUST succeed");

        value
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bincode::{Decode, Encode};
    use facet::Facet;

    use super::*;

    #[derive(Debug, Clone, Encode, Decode, Facet)]
    struct MyTestData {
        name: Name,
        age: i64,
        ff: String,
        //contacts: HashSet<Id<Self>>,
    }

    #[derive(Debug, Clone, Encode, Decode, Facet)]
    struct Name {
        nest: SuperNested,
        first: String,
        last: String,
    }

    #[derive(Debug, Clone, Encode, Decode, Facet)]
    struct SuperNested {
        i_am_a_really_nested_field: i64,
    }

    #[test]
    fn test() {
        let mut db = Database::default();
        db.register_type("MyTestData", |data| {
            let (data, _): (MyTestData, usize) =
                bincode::decode_from_slice(data, bincode::config::standard())
                    .expect("decode MUST succeed");

            Box::new(data)
        });

        let mut accessor = DatabaseAccessor::new(db);

        let data = MyTestData {
            ff: "FIRST".to_string(),
            name: Name {
                first: "John".to_string(),
                last: "Doe".to_string(),
                nest: SuperNested {
                    i_am_a_really_nested_field: 67,
                },
            },
            age: 1337,
            //contacts: vec![Id::new(1)].into_iter().collect(),
        };

        println!("initial state: {:#?}", accessor.db.lock().unwrap());

        accessor.transact(move |t| {
            t.insert(Id::new(1), data.clone());
            t.insert(
                Id::new(2),
                MyTestData {
                    ff: "FIRST".to_string(),
                    name: Name {
                        first: "Oldy".to_string(),
                        last: "McOlderton".to_string(),
                        nest: SuperNested {
                            i_am_a_really_nested_field: 32,
                        },
                    },
                    age: 3000,
                    //contacts: vec![Id::new(1)].into_iter().collect(),
                },
            );
        });

        let cloned_accessor = accessor.clone();
        let handle = std::thread::spawn(move || {
            cloned_accessor.transact(|t| {
                //let Some(Value::String(last_name_of_oldy)) =
                //    t.get_field(Id::<MyTestData>::new(2), "name.last")
                //else {
                //    panic!("expected a string")
                //};
                //
                t.modify(Id::<MyTestData>::new(1), |mut data| {
                    data.age = 9;
                    data.name.nest.i_am_a_really_nested_field = 99999;
                    //data.name.last = format!("Gearbox {}", last_name_of_oldy);
                    data
                });
            });
        });

        let id = Id::<MyTestData>::new(1);
        accessor.transact(|t| {
            t.modify(id.clone(), |mut data| {
                data.ff = "hello".to_string();
                data.age = 8;
                data.name.first = "JingleJeimer".to_string();
                data.name.last = "Not McOlderton Gearbox".to_string();

                data
            });
        });

        let oldys_contacts = accessor.transact(|t| {
            let oldy = t.get(Id::<MyTestData>::new(2)).unwrap();

            vec![0]

            //oldy.contacts
            //    .into_iter()
            //    .filter_map(|id| t.get(id))
            //    .map(|data| format!("{} {}", data.name.first, data.name.last))
            //    .collect::<Vec<_>>()
        });

        handle.join().unwrap();

        let data0 = accessor.get(Id::<MyTestData>::new(1), 0.into());
        let data1 = accessor.get(Id::<MyTestData>::new(1), 1.into());
        let data2 = accessor.get(Id::<MyTestData>::new(1), 2.into());
        let data3 = accessor.get(Id::<MyTestData>::new(1), 3.into());

        println!("data at timestamp 0: {:#?}", data0);
        println!("data at timestamp 1: {:#?}", data1);
        println!("data at timestamp 2: {:#?}", data2);
        println!("data at timestamp 3: {:#?}", data3);

        panic!("oldys contacts: {:#?}", oldys_contacts);
    }
}
