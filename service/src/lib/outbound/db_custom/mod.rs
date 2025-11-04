use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::{Arc, Mutex},
};

#[derive(Default, Copy, Clone, PartialOrd, PartialEq, Eq, Hash, Debug)]
struct TransactionId(u64);

impl From<u64> for TransactionId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Debug, Copy, Clone)]
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

#[derive(Default, Copy, Clone, PartialEq, Eq, Hash, Debug)]
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
}

impl Database {
    fn build_snapshot(&mut self, timestamp: TransactionId) -> &Snapshot {
        let mut snapshot = Snapshot {
            timestamp,
            cached: Default::default(),
        };

        for t in self.transactions_to(timestamp) {
            for write in &t.write_set.0 {
                match &write.operation {
                    DocumentWriteOperation::New(data) => {
                        snapshot.cached.insert(write.id, data.cloned());
                    }
                    DocumentWriteOperation::Modified(fields) => {
                        match snapshot.get_typeless_mut(write.id) {
                            Some(data) => {
                                for field in fields {
                                    data.write_modification(field);
                                }
                            }
                            None => {
                                panic!(
                                    "somehow found a modified write for id {:?} at timestamp {:?} that doesn't exist in snapshot at timestamp {:?}",
                                    write.id, t.id, snapshot.timestamp
                                )
                            }
                        }
                    }
                }
            }
        }

        self.snapshots.insert(timestamp, snapshot);
        self.snapshots.get(&timestamp).unwrap()
    }

    fn transactions_to(&mut self, timestamp: TransactionId) -> impl Iterator<Item = &Transaction> {
        self.log.iter().take_while(move |t| t.id <= timestamp)
    }

    fn transactions_from(
        &mut self,
        timestamp: TransactionId,
    ) -> impl Iterator<Item = &Transaction> {
        self.log.iter().skip_while(move |t| t.id < timestamp)
    }

    fn apply_transaction(&mut self, candidate: TransactionCandidate) {
        let timestamp = (self.log.len() as u64).into();

        println!("applying transaction to {:?}: {:?}", timestamp, &candidate);

        self.log.push(Transaction::commit(timestamp, candidate));
        self.build_snapshot(timestamp);
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

#[derive(Debug)]
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

        self.write_set.push(DocumentWrite::new(id.into(), data));
    }

    pub fn modify<T: Storable + Send + Modifiable + Clone + 'static>(
        &mut self,
        id: Id<T>,
        transform: impl Fn(T) -> T,
    ) {
        println!("attempting modification in timestamp {:?}", self.timestamp);

        let old_data = self.accessor.get(id.clone(), self.timestamp).unwrap();
        let new_data = transform(old_data.clone());

        let modifications = old_data.modifications_between(&new_data);

        for modification in &modifications {
            self.read_set.push(DocumentRead::Field(modification.field));
        }

        self.write_set
            .push(DocumentWrite::modification(id, modifications));
    }

    pub fn get<T: Storable + Clone + 'static>(&mut self, id: Id<T>) -> Option<T> {
        self.read_set.push(DocumentRead::Complete);
        self.accessor.get(id, self.timestamp)
    }

    pub fn get_field<T: Storable + Clone + 'static>(
        &mut self,
        id: Id<T>,
        field: &'static str,
    ) -> Option<Value> {
        self.read_set.push(DocumentRead::Field(field));
        self.accessor
            .get(id, self.timestamp)
            .and_then(|data| data.field(field))
    }
}

// TODO: make these actual sets?
#[derive(Debug, Default)]
struct ReadSet(Vec<DocumentRead>);
#[derive(Debug, Default)]
struct WriteSet(Vec<DocumentWrite>);

impl ReadSet {
    fn push(&mut self, read: DocumentRead) {
        self.0.push(read);
    }
}

impl WriteSet {
    fn push(&mut self, write: DocumentWrite) {
        self.0.push(write);
    }
}

impl ReadSet {
    fn overlaps_with(&self, write_set: &WriteSet) -> bool {
        if !write_set.0.is_empty()
            && self
                .0
                .iter()
                .any(|read| matches!(read, DocumentRead::Complete))
        {
            return true;
        }

        let writes = write_set
            .0
            .iter()
            .filter_map(|write| match &write.operation {
                DocumentWriteOperation::New(_) => None,
                DocumentWriteOperation::Modified(fields) => Some(fields.iter()),
            })
            .flatten()
            .map(|field| field.field)
            .collect::<HashSet<_>>();
        let reads = self
            .0
            .iter()
            .filter_map(|read| match read {
                DocumentRead::Complete => None,
                DocumentRead::Field(field) => Some(*field),
            })
            .collect::<HashSet<_>>();

        reads.intersection(&writes).count() > 0
    }
}

pub trait Storable: Any + std::fmt::Debug {
    fn cloned(&self) -> Box<dyn Storable + Send>;
    fn write_modification(&mut self, field: &DocumentField);
    fn field(&self, field: &'static str) -> Option<Value>;
}
pub trait Modifiable {
    fn modifications_between(&self, other: &Self) -> Vec<DocumentField>;
}

#[derive(Debug)]
enum DocumentRead {
    Complete,
    Field(&'static str),
}

#[derive(Debug)]
struct DocumentWrite {
    id: TypelessId,
    type_id: TypeId,
    operation: DocumentWriteOperation,
}

impl DocumentWrite {
    fn new<T: Storable + Send + 'static>(id: TypelessId, data: T) -> Self {
        Self {
            id,
            type_id: TypeId::of::<T>(),
            operation: DocumentWriteOperation::New(Box::new(data)),
        }
    }

    fn modification<T: 'static>(id: Id<T>, operation: Vec<DocumentField>) -> Self {
        Self {
            id: id.into(),
            type_id: TypeId::of::<T>(),
            operation: DocumentWriteOperation::Modified(operation),
        }
    }
}

#[derive(Debug)]
pub enum DocumentWriteOperation {
    New(Box<dyn Storable + Send>),
    Modified(Vec<DocumentField>),
}

#[derive(Debug, Clone)]
pub struct DocumentField {
    field: &'static str,
    value: Value,
}

impl DocumentField {
    fn of(field: &'static str, value: Value) -> Self {
        Self { field, value }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Array(Vec<TypelessId>),
}

impl Value {
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[derive(Debug, Clone)]
    struct MyTestData {
        name: Name,
        age: i64,
        contacts: HashSet<Id<Self>>,
    }

    #[derive(Debug, Clone)]
    struct Name {
        first: String,
        last: String,
    }

    // TODO: make this a derive macro
    impl Storable for MyTestData {
        fn cloned(&self) -> Box<dyn Storable + Send> {
            Box::new(self.clone())
        }
        fn write_modification(&mut self, field: &DocumentField) {
            match field.field {
                "name.first" => self.name.first = field.value.as_string().unwrap().to_string(),
                "name.last" => self.name.last = field.value.as_string().unwrap().to_string(),
                "age" => self.age = *field.value.as_int().unwrap(),
                "contacts" => {
                    self.contacts = field
                        .value
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|id| Id::<Self>::new(id.0))
                        .collect();
                }
                _ => panic!("unknown field"),
            }
        }
        fn field(&self, field: &'static str) -> Option<Value> {
            match field {
                "name.first" => Some(Value::String(self.name.first.clone())),
                "name.last" => Some(Value::String(self.name.last.clone())),
                "age" => Some(Value::Int(self.age)),
                "contacts" => Some(Value::Array(
                    self.contacts.iter().map(|id| id.into()).collect(),
                )),
                _ => None,
            }
        }
    }

    // TODO: make this a derive macro
    impl Modifiable for MyTestData {
        fn modifications_between(&self, other: &Self) -> Vec<DocumentField> {
            let mut modifications = vec![];

            if self.name.first != other.name.first {
                modifications.push(DocumentField::of(
                    "name.first",
                    Value::String(other.name.first.clone()),
                ));
            }

            if self.name.last != other.name.last {
                modifications.push(DocumentField::of(
                    "name.last",
                    Value::String(other.name.last.clone()),
                ));
            }

            if self.age != other.age {
                modifications.push(DocumentField::of("age", Value::Int(other.age)));
            }

            let contacts_diff = self.contacts.difference(&other.contacts);
            if contacts_diff.count() > 0 {
                modifications.push(DocumentField::of(
                    "contacts",
                    Value::Array(other.contacts.iter().map(|id| id.into()).collect()),
                ));
            }

            modifications
        }
    }

    #[test]
    fn test() {
        let db = Database::default();
        let mut accessor = DatabaseAccessor::new(db);

        let data = MyTestData {
            name: Name {
                first: "John".to_string(),
                last: "Doe".to_string(),
            },
            age: 42,
            contacts: HashSet::new(),
        };

        println!("initial state: {:#?}", accessor.db.lock().unwrap());

        accessor.transact(move |t| {
            t.insert(Id::new(1), data.clone());
            t.insert(
                Id::new(2),
                MyTestData {
                    name: Name {
                        first: "Oldy".to_string(),
                        last: "McOlderton".to_string(),
                    },
                    age: 69,
                    contacts: vec![Id::new(1)].into_iter().collect(),
                },
            );
        });

        let mut cloned_accessor = accessor.clone();
        let handle = std::thread::spawn(move || {
            cloned_accessor.transact(|t| {
                let Some(Value::String(last_name_of_oldy)) =
                    t.get_field(Id::<MyTestData>::new(2), "name.last")
                else {
                    panic!("expected a string")
                };

                t.modify(Id::<MyTestData>::new(1), |mut data| {
                    data.name.last = format!("Gearbox {}", last_name_of_oldy);
                    data
                });
            });
        });

        let id = Id::<MyTestData>::new(1);
        accessor.transact(|t| {
            t.modify(id.clone(), |mut data| {
                data.name.last = "Not McOlderton Gearbox".to_string();

                data
            });
        });

        let oldys_contacts = accessor.transact(|t| {
            let oldy = t.get(Id::<MyTestData>::new(2)).unwrap();

            oldy.contacts
                .into_iter()
                .filter_map(|id| t.get(id))
                .map(|data| format!("{} {}", data.name.first, data.name.last))
                .collect::<Vec<_>>()
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
