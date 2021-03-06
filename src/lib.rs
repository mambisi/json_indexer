//!# Json Indexer
//!
//!
//! multi value indexer for a json objects.
//!
//! this crate allows to create a sorted map of json objects based on the dot path, its similar to what a database like mongodb
//! will generate and index based on the path given, this crate is meant to be used in create no sql database. this crate was
//! created to be used as indexing structure for [escanordb](https://github.com/mambisi/escanor).



extern crate ordered_float;
extern crate indexmap;
extern crate serde;
extern crate serde_json;
extern crate rayon;
extern crate glob;

use ordered_float::OrderedFloat;
use indexmap::map::IndexMap;
use serde_json::{Value};
use json_dotpath::DotPaths;
use std::cmp::Ordering;
use rayon::prelude::*;
use serde::{Serialize, Deserialize};
use std::collections::{HashSet, HashMap, BTreeMap};
use std::sync::{RwLock, Arc};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::fmt;
use std::error;
use std::ops::Bound::{Included, Unbounded};

#[derive(Serialize, Deserialize, Clone)]
pub enum Indexer {
    Json(IndexJson),
    Integer(IndexInt),
    Float(IndexFloat),
    String(IndexString),
}

pub enum Op {
    EQ,
    LT,
    GT,
    LIKE,
}

#[derive(Debug, Clone)]
pub struct UnknownOperatorError;

impl fmt::Display for UnknownOperatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid operator")
    }
}

impl error::Error for UnknownOperatorError {}

impl FromStr for Op {
    type Err = UnknownOperatorError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let op = s.to_lowercase();
        match op.as_str() {
            "eq" => Ok(Op::EQ),
            "lt" => Ok(Op::LT),
            "gt" => Ok(Op::GT),
            "like" => Ok(Op::LIKE),
            _ => { Err(UnknownOperatorError) }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum IndexOrd {
    ASC,
    DESC,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct IndexInt {
    pub ordering: IndexOrd
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct IndexString {
    pub ordering: IndexOrd
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct IndexFloat {
    pub ordering: IndexOrd
}

#[derive(Serialize, Deserialize, Clone)]
pub struct IndexJson {
    pub path_orders: Vec<JsonPathOrder>
}

#[derive(Serialize, Deserialize, Clone)]
pub struct JsonPathOrder {
    pub path: String,
    pub ordering: IndexOrd,
}

impl JsonPathOrder {
    pub fn new(path: &str, ord: IndexOrd) -> Self {
        JsonPathOrder {
            path: path.to_string(),
            ordering: ord,
        }
    }
}


#[derive(Serialize, Deserialize, Clone, Copy)]
struct FloatKey(f64);

impl Hash for FloatKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        OrderedFloat(self.0).hash(state)
    }
}

impl PartialEq for FloatKey {
    fn eq(&self, other: &Self) -> bool {
        OrderedFloat(self.0).eq(&OrderedFloat(other.0))
    }
}


impl Ord for FloatKey {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.0).cmp(&OrderedFloat(other.0))
    }
}

impl PartialOrd for FloatKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for FloatKey {}

type MultiMap<K1, K2, V> = BTreeMap<K1, HashMap<K2, V>>;

#[derive(Serialize, Deserialize, Clone)]
enum KeyCase {
    UpperCased,
    LowerCased,
    None,
}


#[derive(Serialize, Deserialize, Clone)]
pub struct Index {
    pub indexer: Indexer,
    int_tree: Arc<RwLock<HashMap<String, MultiMap<i64, String, Value>>>>,
    str_tree: Arc<RwLock<HashMap<String, MultiMap<String, String, Value>>>>,
    float_tree: Arc<RwLock<HashMap<String, MultiMap<FloatKey, String, Value>>>>,
    items: Arc<RwLock<IndexMap<String, Value>>>
}

impl Index {
    pub fn from(v: &[u8]) -> Result<Self, ()> {
        match serde_json::from_slice::<Index>(v) {
            Ok(s) => {
                Ok(s)
            }
            Err(_) => {
                Err(())
            }
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let b = serde_json::to_vec(&self).unwrap();
        b
    }

}

pub trait BatchTransaction<'a> {
    fn insert<V>(&mut self, k: &str, v: V) where V: Serialize + Deserialize<'a>;
    fn update<V>(&mut self, k: &str, v: V) where V: Serialize + Deserialize<'a>;
    fn delete(&mut self, k: &str);
    fn commit(&mut self);
}

pub struct Batch<'a> {
    index: &'a mut Index,
    inserts: HashMap<String, Value>,
    updates: HashMap<String, Value>,
    deletes: HashSet<String>,
}

impl<'a> Batch<'a> {
    fn new(idx: &'a mut Index) -> Self {
        Batch {
            index: idx,
            inserts: HashMap::new(),
            updates: HashMap::new(),
            deletes: HashSet::new(),
        }
    }

    fn filter(&'a self, k: &'a String, v: &'a Value) -> Result<(&'a String, &'a Value), ()> {
        let indexer = self.index.indexer.clone();
        match indexer {
            Indexer::Json(j) => {
                let mut found = 0;
                j.path_orders.iter().for_each(|p| {
                    let value = v.dot_get_or(&p.path, Value::Null).unwrap_or(Value::Null);
                    if !value.is_null() {
                        found += 1
                    }
                });
                if found == j.path_orders.len() {
                    Ok((k, v))
                } else {
                    Err(())
                }
            }
            Indexer::Integer(_) => {
                if v.is_i64() {
                    Ok((k, v))
                } else {
                    Err(())
                }
            }
            Indexer::Float(_) => {
                if v.is_f64() {
                    Ok((k, v))
                } else {
                    Err(())
                }
            }
            Indexer::String(_) => {
                if v.is_string() {
                    Ok((k, v))
                } else {
                    Err(())
                }
            }
        }
    }
}

impl<'a> BatchTransaction<'a> for Batch<'a> {
    fn insert<V>(&mut self, k: &str, v: V) where V: Serialize + Deserialize<'a> {
        let v = serde_json::to_value(v).unwrap();
        let k = k.to_string();
        let (k, v) = match self.filter(&k, &v) {
            Ok((k, v)) => {
                (k.to_owned(), v.clone())
            }
            Err(_) => { return; }
        };
        self.inserts.insert(k, v);
    }

    fn update<V>(&mut self, k: &str, v: V) where V: Serialize + Deserialize<'a> {
        let v = serde_json::to_value(v).unwrap();
        let k = k.to_string();
        let (k, v) = match self.filter(&k, &v) {
            Ok((k, v)) => {
                (k.to_owned(), v.clone())
            }
            Err(_) => {
                return;
            }
        };
        self.updates.insert(k, v);
    }

    fn delete(&mut self, k: &str) {
        self.deletes.insert(k.to_string());
    }

    fn commit(&mut self) {
        self.inserts.iter().for_each(|(k, v)| {
            let mut collection = self.index.items.write().unwrap();
            collection.insert(k.to_string(), v.clone());
        });
        self.updates.iter().for_each(|(k, v)| {
            let mut collection = self.index.items.write().unwrap();
            if collection.contains_key(k) {
                collection.insert(k.to_string(), v.clone());
            }
        });
        self.deletes.iter().for_each(|k| {
            let mut collection = self.index.items.write().unwrap();
            collection.remove(k);
        });

        self.inserts.clear();
        self.inserts.shrink_to_fit();
        self.updates.clear();
        self.updates.shrink_to_fit();
        self.deletes.clear();
        self.deletes.shrink_to_fit();
        //rebuild index
        {
            self.index.build();
        }
    }
}

pub struct QueryResult {
    matches: Vec<(String, Value)>,
    indexer: Indexer,
    index: Index,
}

impl<'a> QueryResult {
    pub fn new(matches: Vec<(String, Value)>, indexer: Indexer) -> Self {
        QueryResult {
            matches,
            indexer: indexer.clone(),
            index: Index::new(indexer),
        }
    }
    pub fn and_then(&'a mut self) -> &'a Index {
        //let mut new_index = Index::new(self.indexer.clone());
        for (k, v) in self.matches.iter() {
            self.index.insert(k, v.clone())
        }
        &self.index
    }

    pub fn count(&self) -> usize {
        self.matches.len()
    }

    pub fn order_by(&mut self, indexer: Indexer) -> OrderedResult {
        self.indexer = indexer.clone();
        self.sort();
        OrderedResult {
            matches: &mut self.matches
        }
    }

    pub fn get(&self) -> &Vec<(String, Value)> {
        return &self.matches;
    }


    fn sort(&mut self) {
        let indexer = self.indexer.clone();
        match indexer {
            Indexer::Json(j) => {
                self.matches.par_sort_by(|(_, lhs), (_, rhs)| {
                    let ordering: Vec<Ordering> = j.path_orders.iter().map(|path_order| {
                        let lvalue = lhs.dot_get_or(&path_order.path, Value::Null).unwrap_or(Value::Null);

                        let rvalue = rhs.dot_get_or(&path_order.path, Value::Null).unwrap_or(Value::Null);
                        match (lvalue, rvalue) {
                            (Value::String(ls), Value::String(rs)) => {
                                match path_order.ordering {
                                    IndexOrd::ASC => {
                                        ls.cmp(&rs)
                                    }
                                    IndexOrd::DESC => {
                                        rs.cmp(&ls)
                                    }
                                }
                            }
                            (Value::Number(ls), Value::Number(rs)) => {
                                let ln = ls.as_f64().unwrap_or(0.0);
                                let rn = rs.as_f64().unwrap_or(0.0);

                                match path_order.ordering {
                                    IndexOrd::ASC => {
                                        OrderedFloat(ln).cmp(&OrderedFloat(rn))
                                    }
                                    IndexOrd::DESC => {
                                        OrderedFloat(rn).cmp(&OrderedFloat(ln))
                                    }
                                }
                            }
                            _ => {
                                Ordering::Equal
                            }
                        }
                    }).collect();

                    let mut itr = ordering.iter();
                    let mut order_chain = itr.next().unwrap_or(&Ordering::Equal).to_owned();

                    while let Some(t) = itr.next() {
                        order_chain = order_chain.then(t.to_owned()).to_owned();
                    }
                    order_chain
                });
            }
            Indexer::Integer(i) => {
                self.matches.par_sort_by(|(_, lhs), (_, rhs)| {
                    let lvalue = lhs.as_i64().unwrap_or(0);
                    let rvalue = rhs.as_i64().unwrap_or(0);
                    match i.ordering {
                        IndexOrd::ASC => {
                            lvalue.cmp(&rvalue)
                        }
                        IndexOrd::DESC => {
                            rvalue.cmp(&lvalue)
                        }
                    }
                });
            }
            Indexer::Float(f) => {
                self.matches.par_sort_by(|(_, lhs), (_, rhs)| {
                    let lvalue = lhs.as_f64().unwrap_or(0.0);
                    let rvalue = rhs.as_f64().unwrap_or(0.0);

                    match f.ordering {
                        IndexOrd::ASC => {
                            OrderedFloat(lvalue).cmp(&OrderedFloat(rvalue))
                        }
                        IndexOrd::DESC => {
                            OrderedFloat(rvalue).cmp(&OrderedFloat(lvalue))
                        }
                    }
                });
            }
            Indexer::String(s) => {
                self.matches.par_sort_by(|(_, lhs), (_, rhs)| {
                    let lvalue = lhs.as_str().unwrap_or("");
                    let rvalue = rhs.as_str().unwrap_or("");
                    match s.ordering {
                        IndexOrd::ASC => {
                            lvalue.cmp(&rvalue)
                        }
                        IndexOrd::DESC => {
                            rvalue.cmp(&lvalue)
                        }
                    }
                });
            }
        }
    }
}

pub struct OrderedResult<'a> {
    matches: &'a mut Vec<(String, Value)>,
}

impl<'a> OrderedResult<'a> {
    pub fn get(&self) -> &Vec<(String, Value)> {
        return &self.matches;
    }

    pub fn count(&self) -> usize {
        self.matches.len()
    }

    pub fn limit(&'a mut self, size: usize) -> &mut Self {
        self.matches.truncate(size);
        self
    }
}

impl<'a> Index {
    /// # Creates a new Index
    /// ## Example
    /// ```rust
    /// use indexer::{Indexer, IndexString, IndexOrd};
    /// let string_indexer = Indexer::String(IndexString {
    ///     ordering: IndexOrd::ASC
    /// });
    /// ```
    pub fn new(indexer: Indexer) -> Self {
        let collection: IndexMap<String, Value> = IndexMap::new();
        let mut idx = Index {
            indexer,
            items: Arc::new(RwLock::new(collection.clone())),
            int_tree: Arc::new(RwLock::new(HashMap::new())),
            str_tree: Arc::new(RwLock::new(HashMap::new())),
            float_tree: Arc::new(RwLock::new(HashMap::new())),
        };
        idx.build();
        idx
    }

    /// Inserts a new entry or overrides a previous entry in the index
    pub fn insert<V>(&mut self, key: &str, value: V) where V: Serialize + Deserialize<'a> {
        let k = key.to_string();
        let v = serde_json::to_value(value).unwrap();
        match self.filter(&k, &v) {
            Ok(e) => {
                let mut collection = self.items.write().unwrap();
                let (key, v) = e;
                collection.insert(key.to_string(), v.clone());
                let indexer = self.indexer.clone();
                match indexer {
                    Indexer::Json(j) => {
                        j.path_orders.iter().for_each(|path_order| {
                            let value: Value = v.dot_get_or(&path_order.path, Value::Null).unwrap_or(Value::Null);
                            if value.is_i64() {
                                self.insert_int_index(&path_order.path, &value, key, v)
                            } else if value.is_f64() {
                                self.insert_float_index(&path_order.path, &value, key, v)
                            } else if value.is_string() {
                                self.insert_string_index(&path_order.path, &value, key, v)
                            }
                        });
                    }
                    Indexer::Integer(_) => {
                        let value: Value = v.clone();
                        self.insert_int_index("*", &value, key, v);
                    }
                    Indexer::Float(_) => {
                        let value: Value = v.clone();
                        self.insert_float_index("*", &value, key, v)
                    }
                    Indexer::String(_) => {
                        let value: Value = v.clone();
                        self.insert_string_index("*", &value, key, v)
                    }
                }
            }
            Err(_) => {}
        }
    }

    /// Removes an entry from the index
    pub fn remove(&mut self, k: &str) {
        let mut write_side = self.items.write().unwrap();

        let v: Value = match write_side.swap_remove(k) {
            Some(v) => {
                v
            }
            None => {
                return;
            }
        };
        drop(write_side);

        let indexer = self.indexer.clone();
        match indexer {
            Indexer::Json(j) => {
                j.path_orders.iter().for_each(|path_order| {
                    let value: Value = v.dot_get_or(&path_order.path, Value::Null).unwrap_or(Value::Null);
                    if value.is_i64() {} else if value.is_f64() {} else if value.is_string() {
                        self.remove_string_index(&path_order.path, &value, k)
                    }
                })
            }
            Indexer::Integer(_) => {
                let value: Value = v.clone();
                self.remove_int_index("*", &value, k)
            }
            Indexer::Float(_) => {
                let value: Value = v.clone();
                self.remove_float_index("*", &value, k)
            }
            Indexer::String(_) => {
                let value: Value = v.clone();
                self.remove_string_index("*", &value, k)
            }
        }
        //self.build()
    }

    /// Batch transaction on the index. you can insert/update/delete multiple entries with one operation by commit the operation with ```b.commit()```
    /// Example
    /// ```rust
    /// use indexer::{Index, BatchTransaction};
    /// use serde_json::Value;
    /// let mut names_index = Index::new(string_indexer);
    /// names_index.batch(|b| {
    ///     b.delete("user.4");
    ///     b.insert("user.1", "Kwadwo".to_string());
    ///     b.insert("user.2", Value::String("Kwame".to_string()));
    ///     b.update("user.3", Value::String("Joseph".to_string()));
    ///     b.commit()
    /// });
    /// ```
    pub fn batch(&mut self, f: impl Fn(&mut Batch) + std::marker::Sync + std::marker::Send) {
        let mut batch = Batch::new(self);
        f(&mut batch);
    }

    pub fn get_all_items(&self, f: impl Fn((&String, &Value)) + std::marker::Sync + std::marker::Send) {
        let mut new_index = self.clone();
        new_index.sort();
        let reader = new_index.items.read().unwrap();
        reader.iter().for_each(f);
    }

    pub fn par_get_all_items(&self, f: impl Fn((&String, &Value)) + std::marker::Sync + std::marker::Send) {
        let mut new_index = self.clone();
        new_index.sort();
        let reader = new_index.items.read().unwrap();
        reader.par_iter().for_each(f);
    }

    ///Query on an index. by using conditional operators
    /// - `eq` Equals
    /// - `lt` Less than
    /// - `gt` Greater than
    /// - `like` Check for match using Glob style pattern matching
    ///
    /// ## Example
    ///  ```rust
    ///   let query = students_index.find_where("state", Op::EQ, "CA");
    ///   println!("Find all students in CA: {:?}", query());
    ///  ```
    ///
    pub fn find_where<V>(&self, field: &str, op: Op, value: V) -> QueryResult where V: Serialize + Deserialize<'a> {
        let value = serde_json::to_value(value).unwrap();
        let indexer = self.indexer.clone();
        let matches = match &indexer {
            Indexer::Json(_) => {
                if value.is_i64() {
                    let q = value.as_i64().unwrap();
                    self.query_int_index(field, q, op)
                } else if value.is_f64() {
                    let q = value.as_f64().unwrap();
                    self.query_float_index(field, q, op)
                } else if value.is_string() {
                    let q = String::from(value.as_str().unwrap());
                    self.query_string_index(field, q, op)
                } else {
                    HashMap::new()
                }
            }
            Indexer::Integer(_) => {
                let q = value.as_i64().unwrap();
                self.query_int_index(field, q, op)
            }
            Indexer::Float(_) => {
                let q = value.as_f64().unwrap();
                self.query_float_index(field, q, op)
            }
            Indexer::String(_) => {
                let q = String::from(value.as_str().unwrap());
                self.query_string_index(field, q, op)
            }
        };
        let matches = matches.into_iter().map(|(k, v)| { (k, v) }).collect();
        QueryResult::new(matches, indexer)
    }

    fn query_int_index(&self, key: &str, q: i64, op: Op) -> HashMap<String, Value> {
        let empty_map = MultiMap::new();
        let empty_matches: HashMap<String, Value> = HashMap::new();
        let read_guard = self.int_tree.read().unwrap();
        let int_tree_reader = read_guard.get(key).unwrap_or(&empty_map);
        match op {
            Op::EQ => {
                let m = int_tree_reader.get(&q).unwrap_or(&empty_matches);
                m.clone()
            }
            Op::LT => {
                let mut matches: HashMap<String, Value> = HashMap::new();
                int_tree_reader.range(..q).for_each(|(k, v)| {
                    if k.lt(&q) {
                        matches.extend(v.iter().map(|(k, v)| { (k.to_string(), v.clone()) }));
                    }
                });
                matches
            }
            Op::GT => {
                let mut matches: HashMap<String, Value> = HashMap::new();
                int_tree_reader.range(q..).for_each(|(k, v)| {
                    if k.gt(&q) {
                        matches.extend(v.iter().map(|(k, v)| { (k.to_string(), v.clone()) }));
                    }
                });
                matches
            }
            Op::LIKE => { HashMap::new() }
        }
    }
    fn query_float_index(&self, key: &str, q: f64, op: Op) -> HashMap<String, Value> {
        let empty_map = MultiMap::new();
        let read_guard = self.float_tree.read().unwrap();
        let float_tree_reader = read_guard.get(key).unwrap_or(&empty_map);
        let empty_matches: HashMap<String, Value> = HashMap::new();
        match op {
            Op::EQ => {
                float_tree_reader.get(&FloatKey(OrderedFloat(q).0)).unwrap_or(&empty_matches).clone()
            }
            Op::LT => {
                let mut matches: HashMap<String, Value> = HashMap::new();
                float_tree_reader.range((Unbounded, Included(FloatKey(q)))).for_each(|(k, v)| {
                    if OrderedFloat(k.0).lt(&OrderedFloat(q)) {
                        matches.extend(v.iter().map(|(k, v)| { (k.to_string(), v.clone()) }));
                    }
                });
                matches
            }
            Op::GT => {
                let mut matches: HashMap<String, Value> = HashMap::new();
                float_tree_reader.range((Included(FloatKey(q)), Unbounded)).for_each(|(k, v)| {
                    if OrderedFloat(k.0).gt(&OrderedFloat(q)) {
                        matches.extend(v.iter().map(|(k, v)| { (k.to_string(), v.clone()) }));
                    }
                });
                matches
            }
            Op::LIKE => { HashMap::new() }
        }
    }
    fn query_string_index(&self, key: &str, q: String, op: Op) -> HashMap<String, Value> {
        let empty_map = MultiMap::new();
        let empty_matches: HashMap<String, Value> = HashMap::new();
        let read_guard = self.str_tree.read().unwrap();
        let str_tree_reader = read_guard.get(key).unwrap_or(&empty_map);
        match op {
            Op::EQ => {
                str_tree_reader.get(&q).unwrap_or(&empty_matches).clone()
            }
            Op::LT => {
                let mut matches = HashMap::new();
                str_tree_reader.range(..q.to_owned()).for_each(|(k, v)| {
                    if k.lt(&q) {
                        matches.extend(v.iter().map(|(k, v)| { (k.to_string(), v.clone()) }));
                    }
                });
                matches
            }
            Op::GT => {
                let mut matches = HashMap::new();
                str_tree_reader.range(q.to_owned()..).for_each(|(k, v)| {
                    if k.gt(&q) {
                        matches.extend(v.iter().map(|(k, v)| { (k.to_string(), v.clone()) }));
                    }
                });
                matches
            }
            Op::LIKE => {

                let options = glob::MatchOptions {
                    case_sensitive: true,
                    require_literal_separator: false,
                    require_literal_leading_dot: false,
                };
                let glob_matcher = match glob::Pattern::new(&q) {
                    Ok(m) => { m }
                    Err(_) => {
                        return HashMap::new();
                    }
                };

                let mut prefix = String::new();
                for c in q.chars() {
                    match c {
                        '*' | '?' | '[' => {
                            break;
                        }
                        c => {
                            prefix.push(c);
                        }
                    }
                }

                let (lb, fchar) = if prefix.is_empty() {
                    (Unbounded, None)
                }else {
                    (Included(prefix.to_string()), prefix.chars().nth(0))
                };

                if lb == Unbounded {
                    let mut matches = HashMap::new();
                    str_tree_reader.iter().for_each(|(k,v)|{
                        if glob_matcher.matches_with(k, options) {
                            matches.extend(v.iter().map(|(k, v)| { (k.to_string(), v.clone()) }));
                        }
                    });
                    matches
                }
                else {
                    let mut matches = HashMap::new();
                    for (k,v) in str_tree_reader.range((lb,Unbounded)) {
                        let c2 = k.chars().nth(0);
                        match (fchar,c2)  {
                            (Some(c),Some(c2)) => {
                                let c1 = c as u32;
                                let c2 = c2 as u32;
                                if c2 > c1 {
                                    break;
                                }
                            }
                            _ => {}
                        }

                        if glob_matcher.matches_with(k, options) {
                            matches.extend(v.iter().map(|(k, v)| { (k.to_string(), v.clone()) }));
                        }
                    }
                    matches
                }

            }
        }
    }

    fn insert_int_index(&self, field: &str, iv: &Value, k: &str, v: &Value) {
        let mut int_tree_writer = self.int_tree.write().unwrap();

        let key = iv.as_i64().unwrap();

        match int_tree_writer.get_mut(field) {
            None => {
                let mut m = MultiMap::new();
                match m.get_mut(&key) {
                    None => {
                        let mut new_map = HashMap::new();
                        new_map.insert(k.to_string(), v.clone());
                        m.insert(key, new_map);
                        int_tree_writer.insert(field.to_string(), m);
                    }
                    Some(b) => {
                        b.insert(k.to_string(), v.clone());
                    }
                }
            }
            Some(m) => {
                match m.get_mut(&key) {
                    None => {
                        let mut new_map = HashMap::new();
                        new_map.insert(k.to_string(), v.clone());
                        m.insert(key, new_map);
                    }
                    Some(b) => {
                        b.insert(k.to_string(), v.clone());
                    }
                }
            }
        };
    }
    fn insert_float_index(&self, field: &str, iv: &Value, k: &str, v: &Value) {
        let mut float_tree_writer = self.float_tree.write().unwrap();
        let key = iv.as_f64().unwrap();
        match float_tree_writer.get_mut(field) {
            None => {
                let mut m = MultiMap::new();
                match m.get_mut(&FloatKey(key)) {
                    None => {
                        let mut new_map = HashMap::new();
                        new_map.insert(k.to_string(), v.clone());
                        m.insert(FloatKey(key), new_map);
                        float_tree_writer.insert(field.to_string(), m);
                    }
                    Some(b) => {
                        b.insert(k.to_string(), v.clone());
                    }
                };
            }
            Some(m) => {
                match m.get_mut(&FloatKey(key)) {
                    None => {
                        let mut new_map = HashMap::new();
                        new_map.insert(k.to_string(), v.clone());
                        m.insert(FloatKey(key), new_map);
                    }
                    Some(b) => {
                        b.insert(k.to_string(), v.clone());
                    }
                }
            }
        };
    }
    fn insert_string_index(&self, field: &str, iv: &Value, k: &str, v: &Value) {
        let mut str_tree_writer = self.str_tree.write().unwrap();
        let key = String::from(iv.as_str().unwrap());
        match str_tree_writer.get_mut(field) {
            None => {
                let mut m = MultiMap::new();
                match m.get_mut(&key) {
                    None => {
                        let mut new_map = HashMap::new();
                        new_map.insert(k.to_string(), v.clone());
                        m.insert(key, new_map);
                        str_tree_writer.insert(field.to_string(), m);
                    }
                    Some(b) => {
                        b.insert(k.to_string(), v.clone());
                    }
                }
            }
            Some(m) => {
                match m.get_mut(&key) {
                    None => {
                        let mut new_map = HashMap::new();
                        new_map.insert(k.to_string(), v.clone());
                        m.insert(key, new_map);
                    }
                    Some(b) => {
                        b.insert(k.to_string(), v.clone());
                    }
                }
            }
        };
    }


    fn remove_int_index(&self, field: &str, iv: &Value, k: &str) {
        let mut int_tree_writer = self.int_tree.write().unwrap();
        let key = iv.as_i64().unwrap();
        let mut empty_map = HashMap::new();
        match int_tree_writer.get_mut(field) {
            None => {}
            Some(m) => {
                let items = m.get_mut(&key).unwrap_or(&mut empty_map);
                items.remove(k);
                if items.is_empty() {
                    m.remove(&key);
                }
            }
        }
    }
    fn remove_float_index(&self, field: &str, iv: &Value, k: &str) {
        let mut float_tree_writer = self.float_tree.write().unwrap();
        let key = iv.as_f64().unwrap();
        let mut empty_map = HashMap::new();
        match float_tree_writer.get_mut(field) {
            None => {}
            Some(m) => {
                let items = m.get_mut(&FloatKey(key)).unwrap_or(&mut empty_map);
                items.remove(k);
                if items.is_empty() {
                    m.remove(&FloatKey(key));
                }
            }
        }
    }
    fn remove_string_index(&self, field: &str, iv: &Value, k: &str) {
        let mut str_tree_writer = self.str_tree.write().unwrap();
        let key = iv.as_str().unwrap();

        let mut empty_map = HashMap::new();
        match str_tree_writer.get_mut(field) {
            None => {}
            Some(m) => {
                let items = m.get_mut(key).unwrap_or(&mut empty_map);
                items.remove(k);
                if items.is_empty() {
                    m.remove(key);
                }
            }
        }
    }

    fn filter(&mut self, k: &'a String, v: &'a Value) -> Result<(&'a String, &'a Value), ()> {
        match &self.indexer {
            Indexer::Json(j) => {
                let mut found = 0;
                j.path_orders.iter().for_each(|p| {
                    let value = v.dot_get_or(&p.path, Value::Null).unwrap_or(Value::Null);
                    if !value.is_null() {
                        found += 1
                    }
                });
                if found == j.path_orders.len() {
                    Ok((k, v))
                } else {
                    Err(())
                }
            }
            Indexer::Integer(_) => {
                if v.is_i64() {
                    Ok((k, v))
                } else {
                    Err(())
                }
            }
            Indexer::Float(_) => {
                if v.is_f64() {
                    Ok((k, v))
                } else {
                    Err(())
                }
            }
            Indexer::String(_) => {
                if v.is_string() {
                    Ok((k, v))
                } else {
                    Err(())
                }
            }
        }
    }

    #[deprecated(since = "0.2.5", note = "Please use the size() instead")]
    pub fn count(&self) -> usize {
        let reader = self.items.read().unwrap();
        reader.len()
    }

    pub fn size(&self) -> usize {
        let reader = self.items.read().unwrap();
        reader.len()
    }

    fn sort(&mut self) {
        //let reader = self.ws.read().unwrap();
        //self.rs.clone_from(reader.deref()
        let indexer = self.indexer.clone();

        match indexer {
            Indexer::Json(j) => {
                self.items.write().unwrap().par_sort_by(|_, lhs, _, rhs| {
                    let ordering: Vec<Ordering> = j.path_orders.iter().map(|path_order| {
                        let lvalue = lhs.dot_get_or(&path_order.path, Value::Null).unwrap_or(Value::Null);

                        let rvalue = rhs.dot_get_or(&path_order.path, Value::Null).unwrap_or(Value::Null);
                        match (lvalue, rvalue) {
                            (Value::String(ls), Value::String(rs)) => {
                                match path_order.ordering {
                                    IndexOrd::ASC => {
                                        ls.cmp(&rs)
                                    }
                                    IndexOrd::DESC => {
                                        rs.cmp(&ls)
                                    }
                                }
                            }
                            (Value::Number(ls), Value::Number(rs)) => {
                                let ln = ls.as_f64().unwrap_or(0.0);
                                let rn = rs.as_f64().unwrap_or(0.0);

                                match path_order.ordering {
                                    IndexOrd::ASC => {
                                        OrderedFloat(ln).cmp(&OrderedFloat(rn))
                                    }
                                    IndexOrd::DESC => {
                                        OrderedFloat(rn).cmp(&OrderedFloat(ln))
                                    }
                                }
                            }
                            _ => {
                                Ordering::Equal
                            }
                        }
                    }).collect();

                    let mut itr = ordering.iter();
                    let mut order_chain = itr.next().unwrap_or(&Ordering::Equal).to_owned();

                    while let Some(t) = itr.next() {
                        order_chain = order_chain.then(t.to_owned()).to_owned();
                    }
                    order_chain
                });
            }
            Indexer::Integer(i) => {
                self.items.write().unwrap().par_sort_by(|_, lhs, _, rhs| {
                    let lvalue = lhs.as_i64().unwrap_or(0);
                    let rvalue = rhs.as_i64().unwrap_or(0);
                    match i.ordering {
                        IndexOrd::ASC => {
                            lvalue.cmp(&rvalue)
                        }
                        IndexOrd::DESC => {
                            rvalue.cmp(&lvalue)
                        }
                    }
                });
            }
            Indexer::Float(f) => {
                self.items.write().unwrap().par_sort_by(|_, lhs, _, rhs| {
                    let lvalue = lhs.as_f64().unwrap_or(0.0);
                    let rvalue = rhs.as_f64().unwrap_or(0.0);

                    match f.ordering {
                        IndexOrd::ASC => {
                            OrderedFloat(lvalue).cmp(&OrderedFloat(rvalue))
                        }
                        IndexOrd::DESC => {
                            OrderedFloat(rvalue).cmp(&OrderedFloat(lvalue))
                        }
                    }
                });
            }
            Indexer::String(s) => {
                self.items.write().unwrap().par_sort_by(|_, lhs, _, rhs| {
                    let lvalue = lhs.as_str().unwrap_or("");
                    let rvalue = rhs.as_str().unwrap_or("");
                    match s.ordering {
                        IndexOrd::ASC => {
                            lvalue.cmp(&rvalue)
                        }
                        IndexOrd::DESC => {
                            rvalue.cmp(&lvalue)
                        }
                    }
                });
            }
        }
    }
    fn build(&mut self) {
        //let reader = self.ws.read().unwrap();
        //self.rs.clone_from(reader.deref()

        let reader = self.items.read().unwrap();

        {
            let mut int_tree_writer = self.int_tree.write().unwrap();
            let mut float_tree_writer = self.float_tree.write().unwrap();
            let mut str_tree_writer = self.str_tree.write().unwrap();
            int_tree_writer.clear();
            float_tree_writer.clear();
            str_tree_writer.clear();
        }


        reader.par_iter().for_each(|(k, v)| {
            let indexer = self.indexer.clone();
            match indexer {
                Indexer::Json(j) => {
                    j.path_orders.iter().for_each(|path_order| {
                        let value: Value = v.dot_get_or(&path_order.path, Value::Null).unwrap_or(Value::Null);
                        if value.is_i64() {
                            self.insert_int_index(&path_order.path, &value, k, v)
                        } else if value.is_f64() {
                            self.insert_float_index(&path_order.path, &value, k, v)
                        } else if value.is_string() {
                            self.insert_string_index(&path_order.path, &value, k, v)
                        }
                    })
                }
                Indexer::Integer(_) => {
                    let value: Value = v.clone();
                    self.insert_int_index("*", &value, k, v)
                }
                Indexer::Float(_) => {
                    let value: Value = v.clone();
                    self.insert_float_index("*", &value, k, v)
                }
                Indexer::String(_) => {
                    let value: Value = v.clone();
                    self.insert_string_index("*", &value, k, v)
                }
            }
        });
    }

    pub fn get_items(&self) -> Vec<(String, Value)> {
        let mut new_index = self.clone();
        new_index.sort();
        let reader = new_index.items.read().unwrap();
        let items = reader.par_iter().map(|(k, v)| { (k.to_string(), v.clone()) }).collect();
        items
    }
}


#[cfg(test)]
mod tests;