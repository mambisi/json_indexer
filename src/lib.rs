

extern crate ordered_float;
extern crate indexmap;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate serde_json;
extern crate rayon;
#[macro_use]
extern crate log;

use ordered_float::OrderedFloat;
use indexmap::map::IndexMap;
use serde_json::{Value};
use json_dotpath::DotPaths;
use std::cmp::Ordering;
use rayon::prelude::*;
use serde::{Serialize, Deserialize};
use std::collections::{HashSet, HashMap};
use std::sync::{Mutex, RwLock, Arc};
use std::borrow::Borrow;
use std::ops::{DerefMut, Deref};

#[derive(Serialize, Deserialize,Clone)]
pub enum Indexer {
    Json(IndexJson),
    Integer(IndexInt),
    Float(IndexFloat),
    String(IndexString),
}
#[derive(Serialize, Deserialize, Clone)]
pub enum IndexOrd {
    ASC,
    DESC,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct IndexInt {
    pub ordering: IndexOrd
}
#[derive(Serialize, Deserialize, Clone)]
pub struct IndexString {
    pub ordering: IndexOrd
}
#[derive(Serialize, Deserialize, Clone)]
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
#[derive(Serialize, Deserialize, Clone)]
pub struct Index {
    pub indexer: Indexer,
    rs: IndexMap<String,Value>,
    ws: Arc<RwLock<IndexMap<String, Value>>>,
}

trait BatchTransaction {
    fn insert(&mut self, k: String, v: Value);
    fn update(&mut self, k: String, v: Value);
    fn delete(&mut self, k: String);
    fn commit(&mut self);
}

pub struct Batch<'a> {
    index : &'a mut Index,
    inserts : HashMap<String, Value>,
    updates : HashMap<String, Value>,
    deletes : HashSet<String>
}

impl<'a > Batch<'a> {
    fn new( idx : &'a mut Index) -> Self {
        Batch {
            index: idx,
            inserts: HashMap::new(),
            updates: HashMap::new(),
            deletes: HashSet::new()
        }
    }
}

impl<'a> BatchTransaction for Batch<'a> {
    fn insert(&mut self, k: String, v: Value) {
        self.inserts.insert(k,v);
    }

    fn update(&mut self, k: String, v: Value) {
        self.updates.insert(k,v);
    }

    fn delete(&mut self, k: String) {
        self.deletes.insert(k);
    }

    fn commit(&mut self) {

        self.inserts.iter().for_each(|(k,v)|{
            let mut collection = self.index.ws.write().unwrap();
           collection.insert(k.to_string(),v.clone());
        });
        self.updates.iter().for_each(|(k,v)| {
            let mut collection = self.index.ws.write().unwrap();
            if collection.contains_key(k) {
                collection.insert(k.to_string(),v.clone());
            }
        });
        self.deletes.iter().for_each(|k|{
            let mut collection = self.index.ws.write().unwrap();
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


impl Index {
    pub fn new(indexer: Indexer, items: IndexMap<String, Value>) -> Self {
        let mut items = &mut items.clone();
        let filtered: IndexMap<&String, &Value> = match &indexer {
            Indexer::Json(j) => {
                items.iter().filter(|(_, v)| {
                    let mut found = 0;
                    j.path_orders.iter().for_each(|p| {
                        let value = v.dot_get_or(&p.path, Value::Null).unwrap_or(Value::Null);
                        if !value.is_null() {
                            found += 1
                        }
                    });
                    found == j.path_orders.len()
                }).collect()
            }
            Indexer::Integer(_) => {
                items.iter().filter(|(_, v)| {
                    v.is_i64()
                }).collect()
            }
            Indexer::Float(_) => {
                items.iter().filter(|(_, v)| {
                    v.is_f64()

                }).collect()
            }
            Indexer::String(_) => {
                items.iter().filter(|(_, v)| {
                    v.is_string()
                }).collect()
            }
        };
        let mut collection : IndexMap<String,Value> = IndexMap::new();

        filtered.iter().for_each(|(k,v)| {
            &collection.insert(k.to_string(), v.clone().clone());
        });

        let mut idx = Index {
            indexer,
            ws : Arc::new(RwLock::new(collection.clone())),
            rs : collection.clone(),
        };
        drop(collection);
        idx.build();
        idx
    }

    pub fn insert(&mut self, k: String, v: Value) {

        match &self.indexer {
            Indexer::Json(j) => {
                let mut collection = self.ws.write().unwrap();
                let mut found = 0;
                j.path_orders.iter().for_each(|p| {
                    let value = v.dot_get_or(&p.path, Value::Null).unwrap_or(Value::Null);
                    if !value.is_null() {
                        found += 1
                    }
                });
                if found == j.path_orders.len() {

                    collection.insert(k,v);
                }
            }
            Indexer::Integer(_) => {
                let mut collection = self.ws.write().unwrap();
                if v.is_i64() {
                    collection.insert(k,v);
                }

            }
            Indexer::Float(_) => {
                let mut collection = self.ws.write().unwrap();
                if v.is_f64() {
                    collection.insert(k,v);
                }
            }
            Indexer::String(_) => {
                let mut collection = self.ws.write().unwrap();
                if v.is_string() {
                    collection.insert(k,v);
                }
            }
        };
        {
            self.build();
        }

    }

    pub fn remove(&mut self, k : &String){
        let mut  write_side = self.ws.write().unwrap();
        write_side.remove(k);
    }

    pub fn batch( &mut self, f : impl Fn(&mut Batch) + std::marker::Sync + std::marker::Send){
        let mut batch = Batch::new(self);
        f(&mut batch);
    }

    pub fn iter(&self, f : impl Fn((&String,&Value)) + std::marker::Sync + std::marker::Send){
        self.rs.iter().for_each(f);
    }

    pub fn par_iter(&self, f : impl Fn((&String,&Value)) + std::marker::Sync + std::marker::Send){
        self.rs.par_iter().for_each(f);
    }

    pub fn read(&self) -> &IndexMap<String,Value> {
        &self.rs
    }


    fn build(&mut self) {
        let mut indexer = self.indexer.clone();
        match indexer {

            Indexer::Json(j) => {

                self.ws.write().unwrap().par_sort_by(|_, lhs, _, rhs| {
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
                self.ws.write().unwrap().par_sort_by(|_, lhs, _, rhs| {
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
                self.ws.write().unwrap().par_sort_by(|_, lhs, _, rhs| {
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
                self.ws.write().unwrap().par_sort_by(|_, lhs, _, rhs| {
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
        self.rs.clone_from(self.ws.read().unwrap().deref())
    }
}

#[cfg(test)]
mod tests;