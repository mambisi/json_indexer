

extern crate ordered_float;
extern crate indexmap;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate serde_json;
extern crate rayon;

use ordered_float::OrderedFloat;
use indexmap::map::IndexMap;
use serde_json::{Value};
use json_dotpath::DotPaths;
use std::cmp::Ordering;
use rayon::prelude::*;
use serde::{Serialize, Deserialize};
use std::collections::{HashSet, HashMap};
use std::sync::Mutex;
use std::borrow::Borrow;

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

#[derive(Serialize, Deserialize)]
pub struct Index {
    pub indexer: Indexer,
    rs: IndexMap<String, Value>,
    ws: IndexMap<String, Value>,
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
        let mut collection = self.index.write();
        self.inserts.iter().for_each(|(k,v)|{
           collection.insert(k.to_string(),v.clone());
        });
        self.updates.iter().for_each(|(k,v)| {
            if collection.contains_key(k) {
                collection.insert(k.to_string(),v.clone());
            }
        });
        self.deletes.iter().for_each(|k|{
            collection.remove(k);
        });

        self.inserts.clear();
        self.inserts.shrink_to_fit();
        self.updates.clear();
        self.updates.shrink_to_fit();
        self.deletes.clear();
        self.deletes.shrink_to_fit();
        //rebuild index
        self.index.build();
    }
}


impl Index {
    pub fn new(indexer: Indexer, items: &mut IndexMap<String, Value>) -> Self {
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
            ws : collection.clone(),
            rs : collection.clone(),
        };
        drop(collection);
        idx.build();
        idx
    }

    pub fn insert(&mut self, k: String, v: Value) {

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
                    self.write().insert(k,v);
                }
            }
            Indexer::Integer(_) => {
                if v.is_i64() {
                    self.write().insert(k,v);
                }

            }
            Indexer::Float(_) => {
                if v.is_f64() {
                    self.write().insert(k,v);
                }
            }
            Indexer::String(_) => {
                if v.is_string() {
                    self.write().insert(k,v);
                }
            }
        };
        self.build();
    }

    pub fn remove(&mut self, k : &String){
        self.write().remove(k);
    }

    pub fn batch( &mut self, f : fn(&mut Batch) ){
        let mut batch = Batch::new(self);
        f(&mut batch);
    }

    pub fn read(&self) -> &IndexMap<String,Value> {
        &self.rs
    }


    pub fn write(&mut self) -> &mut IndexMap<String,Value> {
        &mut self.ws
    }


    fn build(&mut self) {
        let mut indexer = self.indexer.clone();
        match indexer {

            Indexer::Json(j) => {

                self.write().par_sort_by(|_, lhs, _, rhs| {
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
                self.write().par_sort_by(|_, lhs, _, rhs| {
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
                self.write().par_sort_by(|_, lhs, _, rhs| {
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
                self.write().par_sort_by(|_, lhs, _, rhs| {
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
        self.rs.clone_from(&self.ws)
    }
}

#[cfg(test)]
mod tests;