use serde::{Serialize, Deserialize};
use crate::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::{thread, env, time};
use std::time::Instant;

#[derive(Serialize, Deserialize)]
struct Student {
    name: String,
    age: u8,
    state: String,
    gpa: f64,
}

#[test]
fn it_works() {
    let mut students: HashMap<String, Student> = HashMap::new();
    students.insert("student:0".to_owned(), Student {
        name: "Mambisi".to_owned(),
        age: 21,
        state: "CA".to_owned(),
        gpa: 3.1,
    });
    students.insert("student:1".to_owned(), Student {
        name: "Joseph".to_owned(),
        age: 12,
        state: "CA".to_owned(),
        gpa: 3.1,
    });
    students.insert("student:2".to_owned(), Student {
        name: "Elka".to_owned(),
        age: 12,
        state: "FL".to_owned(),
        gpa: 4.0,
    });

    students.insert("student:18".to_owned(), Student {
        name: "Alex".to_owned(),
        age: 15,
        state: "NY".to_owned(),
        gpa: 3.7,
    });

    students.insert("student:18".to_owned(), Student {
        name: "Jackson".to_owned(),
        age: 17,
        state: "NY".to_owned(),
        gpa: 3.8,
    });

    let gpa_order = JsonPathOrder {
        path: "gpa".to_string(),
        ordering: IndexOrd::DESC,
    };

    let name_order = JsonPathOrder {
        path: "name".to_string(),
        ordering: IndexOrd::ASC,
    };

    let state_order = JsonPathOrder {
        path: "state".to_string(),
        ordering: IndexOrd::ASC,
    };

    let indexer = Indexer::Json(IndexJson {
        path_orders: vec![name_order, gpa_order, state_order]
    });

    let mut students_index = Index::new(indexer);




    students_index.batch(|b| {
        &students.iter().for_each(|(k, v)| {
            let json = serde_json::to_value(v).unwrap_or(Value::Null);
            b.insert(k.to_owned(), json);
        });
        b.commit()
    });




    let query = students_index.find_where("state", "eq", Value::String("CA".to_string()));
    println!("Find all students in CA: {:?}", query.read());

    let query = students_index.find_where("gpa", "gt", Value::from(3.5));
   // println!("Find all students whose gpa greater than 3.5: {:?}", query.read());



    let string_indexer = Indexer::String(IndexString {
        ordering: IndexOrd::ASC
    });

    let mut names_index = Index::new(string_indexer);
    names_index.batch(|b| {
        b.insert("user.1".to_owned(), Value::String("Kwadwo".to_string()));
        b.insert("user.9".to_owned(), Value::String("Kwadwo".to_string()));
        b.insert("user.8".to_owned(), Value::String("Kwabena".to_string()));
        b.insert("user.2".to_owned(), Value::String("Kwame".to_string()));
        b.insert("user.3".to_owned(), Value::String("Joseph".to_string()));
        b.insert("user.4".to_owned(), Value::String("Jake".to_string()));
        b.insert("user.5".to_owned(), Value::String("Mambisi".to_string()));
        b.insert("user.6".to_owned(), Value::String("Ama".to_string()));
        b.commit()
    });

    println!("Index Tree: {}", serde_json::to_string_pretty(&names_index).unwrap());

    names_index.remove("user.1");
    let res = names_index.find_where("*", "like", Value::String("k*".to_string()));
    // println!("users whose name starts with K");
    // println!("{:?}", res.read());
}

use std::fs::File;
use std::io::BufReader;

#[test]
fn load_json_from_file() {
    let movie_json_file = env::var("MOVIES_JSON_FILE").unwrap();
    let file = File::open(movie_json_file).unwrap();
    let reader = BufReader::new(file);
    let json: Value = serde_json::from_reader(reader).unwrap();
    let list = json.as_array().unwrap();

    let title_order = JsonPathOrder {
        path: "title".to_string(),
        ordering: IndexOrd::ASC,
    };

    let release_date_order = JsonPathOrder {
        path: "release_date".to_string(),
        ordering: IndexOrd::DESC,
    };

    let indexer = Indexer::Json(IndexJson {
        path_orders: vec![release_date_order, title_order.to_owned()]
    });

    let mut index = Index::new(indexer);

    index.batch(|b| {
        let timer = Instant::now();
        list.iter().for_each(|v| {
            let key = v.dot_get_or("id", Value::String("".to_string())).unwrap();
            b.insert(String::from(key.as_str().unwrap().to_string()), v.clone())
        });
        b.commit();
        let total_time = timer.elapsed().as_secs_f64();
        println!("Indexed list of size: {:?} in {} secs", list.len(), total_time);
    });

    drop(json);




    let timer = Instant::now();

    let order_indexer = Indexer::Json(IndexJson {
        path_orders: vec![title_order.clone()]
    });

    let mut query = index.find_where("title", "like", Value::String("Juman*".to_string()));
    let found = query.count();

    let completion_time = timer.elapsed().as_millis();
    println!("query completed in {} ms, found {} items", completion_time, found);


    println!("Showing Results: release date");
    query.order_by(order_indexer).limit(10).iter(|(k, v)| {
        println!("{}:{}", k, serde_json::to_string_pretty(v).unwrap());
    })
}

#[test]
fn load_json_from_with_incremental_inserts() {
    let title_order = JsonPathOrder {
        path: "title".to_string(),
        ordering: IndexOrd::ASC,
    };

    let release_date_order = JsonPathOrder {
        path: "release_date".to_string(),
        ordering: IndexOrd::DESC,
    };

    let indexer = Indexer::Json(IndexJson {
        path_orders: vec![release_date_order, title_order]
    });

    let index = Arc::new(RwLock::new(Index::new(indexer))).clone();
    let mut handles = vec![];
    {
        let index = Arc::clone(&index);
        let reading = thread::spawn(move || {
            let wait = time::Duration::from_secs_f64(0.5);
            loop {
                thread::sleep(wait);
                let read_guard = index.read().unwrap();
                println!("Items count {:?}", read_guard.count())
            }
        });

        handles.push(reading);
    }

    {
        let index = Arc::clone(&index);

        let writing = thread::spawn(move || {
            let movie_json_file = env::var("MOVIES_JSON_FILE").unwrap();
            let file = File::open(movie_json_file).unwrap();
            let reader = BufReader::new(file);
            let json: Value = serde_json::from_reader(reader).unwrap();
            let list = json.as_array().unwrap();

            let timer = Instant::now();
            list.iter().for_each(|v| {
                let key = v.dot_get_or("id", Value::String("".to_string())).unwrap();
                let mut idx = index.write().unwrap();
                idx.insert(String::from(key.to_string()), v.clone())
            });
            let total_time = timer.elapsed().as_secs_f64();
            println!("Indexed list of size: {:?} in {} secs", list.len(), total_time);
            std::process::exit(1)
        });
        handles.push(writing);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
