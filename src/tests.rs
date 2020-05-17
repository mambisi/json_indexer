use serde::{Serialize, Deserialize};
use crate::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::{thread, env, time};
use env_logger;
use serde_json::Number;

#[derive(Serialize, Deserialize)]
struct Student {
    name: String,
    age: u8,
    state: String,
    grade: f64,
}

#[test]
fn it_works() {
    let mut students: HashMap<String, Student> = HashMap::new();
    students.insert("student:0".to_owned(), Student {
        name: "Mambisi".to_owned(),
        age: 21,
        state : "CA".to_owned(),
        grade: 3.1,
    });
    students.insert("student:1".to_owned(), Student {
        name: "Joseph".to_owned(),
        age: 12,
        state : "CA".to_owned(),
        grade: 3.1,
    });
    students.insert("student:2".to_owned(), Student {
        name: "Elka".to_owned(),
        age: 12,
        state : "FL".to_owned(),
        grade: 4.0,
    });

    let age_order = JsonPathOrder {
        path: "age".to_string(),
        ordering: IndexOrd::DESC,
    };

    let name_order = JsonPathOrder {
        path: "name".to_string(),
        ordering: IndexOrd::ASC,
    };

    let indexer = Indexer::Json(IndexJson {
        path_orders: vec![name_order, age_order]
    });

    let mut index = Index::new(indexer);

    index.batch(|b| {
        &students.iter().for_each(|(k, v)| {
            let json = serde_json::to_value(v).unwrap_or(Value::Null);
            b.insert(k.to_owned(), json);
        });
        b.commit()
    });

    println!("{:?}", index.read());


    let string_indexer = Indexer::String(IndexString {
        ordering: IndexOrd::ASC
    });

    let mut names_index = Index::new(string_indexer);
    names_index.batch(|b| {
        b.insert("user.1".to_owned(), Value::String("Kwadwo".to_string()));
        b.insert("user.8".to_owned(), Value::String("Kwadwo".to_string()));
        b.insert("user.2".to_owned(), Value::String("Kwame".to_string()));
        b.insert("user.3".to_owned(), Value::String("Joseph".to_string()));
        b.insert("user.4".to_owned(), Value::String("Jake".to_string()));
        b.insert("user.5".to_owned(), Value::String("Mambisi".to_string()));
        b.insert("user.6".to_owned(), Value::String("Ama".to_string()));
        b.commit()
    });

    println!("{:?}", names_index.read());
    let res = names_index.select_where("*", QueryOperator::EQ, Value::String("Kwadwo".to_string()));
    println!("Student with name kwadwo {:?}", res.read());

}



/*

#[test]
fn test_shared_state() {
    env::set_var("RUST_LOG", "debug");
    env_logger::init();

    /*
    let name_order = JsonPathOrder {
        path: "name".to_string(),
        ordering: IndexOrd::ASC,
    };
    let indexer = Indexer::Json(IndexJson {
        path_orders: vec![name_order]
    });
    */
    let mut indices = HashMap::new();
    indices.insert("name_index".to_string(), Index::new(Indexer::Integer(IndexInt { ordering: IndexOrd::ASC }), IndexMap::new()));
    let index = Arc::new(RwLock::new(indices));

    let mut handles = vec![];

    {
        let index = Arc::clone(&index);
        let writing = thread::spawn(move || {
            let wait = time::Duration::from_millis(1);
            let mut write_guard = index.write().unwrap();
            let name_index = write_guard.get_mut("name_index").unwrap();
            name_index.batch(|b| {

                for i in 0..20000 {
                    thread::sleep(wait);
                    &b.insert(format!("student:{:?}", i), json!(i));
                }

                b.commit()
            });


        });
        handles.push(writing);
    }
    {
        let index = Arc::clone(&index);
        let writing = thread::spawn(move || {
            let wait = time::Duration::from_millis(1);
            let mut write_guard = index.write().unwrap();
            let name_index = write_guard.get_mut("name_index").unwrap();
            name_index.batch(|b| {

                for i in 21200..40000 {
                    thread::sleep(wait);
                    &b.insert(format!("student:{:?}", i), json!(i));
                }

                b.commit()
            });


        });
        handles.push(writing);
    }

    {
        let index = Arc::clone(&index);

        let reading = thread::spawn(move || {
            let wait = time::Duration::from_secs(30);
            thread::sleep(wait);
            let read_guard = index.read().unwrap();
            let name_index = read_guard.get("name_index").unwrap();
            debug!("{:?}", name_index.read().len())
        });

        handles.push(reading);
    }

    for handle in handles {
        handle.join().unwrap();
    }
    // writing.join().unwrap();
}
*/