use serde::{Serialize, Deserialize};
use crate::*;
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
struct Student {
    name: String,
    age: u8,
    grade: f64,
}
#[test]
fn it_works() {
    let mut students: HashMap<String, Student> = HashMap::new();
    students.insert("student:0".to_owned(), Student {
        name: "Mambisi".to_owned(),
        age: 21,
        grade: 3.1,
    });
    students.insert("student:1".to_owned(), Student {
        name: "Joseph".to_owned(),
        age: 13,
        grade: 3.1,
    });
    students.insert("student:2".to_owned(), Student {
        name: "Elka".to_owned(),
        age: 12,
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
        path_orders: vec![age_order, name_order]
    });

    let mut items: IndexMap<String, Value> = IndexMap::new();

    students.into_iter().for_each(|(k, v)| {
        let json = serde_json::to_value(v).unwrap_or(Value::Null);
        items.insert(k, json);
    });

    let mut index = Index::new(indexer, &mut items);
    index.insert("student:4".to_string(), json!({
        "name": "Bug",
        "age" : 11,
        "grade": 3.1,
        "photo" : {
            "id" : "2121",
            "url" : "example.com"
        }
    }));

    println!("{:?}", index.collection);

    let mut names = IndexMap::new();
    names.insert("user.1".to_owned(), Value::String("Kwadwo".to_string()));
    names.insert("user.2".to_owned(), Value::String("Kwame".to_string()));
    names.insert("user.3".to_owned(), Value::String("Joseph".to_string()));
    names.insert("user.4".to_owned(), Value::String("Jake".to_string()));
    names.insert("user.5".to_owned(), Value::String("Mambisi".to_string()));
    names.insert("user.6".to_owned(), Value::String("Ama".to_string()));

    let string_indexer = Indexer::String(IndexString {
        ordering: IndexOrd::ASC
    });

    let names_index = Index::new(string_indexer, &mut names);
    println!("{:?}", names_index.collection);
}