# Json Index

This crate allows you to create an index (a sorted map) based with serde json values.

## Example

Single index
```rust
    let mut names = HashMap::new();
    names.insert("user.1".to_owned(), Value::String("Kwadwo".to_string()));
    names.insert("user.2".to_owned(), Value::String("Kwame".to_string()));
    names.insert("user.3".to_owned(), Value::String("Joseph".to_string()));
    names.insert("user.4".to_owned(), Value::String("Jake".to_string()));
    names.insert("user.5".to_owned(), Value::String("Mambisi".to_string()));
    names.insert("user.6".to_owned(), Value::String("Ama".to_string()));

    let string_indexer = Indexer::String(IndexString {
        ordering: IndexOrd::ASC
    });

    let mut names_index = Index::new(string_indexer);
    names_index.batch(|b| {
        &names.iter().for_each(|(k, v)| {
            b.insert(k.to_owned(), v.clone());
        });
        b.commit()
    });

/*outputs
{
    "user.6": String("Ama"),
    "user.4": String("Jake"),
    "user.3": String("Joseph"),
    "user.1": String("Kwadwo"),
    "user.2": String("Kwame"),
    "user.5": String("Mambisi")
}
*/
```

Multi index with dot path
```rust
    let mut students: HashMap<String, Student> = HashMap::new();
    students.insert("student:0".to_owned(), Student {
        name: "Mambisi".to_owned(),
        age: 21,
        grade: 3.1,
    });
    students.insert("student:1".to_owned(), Student {
        name: "Joseph".to_owned(),
        age: 12,
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
/* Outputs
{
    "student:4": Object({"age": Number(11), "grade": Number(3.1), "name": String("Bug"), "photo": Object({"id": String("2121"), "url": String("example.com")})}),
    "student:2": Object({"age": Number(12), "grade": Number(4.0), "name": String("Elka")}),
    "student:1": Object({"age": Number(12), "grade": Number(3.1), "name": String("Joseph")}),
    "student:0": Object({"age": Number(21), "grade": Number(3.1), "name": String("Mambisi")})
}
*/
```