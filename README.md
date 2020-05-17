# Json Indexer
![Crates.io](https://img.shields.io/crates/v/indexer)

multi value indexer for a json object.

this crate allows to create a sorted map of json objects based on the dot path, its similar to what a database like mongodb
will generate and index based on the path given, this crate is meant to be used in create no sql database. this crate was 
created to be used as indexing structure for ![escanordb](https://github.com/mambisi/escanor).

## Example

Single index

> This example demonstrates how you can use json indexer to index a json value
```rust
    let string_indexer = Indexer::String(IndexString {
        ordering: IndexOrd::ASC
    });

    let mut names_index = Index::new(string_indexer);
    names_index.batch(|b| {
        b.insert("user.1".to_owned(), Value::String("Kwadwo".to_string()));
        b.insert("user.2".to_owned(), Value::String("Kwame".to_string()));
        b.insert("user.3".to_owned(), Value::String("Joseph".to_string()));
        b.insert("user.4".to_owned(), Value::String("Jake".to_string()));
        b.insert("user.5".to_owned(), Value::String("Mambisi".to_string()));
        b.insert("user.6".to_owned(), Value::String("Ama".to_string()));
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
> This example demonstrates how you can use json indexer to index a full json object using multiple dot paths
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