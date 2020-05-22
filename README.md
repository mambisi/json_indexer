# Json Indexer
[![Crates.io](https://img.shields.io/crates/v/indexer)](https://crates.io/crates/indexer)

multi value indexer for a json object.

this crate allows to create a sorted map of json objects based on the dot path, its similar to what a database like mongodb
will generate and index based on the path given, this crate is meant to be used in creating no sql databases. This library was 
created to be used as indexing structure for [escanordb](https://github.com/mambisi/escanor).

## Road to 0.2 :  TODO
- [ ] Basic Query support
    - [X] Operators  `eq`  `lt` `gt` 
    - [ ] limit query output
    - [X] like (MS SQL LIKE `*ja*`)
- [X] Compound queries
- [ ] Order by

## Version 0.1.5
Features
- Basic queries check below for documentation

## Example

Single index

> This example demonstrates how you can use json indexer to index a json value
```rust
    use indexer::{Indexer, IndexString, Index, IndexOrd, BatchTransaction};
    use serde_json::Value;

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
    let res = names_index.find_where("*", "like", Value::String("k*".to_string()));
    println!("users whose name starts with K: {:?}", res.read());
/* outputs
users whose name starts with K: {"user.8": String("Kwadwo"), "user.1": String("Kwadwo"), "user.2": String("Kwame")}
*/
```

Multi index with dot path
> This example demonstrates how you can use json indexer to index a full json object using multiple dot paths
```rust
    let mut students: HashMap<String, Student> = HashMap::new();
    students.insert("student:0".to_owned(), Student {
        name: "Mambisi".to_owned(),
        age: 21,
        state : "CA".to_owned(),
        gpa: 3.1,
    });
    students.insert("student:1".to_owned(), Student {
        name: "Joseph".to_owned(),
        age: 12,
        state : "CA".to_owned(),
        gpa: 3.1,
    });
    students.insert("student:2".to_owned(), Student {
        name: "Elka".to_owned(),
        age: 12,
        state : "FL".to_owned(),
        gpa: 4.0,
    });

    students.insert("student:18".to_owned(), Student {
        name: "Alex".to_owned(),
        age: 15,
        state : "NY".to_owned(),
        gpa: 3.7,
    });

    students.insert("student:18".to_owned(), Student {
        name: "Jackson".to_owned(),
        age: 17,
        state : "NY".to_owned(),
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

    println!("{:?}", students_index.read());
/* Outputs
 {
    "student:2": Object({"age": Number(12), "gpa": Number(4.0), "name": String("Elka"), "state": String("FL")}),
    "student:18": Object({"age": Number(17), "gpa": Number(3.8), "name": String("Jackson"), "state": String("NY")}),
    "student:1": Object({"age": Number(12), "gpa": Number(3.1), "name": String("Joseph"), "state": String("CA")}),
    "student:0": Object({"age": Number(21), "gpa": Number(3.1), "name": String("Mambisi"), "state": String("CA")})
}
*/

    let query = students_index.find_where("state", "eq", Value::String("CA".to_string()));
    println!("Find all students in CA: {:?}", query.read());
/*output
Find all students in CA: {
"student:1": Object({"age": Number(12), "gpa": Number(3.1), "name": String("Joseph"), "state": String("CA")}),
"student:0": Object({"age": Number(21), "gpa": Number(3.1), "name": String("Mambisi"), "state": String("CA")})
}
*/

    let query = students_index.find_where("gpa", "gt", Value::from(3.5));
    println!("Find all students whose gpa greater than 3.5: {:?}", query.read());
/*
Find all students whose gpa greater than 3.5: {
"student:2": Object({"age": Number(12), "gpa": Number(4.0), "name": String("Elka"), "state": String("FL")}),
"student:18": Object({"age": Number(17), "gpa": Number(3.8), "name": String("Jackson"), "state": String("NY")})
}
*/
```