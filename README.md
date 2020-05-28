# Json Indexer
[![Crates.io](https://img.shields.io/crates/v/indexer)](https://crates.io/crates/indexer)

multi value indexer for a json object.

this crate allows to create a sorted map of json objects based on the dot path, its similar to what a database like mongodb
will generate and index based on the path given, this crate is meant to be used in creating no sql databases. This library was 
created to be used as indexing structure for [escanordb](https://github.com/mambisi/escanor).

## Road to 0.3 : TODO 
- Array Query Operators
    - [ ] Contains: `all`
    - [ ] Contains Any: `any`
    - [ ] In: `in`

## Road to 0.2 :  TODO
- [X] Basic Query support
    - [X] Operators  `eq`  `lt` `gt` 
    - [X] limit query output
    - [X] like (MS SQL LIKE `*ja*`)
- [X] Compound queries
- [X] Order by

## Version 0.1.5
Features
- Basic queries check below for documentation

## Example

Single index

> This example demonstrates how you can use json indexer to index a json value
```rust
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
    students_index.insert("student:0", Student {
        name: "Mambisi".to_owned(),
        age: 21,
        state: "CA".to_owned(),
        gpa: 3.1,
    });
    students_index.insert("student:1", Student {
        name: "Joseph".to_owned(),
        age: 12,
        state: "CA".to_owned(),
        gpa: 3.1,
    });
    students_index.insert("student:2", Student {
        name: "Elka".to_owned(),
        age: 12,
        state: "FL".to_owned(),
        gpa: 4.0,
    });

    students_index.insert("student:18", Student {
        name: "Alex".to_owned(),
        age: 15,
        state: "NY".to_owned(),
        gpa: 3.7,
    });

    students_index.insert("student:18", Student {
        name: "Jackson".to_owned(),
        age: 17,
        state: "NY".to_owned(),
        gpa: 3.8,
    });



    let query = students_index.find_where("state", Op::EQ, "CA");
    println!("Find all students in CA: {:?}", query.get());

    let query = students_index.find_where("gpa", Op::GT, 3.5);
    println!("Find all students whose gpa greater than 3.5: {:?}", query.get());



    let string_indexer = Indexer::String(IndexString {
        ordering: IndexOrd::ASC
    });

    let mut names_index = Index::new(string_indexer);
    names_index.batch(|b| {
        b.insert("user.1", "Kwadwo");
        b.insert("user.9", "Kwadwo");
        b.insert("user.8", "Kwabena");
        b.insert("user.2", "Kwame");
        b.insert("user.3", "Joseph");
        b.insert("user.4", "Jake");
        b.insert("user.5", "Mambisi");
        b.insert("user.6", "Ama");
        b.commit()
    });

    println!("Index Tree: {}", serde_json::to_string_pretty(&names_index).unwrap());

    names_index.remove("user.1");
    let res = names_index.find_where("*", Op::LIKE, Value::String("k*".to_string()));
    println!("users whose name starts with K");
    println!("{:?}", res.get());
/* Output
Find all students in CA: [
    ("student:1", Object({"age": Number(12), "gpa": Number(3.1), "name": String("Joseph"), "state": String("CA")})),
    ("student:0", Object({"age": Number(21), "gpa": Number(3.1), "name": String("Mambisi"), "state": String("CA")}))
]
Find all students whose gpa greater than 3.5: [
    ("student:18", Object({"age": Number(17), "gpa": Number(3.8), "name": String("Jackson"), "state": String("NY")})),
    ("student:2", Object({"age": Number(12), "gpa": Number(4.0), "name": String("Elka"), "state": String("FL")}))
]
users whose name starts with K
[
    ("user.2", String("Kwame")),
    ("user.8", String("Kwadwo")),
    ("user.1", String("Kwadwo"))
]
*/
```

Multi index with dot path
> This example demonstrates how you can use json indexer to index a full json object using multiple dot paths
>
>Download sample data file [MOVIES_JSON_FILE](https://raw.githubusercontent.com/mambisi/json_indexer/master/sample/movies.json)
```rust
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
            b.insert(key.as_str().unwrap(), v.clone())
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

    let mut query = index.find_where("title", Op::LIKE, "Jumanji*");
    let found = query.count();

    let completion_time = timer.elapsed().as_millis();
    println!("query completed in {} ms, found {} items", completion_time, found);


    println!("Showing Results: release date");
    query.order_by(order_indexer).limit(10).get().iter().for_each(|(k, v)| {
        println!("{}:{}", k, serde_json::to_string_pretty(v).unwrap());
    })
/* outputs
Indexed list of size: 19653 in 1.043351253 secs
query completed in 10 ms, found 3 items
Showing Results: release date
512200:{
  "id": "512200",
  "overview": "Plot kept under wraps.",
  "poster": "https://image.tmdb.org/t/p/w1280null",
  "release_date": 1576022400,
  "title": "Jumanji 3"
}
353486:{
  "id": "353486",
  "overview": "The tables are turned as four teenagers are sucked into Jumanji's world - pitted against rhinos, black mambas and an endless variety of jungle traps and puzzles. To survive, they'll play as characters from the game.",
  "poster": "https://image.tmdb.org/t/p/w1280/bXrZ5iHBEjH7WMidbUDQ0U2xbmr.jpg",
  "release_date": 1512777600,
  "title": "Jumanji: Welcome to the Jungle"
}
8844:{
  "id": "8844",
  "overview": "When siblings Judy and Peter discover an enchanted board game that opens the door to a magical world, they unwittingly invite Alan -- an adult who's been trapped inside the game for 26 years -- into their living room. Alan's only hope for freedom is to finish the game, which proves risky as all three find themselves running from giant rhinoceroses, evil monkeys and other terrifying creatures.",
  "poster": "https://image.tmdb.org/t/p/w1280/vgpXmVaVyUL7GGiDeiK1mKEKzcX.jpg",
  "release_date": 818985600,
  "title": "Jumanji"
}
*/
```