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
        path_orders: vec![release_date_order.clone(), title_order]
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

    let mut timer = Instant::now();

    let order_indexer = Indexer::Json(IndexJson {
        path_orders: vec![release_date_order.clone()]
    });

    let mut query = index.find_where("title", "like", Value::String(String::from("Jumanji*")));
    let found = query.count();

    let completion_time = timer.elapsed().as_millis();
    println!("query completed in {} ms, found {} items", completion_time, found);


    println!("Showing Results: release date");
    query.order_by(order_indexer).limit(10).iter(|(k, v)| {
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