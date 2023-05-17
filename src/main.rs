use futures::TryStreamExt;
use mongodb::{Client, IndexModel};
use mongodb::bson::{doc, Document};
use mongodb::bson::oid::ObjectId;
use mongodb::options::{CreateCollectionOptions, ValidationAction, ValidationLevel};

#[tokio::main]
async fn main() {
    // Connect to database
    let client = Client::with_uri_str("mongodb://mongo:root@localhost:27017").await
        .expect("Unable to connect to MongoDB");
    let db = client.database("mydb");

    // Drop the collection that we are going to create later
    // so that this application can run without error
    db.collection::<Document>("posts").drop(None).await.expect("Unable to drop collection");

    // Create collection (with validation)
    let options = CreateCollectionOptions::builder()
        .validation_level(ValidationLevel::Strict)
        .validation_action(ValidationAction::Error)
        .validator(doc! {
            "$jsonSchema": {
                "title": "Tweet object validation",
                "required": ["title", "message", "tags"],
                "properties": {
                    "title": {
                        "bsonType": "string",
                        "maxLength": 300
                    },
                    "message": {
                        "bsonType": "string",
                        "maxLength": 4000
                    },
                    "tags": {
                        "bsonType": "array",
                        "maxItems": 5,
                        "items": {
                            "bsonType": "string",
                            "minLength": 3,
                            "maxLength": 10
                        }
                    }
                }
            }
        })
        .build();
    db.create_collection("posts", options)
        .await
        .expect("Unable to define collection");
    let col = db.collection::<Post>("posts");

    // Create an index
    let index_model = IndexModel::builder()
        .keys(doc! { "tags": 1 })
        .build();
    col.create_index(index_model, None).await.expect("Unable to create index");

    // Insert
    let posts = vec![
        Post {
            id: ObjectId::new(),
            title: "Post 1".to_string(),
            message: "This is post 1".to_string(),
            tags: vec!["tag1".to_string()],
        },
        Post {
            id: ObjectId::new(),
            title: "Post 2".to_string(),
            message: "This is post 2".to_string(),
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        },
        Post {
            id: ObjectId::new(),
            title: "Hello".to_string(),
            message: "World".to_string(),
            tags: vec!["tag1".to_string(), "tag3".to_string()],
        },
    ];
    col.insert_many(posts, None).await.expect("Unable to insert posts");

    // Find
    let posts: Vec<Post> = col.find(doc! { "tags": "tag1" }, None).await
        .expect("Unable to get Cursor")
        .try_collect().await
        .expect("Unable to collect from items Cursor");
    println!("posts: {:?}", posts);

    // Update
    col.update_many(
        doc! { "tags": "tag2" },
        doc! { "$set": { "title": "Updated title" } },
        None,
    ).await.expect("Unable to update posts");
    let posts: Vec<Post> = col.find(doc! { "tags": "tag2" }, None).await
        .expect("Unable to get Cursor")
        .try_collect().await
        .expect("Unable to collect from items Cursor");
    println!("posts: {:?}", posts);

    // Delete
    col.delete_many(
        doc! { "tags": "tag2" },
        None,
    ).await.expect("Unable to delete posts");
    let posts: Vec<Post> = col.find(doc! { "tags": "tag2" }, None).await
        .expect("Unable to get Cursor")
        .try_collect().await
        .expect("Unable to collect items from Cursor");
    println!("posts: {:?}", posts);

    // Aggregation pipeline
    let pipeline = vec![
        doc! { "$unwind": "$tags" },
        doc! { "$group": {
            "_id": "$tags",
            "post_ids": { "$addToSet": "$_id" }
        }},
    ];
    let post_by_tags: Vec<TagWithPosts> = col.aggregate(pipeline, None).await
        .expect("Unable to aggregate posts")
        .with_type()
        .try_collect().await
        .expect("Unable to collect items from Cursor");
    println!("posts_by_tag: {:?}", post_by_tags);
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Post {
    #[serde(rename = "_id")]
    id: ObjectId,
    title: String,
    message: String,
    tags: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct TagWithPosts {
    #[serde(rename = "_id")]
    tag: String,
    post_ids: Vec<ObjectId>,
}


