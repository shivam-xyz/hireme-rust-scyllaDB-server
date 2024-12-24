use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use scylla::{Session, SessionBuilder};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: Uuid,
    name: String,
    email: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct NewUser {
    name: String,
    email: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct UpdateUser {
    name: Option<String>,
    email: Option<String>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let session: Session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await
        .expect("Failed to connect to ScyllaDB");

    async fn get_all_users(data: web::Data<AppState>) -> impl Responder {
        let session = &data.session;

        let query = format!("SELECT id, name, email FROM {}.users", data.keyspace);

        let results = match session.query_iter(&*query, &[]).await {
            Ok(results) => results,
            Err(e) => return HttpResponse::InternalServerError().json(format!("Query error: {}", e)),
        };

        let mut users = Vec::new();

        let mut rows_stream = match results.rows_stream::<(Uuid, String, String)>() {
            Ok(stream) => stream,
            Err(e) => return HttpResponse::InternalServerError().json(format!("Error streaming rows: {}", e)),
        };

        while let Some(row) = match rows_stream.try_next().await {
            Ok(row) => row,
            Err(e) => return HttpResponse::InternalServerError().json(format!("Error fetching next row: {}", e)),
        } {
            let (id, name, email) = row;
            users.push(User { id, name, email });
        }
        println!("Users : {:?}", users);
        HttpResponse::Ok().json(users)
    }

    async fn register_user(
        new_user: web::Json<NewUser>, 
        data: web::Data<AppState>
    ) -> impl Responder {
        let session = &data.session;

        let new_id = Uuid::new_v4();

        let query = format!(
            "INSERT INTO {}.users (id, name, email) VALUES (?, ?, ?)",
            data.keyspace
        );

        match session.query_unpaged(
            &*query,
            (new_id, new_user.name.clone(), new_user.email.clone())
        ).await {
            Ok(_) => HttpResponse::Created().json(format!("User {} created successfully", new_id)),
            Err(e) => HttpResponse::InternalServerError().json(format!("Failed to create user: {}", e)),
        }
    }

    async fn update_user(
        user_id: web::Path<Uuid>,
        updated_user: web::Json<UpdateUser>,
        data: web::Data<AppState>,
    ) -> impl Responder {
        let session = &data.session;
        let user_id_value = user_id.into_inner();

        let mut query = format!("UPDATE {}.users SET", data.keyspace);
        let mut params = Vec::new();

        if let Some(name) = &updated_user.name {
            query.push_str(" name = ?,");
            params.push(name.clone());
        }
        if let Some(email) = &updated_user.email {
            query.push_str(" email = ?,");
            params.push(email.clone());
        }

        if query.ends_with(',') {
            query.pop();
        }
        query.push_str(format!(" WHERE id = {}", user_id_value).as_str());
        match session.query_unpaged(query, params).await {
            Ok(_) => HttpResponse::Ok().json(format!("User with ID {} updated successfully", user_id_value)),
            Err(e) => HttpResponse::InternalServerError().json(format!("Failed to update user: {}", e)),
        }
    }

    async fn delete_user(
        user_id: web::Path<Uuid>,
        data: web::Data<AppState>,
    ) -> impl Responder {
        let session = &data.session;
        let query = format!(
            "DELETE FROM {}.users WHERE id = ?",
            data.keyspace
        );

        let user_id_value = user_id.into_inner();

        match session.query_unpaged(query, (user_id_value,)).await {
            Ok(_) => HttpResponse::Ok().json(format!("User with ID {} deleted successfully", user_id_value)),
            Err(e) => HttpResponse::InternalServerError().json(format!("Failed to delete user: {}", e)),
        }
    }
    

    async fn get_user_by_id(
        user_id: web::Path<Uuid>,
        data: web::Data<AppState>,
    ) -> impl Responder {
        let session = &data.session;
    
        let query = format!(
            "SELECT id, name, email FROM {}.users WHERE id = ?",
            data.keyspace
        );
    
        let user_id_clone = user_id.clone();
    
        match session.query_iter(&*query, (user_id_clone,)).await {
            Ok(results) => {
                let mut rows_stream = match results.rows_stream::<(Uuid, String, String)>() {
                    Ok(stream) => stream,
                    Err(e) => return HttpResponse::InternalServerError().json(format!("Error streaming rows: {}", e)),
                };
    
                // Process the result row-by-row
                if let Some(row) = rows_stream.try_next().await.unwrap_or(None) {
                    let (id, name, email) = row;
                    let user = User { id, name, email };
                    HttpResponse::Ok().json(user)
                } else {
                    HttpResponse::NotFound()
                        .json(format!("User with ID {} not found", user_id.into_inner()))
                }
            }
            Err(e) => HttpResponse::InternalServerError()
                .json(format!("Failed to execute query: {}", e)),
        }
    }
    

    
    

    // Define application state using Arc for the session to be clonable
    #[derive(Clone)]
    struct AppState {
        session: Arc<Session>,
        keyspace: String,
    }

    let app_state = AppState {
        session: Arc::new(session),
        keyspace: String::from("my_keyspace"),
    };

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(app_state.clone()))
            .route("/users", web::get().to(get_all_users))
            .route("/register", web::post().to(register_user))
            .route("/update/{id}", web::patch().to(update_user))
            .route("/delete/{id}", web::delete().to(delete_user))
            .route("/users/{id}", web::get().to(get_user_by_id))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}