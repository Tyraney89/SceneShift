use dotenvy;
use obws::{Client, responses::scenes::Scenes};
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let _env_var_path: PathBuf = dotenvy::dotenv().unwrap();
    let server_ip: String = std::env::var("SERVERIP").unwrap();
    let password: String = std::env::var("PASSWORD").unwrap();
    let port: u16 = std::env::var("PORT").unwrap().parse().unwrap();

    let client: Client = Client::connect(server_ip, port, Some(password))
        .await
        .unwrap();

    let scenes: Scenes = client.scenes().list().await.unwrap();

    client
        .scenes()
        .set_current_program_scene(scenes.scenes[2].id.clone())
        .await
        .unwrap();
    println!("scenes: {:#?}", scenes);
}
