use dotenvy;
use obws::Client;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let _env_var_path: PathBuf = dotenvy::dotenv().unwrap();
    let server_ip: String = std::env::var("SERVERIP").unwrap();
    let password: String = std::env::var("PASSWORD").unwrap();
    let port: u16 = std::env::var("PORT").unwrap().parse().unwrap();

    let client = Client::connect(server_ip, port, Some(password)).await.unwrap();
    let version = client.general().version().await.unwrap();
    println!("{:#?}", version);
}
