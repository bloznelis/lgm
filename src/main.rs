use reqwest;
use tokio;

async fn fetch_topics(namespace: &String) -> Result<Vec<String>, reqwest::Error> {
    let addr = "http://127.0.0.1:8080";
    // Make the GET request to list tenants
    let response = reqwest::get(format!(
        "{}/admin/v2/namespaces/{}/topics",
        addr, namespace
    ))
    .await?;
    // println!("resp {:?}", response);

    let topics: Vec<String> = response.json().await?;

    Ok(topics)
}

async fn fetch_namespaces(tenant: &String) -> Result<Vec<String>, reqwest::Error> {
    let addr = "http://127.0.0.1:8080";
    // Make the GET request to list tenants
    let response = reqwest::get(format!("{}/admin/v2/namespaces/{}", addr, tenant)).await?;

    let namespaces: Vec<String> = response.json().await?;

    Ok(namespaces)
}

#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    env_logger::init();

    let addr = "http://127.0.0.1:8080";
    let response = reqwest::get(format!("{}/admin/v2/tenants", addr)).await?;

    if response.status().is_success() {
        let tenants: Vec<String> = response.json().await?;
        for tenant in tenants {
            println!("Tenant: {:?}", tenant);
            let namespaces = fetch_namespaces(&tenant).await?;
            for namespace in namespaces {
                println!("Namespace: {:?}", namespace);
                let topics = fetch_topics(&namespace).await?;
                println!("Topics: {:?}", topics)
            }
        }
    } else {
        eprintln!("Error: {:?}", response.status());
    }

    Ok(())
}
