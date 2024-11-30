use bevy::prelude::*;
use bevy_pkv::PkvStore;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct PlayerStats {
    health: i32,
    experience: u64,
    level: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct Inventory {
    items: Vec<String>,
    capacity: u32,
}

fn main() {
    App::new()
        .add_plugins(MinimalPlugins)
        .insert_resource(PkvStore::new("BevyPkvExample", "ParquetDemo"))
        .add_systems(Startup, setup)
        .run();
}

fn setup(mut pkv: ResMut<PkvStore>) {
    // Store player stats
    let stats = PlayerStats {
        health: 100,
        experience: 1500,
        level: 5,
    };
    pkv.set("PlayerStats/1", &stats).expect("Failed to store player stats");

    // Store inventory
    let inventory = Inventory {
        items: vec!["Sword".to_string(), "Shield".to_string(), "Potion".to_string()],
        capacity: 10,
    };
    pkv.set("Inventory/1", &inventory).expect("Failed to store inventory");

    // Retrieve and display the stored data
    if let Ok(stored_stats) = pkv.get::<PlayerStats>("PlayerStats/1") {
        info!("Retrieved player stats: {:?}", stored_stats);
    }

    if let Ok(stored_inventory) = pkv.get::<Inventory>("Inventory/1") {
        info!("Retrieved inventory: {:?}", stored_inventory);
    }

    // Remove player stats and verify
    pkv.remove("PlayerStats/1").expect("Failed to remove player stats");
    match pkv.get::<PlayerStats>("PlayerStats/1") {
        Ok(_) => info!("Player stats still exist (unexpected)"),
        Err(_) => info!("Player stats successfully removed"),
    }
}
