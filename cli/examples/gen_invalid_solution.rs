// Copyright (c) 2019-2025 Provable Inc.
// This file is part of the snarkOS library.
//
// Generates a single invalid solution (valid structure, wrong epoch/proof) as JSON
// for use by the invalid-solutions benchmark. Build with: --features bench

#![cfg(feature = "bench")]

use snarkvm::{
    ledger::puzzle::{PartialSolution, Solution},
    prelude::{Address, PrivateKey, Rng, TestRng},
};

type CurrentNetwork = snarkvm::prelude::TestnetV0;

fn main() {
    let mut rng = TestRng::default();
    let private_key = PrivateKey::<CurrentNetwork>::new(&mut rng).unwrap();
    let address = Address::try_from(private_key).unwrap();
    let partial_solution = PartialSolution::new(rng.r#gen(), address, rng.r#gen()).unwrap();
    let solution = Solution::new(partial_solution, rng.r#gen());
    let json = serde_json::to_string(&solution).unwrap();
    println!("{json}");
}
