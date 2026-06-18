use age::secrecy::SecretString;

pub struct AgeKeyPair {
    pub public_key: String,
    pub private_key: SecretString,
}

pub fn generate_key_pair() -> AgeKeyPair {
    let identity = age::x25519::Identity::generate();
    AgeKeyPair {
        public_key: identity.to_public().to_string(),
        private_key: identity.to_string(),
    }
}
